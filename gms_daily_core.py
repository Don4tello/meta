#!/usr/bin/env python3
# @lint-ignore-every DSFMT [do not copy-paste]. Format the file to remove this comment. https://fburl.com/wiki/mmzh332m

from dataswarm.operators import (
    DqOperator,
    GlobalDefaults,
    MixedSourceDqOperator,
    PrestoInsertOperator,
    WaitForHiveOperator,
)
from dataswarm.operators.runcontext import is_test
from dataswarm.operators.txtmacro import ExecuteWithMacros
from dataswarm_commons.operators import PrestoInsertOperatorWithSchema
from dataswarm_commons.operators.schema import (
    Table,
    Column,
    VARCHAR,
    BIGINT,
    INTEGER,
    HiveAnon,
)
from edw_bir01.bi_secure.revenue.etl.weighted_facts.common import Common

from .config import (
    BpoCoverageAsisStg1,
    BpoCoverageAsisStg2,
    BpoCoverageAsisStg3,
    BpoCoverageAsisStg4,
    GmsDailyCoreSignal,
    GmsDailyStg1Signal,
)

# from upm.batch.api import DataSet


extraemails = "sori@fb.com"
"""
  state variable :

This conditional variable controls the behaviour of our DQ
operators depending on the type of run.
if the run is a production run, the dq operator will hard lock on a failure
if the run is a test, the test will soft lock on a fail.
"""
state = False

if is_test():
    state = True

c = Common(oncall=None, extra_emails=[])

GlobalDefaults.set(
    user="bp_t_gms_tech_de_bp_o",
    schedule="@daily",
    num_retries=1,
    depends_on_past=True,
    data_project_acl="gms_tech_data_eng",
    oncall="bp_t_gms_tech_de_bp_o",
    tag=["gms-dashboard", "gms-ca"],
    partition="ds=<DATEID>",
)

GlobalDefaults.add_macros(
    prev_quarter_id=ExecuteWithMacros(
        c.get_quarter_start, "<DATEID>", quarters_behind=1
    )
),
GlobalDefaults.add_macros(quarter_end=ExecuteWithMacros(c.get_quarter_end, "<DATEID>"))
GlobalDefaults.add_macros(
    quarter_id=ExecuteWithMacros(c.get_quarter_start, "<DATEID>")
),

# Table objects

stg1 = BpoCoverageAsisStg1()
stg2 = BpoCoverageAsisStg2()
stg3 = BpoCoverageAsisStg3()
stg4 = BpoCoverageAsisStg4()
sig = GmsDailyCoreSignal()
stg1_sig = GmsDailyStg1Signal()

wait_for_bpo_coverage_ad_acct_quota = WaitForHiveOperator(
    dep_list=[],
    table="bpo_coverage_ad_acct_quota",
    partition="ds=<DATEID>",
    num_retries=2000,
)
wait_for_d_employee = WaitForHiveOperator(
    dep_list=[],
    table="d_employee:hr",
    use_future=False,
    fail_on_future=True,
    num_retries=2000,
)
wait_for_sales_ops_forecast_territory_flat = WaitForHiveOperator(
    dep_list=[],
    table="sales_ops_forecast_territory_flat",
    partition="ds=<DATEID>",
    num_retries=2000,
)


wait_for_bpt_fct_l4_gms_rec_rev_data = WaitForHiveOperator(
    dep_list=[],
    table="staging_bpt_fct_l4_gms_rec_rev_data",
    partition="ds=<DATEID>",
    num_retries=2000,
)
wait_for_d_geo_country = WaitForHiveOperator(dep_list=[], table="d_geo_country")

wait_for_fct_advertiser_lwi_spend_details = WaitForHiveOperator(
    dep_list=[],
    table="fct_advertiser_lwi_spend_details:finance",
    partition="ds=<DATEID>",
)

bpo_coverage_ad_acct_quota_dq = DqOperator(
    dep_list=[wait_for_bpo_coverage_ad_acct_quota],
    datasource="presto",
    date_id="<DATEID>",
    table_name="bpo_coverage_ad_acct_quota",
    check_name="upstrm_check",
    where_clause="",
    notify=["task", "email"],
    task_pri="high",
    notify_cc=["mmazur@fb.com", "seannolan@fb.com"],
    dq_do_not_stop_pipeline=state,
)


bpo_coverage_ad_acct_quota_dq.add_metric(
    metric_name="advertiser_quota",
    aggregator="SUM(adv_quota)",
    anomaly_score_tolerance=0.05,
)

bpo_coverage_ad_acct_quota_dq.add_metric(
    metric_name="agency_quota",
    aggregator="SUM(agency_quota)",
    anomaly_score_tolerance=0.05,
)

bpo_coverage_ad_acct_quota_dq.add_metric(
    metric_name="ad_account_count",
    aggregator="COUNT(ad_account_id)",
    anomaly_score_tolerance=0.05,
)

bpo_coverage_ad_acct_quota_dq.add_metric(
    metric_name="sales_fcast",
    aggregator="sum(sales_forecast)",
    anomaly_score_tolerance=0.05,
)

bpo_coverage_ad_acct_quota_dq.add_metric(
    metric_name="sales_fcast_prior",
    aggregator="sum(sales_forecast_prior)",
    anomaly_score_tolerance=0.05,
)

bpo_coverage_ad_acct_quota_dq.add_metric(
    metric_name="reseller_quota",
    aggregator="sum(reseller_quota)",
    anomaly_score_tolerance=0.05,
)

bpo_coverage_ad_acct_dq_dim_check = DqOperator(
    dep_list=[wait_for_bpo_coverage_ad_acct_quota],
    datasource="presto",
    date_id="<DATEID>",
    table_name="bpo_coverage_ad_acct_quota",
    check_name="program check",
    alert_message=""" You are seeing this issue because
                      we have detected a change in the number
                      of programs in our upstream table.
                      The pipeline has been suspended pending investigation  """,
    notify=["task", "email"],
    task_pri="high",
    notify_cc=["mmazur@fb.com", "achapetta@fb.com", "emilycassell@fb.com"],
    dq_do_not_stop_pipeline=state,
    subquery="""
    SELECT
        COUNT(DISTINCT revenue_segment) number_of_programs,
        COUNT(DISTINCT ad_account_id) number_of_ad_accounts,
        COUNT(DISTINCT l12_advertiser_id) number_of_advertiser_territory_ids,
        COUNT(DISTINCT l12_agency_id) number_of_agency_territory_ids,
        COUNT(DISTINCT l12_reporting_territory_id) number_of_reporting_territory_ids,
        ds
    FROM (
        SELECT
            revenue_segment,
            ds,
            ad_account_id,
            COALESCE(l12fbid_cp, l12fbid_am, l12fbid_unmanaged, 'Unassigned') l12_advertiser_id,
            COALESCE(l12fbid_ap, l12fbid_pm, 'Unassigned') l12_agency_id,
            COALESCE(
                l12fbid_cp,
                l12fbid_am,
                l12fbid_pm,
                l12fbid_ap,
                l12fbid_rp,
                l12fbid_engaged,
                l12fbid_unmanaged
            ) l12_reporting_territory_id
        FROM bpo_coverage_ad_acct_quota
        WHERE
            ds = '<DATEID>'
        GROUP BY
            revenue_segment,
            ad_account_id,
            ds,
            COALESCE(l12fbid_cp, l12fbid_am, l12fbid_unmanaged, 'Unassigned'),
            COALESCE(l12fbid_ap, l12fbid_pm, 'Unassigned'),
            COALESCE(
                l12fbid_cp,
                l12fbid_am,
                l12fbid_pm,
                l12fbid_ap,
                l12fbid_rp,
                l12fbid_engaged,
                l12fbid_unmanaged
            )
    )
    GROUP BY
        DS
    """,
)


bpo_coverage_ad_acct_dq_dim_check.add_metric(
    metric_name="number_of_programs",
    aggregator="SUM(number_of_programs)",
    warn_on_new=True,
    notify_on_warn=True,
    anomaly_score_tolerance=0,
)
bpo_coverage_ad_acct_dq_dim_check.add_metric(
    metric_name="number_of_ad_accounts",
    aggregator="SUM(number_of_ad_accounts)",
    anomaly_score_tolerance=0.15,
)
bpo_coverage_ad_acct_dq_dim_check.add_metric(
    metric_name="number_of_advertiser_territory_ids",
    aggregator="SUM(number_of_advertiser_territory_ids)",
    warn_on_new=True,
    notify_on_warn=True,
    anomaly_score_tolerance=0.1,
)
bpo_coverage_ad_acct_dq_dim_check.add_metric(
    metric_name="number_of_agency_territory_ids",
    aggregator="SUM(number_of_agency_territory_ids)",
    warn_on_new=True,
    notify_on_warn=True,
    anomaly_score_tolerance=0.1,
)
bpo_coverage_ad_acct_dq_dim_check.add_metric(
    metric_name="number_of_reporting_territory_ids",
    aggregator="SUM(number_of_reporting_territory_ids)",
    warn_on_new=True,
    notify_on_warn=True,
    anomaly_score_tolerance=0.1,
)

bpo_coverage_asis_stg_emp_map = PrestoInsertOperatorWithSchema(
    dep_list=[
        wait_for_d_employee,
    ],
    table="<TABLE:bpo_coverage_asis_stg_emp_map>",
    partition={"ds": "<DATEID>"},
    create=Table(
        cols=[
            Column("workplace_fbid", BIGINT, "", HiveAnon.NONE),
            Column("preferred_name", VARCHAR, "", HiveAnon.NONE),
            Column("unix_username", VARCHAR, "", HiveAnon.NONE),
            Column("manager_employee_id", BIGINT, "", HiveAnon.NONE),
            Column("employee_id", BIGINT, "", HiveAnon.NONE),
            Column("personal_fbid", BIGINT, "", HiveAnon.NONE),
            Column("hire_date", VARCHAR, "", HiveAnon.NONE),
            Column("manager", VARCHAR, "", HiveAnon.NONE),
            Column("manager_username", VARCHAR, "", HiveAnon.NONE),
        ],
        partitions=[Column("ds", VARCHAR, "Date Stamp")],
        retention=1,
    ),
    select="""
         SELECT
                workplace_fbid,
                emp.preferred_name,
                emp.unix_username,
                manager_employee_id,
                emp.employee_id,
                personal_fbid,
                hire_date,
                manager,
                manager_username

                FROM d_employee:hr emp

                LEFT JOIN (
                    SELECT employee_id,
                           preferred_name manager,
                           unix_username manager_username

                    from d_employee:hr

                    WHERE ds ='<DATEID>'

                ) manager on manager.employee_id = emp.manager_employee_id

                WHERE
                    ds = '<DATEID>'


    """,
)

bpo_coverage_asis_stg_reduction = PrestoInsertOperatorWithSchema(
    dep_list=[wait_for_bpt_fct_l4_gms_rec_rev_data],
    table="<TABLE:bpo_coverage_asis_stg_reduction>",
    create=Table(
        cols=[
            Column("ad_account_id", BIGINT, "", HiveAnon.NONE),
            Column("reduction", INTEGER, "", HiveAnon.NONE),
        ],
        partitions=[Column("ds", VARCHAR, "", HiveAnon.NONE)],
        retention=1,
    ),
    select="""
        SELECT
        DISTINCT
        account_id ad_account_id,
        1 reduction
    FROM staging_bpt_fct_l4_gms_rec_rev_data
    WHERE
        ds = '<DATEID>'
        AND quarter_id >= cast((cast('<quarter_id>' as date) - interval '2' year) as varchar)
        AND asis_rec_rev  IS NOT NULL
    """,
)
bpo_coverage_asis_stg_lwi_am_ind = PrestoInsertOperatorWithSchema(
    dep_list=[wait_for_fct_advertiser_lwi_spend_details],
    table="<TABLE:bpo_coverage_asis_stg_lwi_am_ind>",
    create=Table(
        cols=[
            Column("account_id", BIGINT, "", HiveAnon.NONE),
            Column(
                "am_lwi_ind",
                VARCHAR,
                "if pct lwi_l90 > .99 then lwi else AM",
                HiveAnon.NONE,
            ),
        ],
        partitions=[Column("ds", VARCHAR, "DateStamp")],
        retention=1,
    ),
    select="""

            SELECT
                IF(lwi_90d_percentage >= .99, 'LWI', 'AM') am_lwi_ind,
                account_id
            FROM fct_advertiser_lwi_spend_details:finance
            CROSS JOIN UNNEST (ad_account_ids) as t(account_id)
            WHERE
                ds = '<DATEID>'
    """,
)

bpo_coverage_asis_stg_1 = PrestoInsertOperator(
    dep_list=[
        wait_for_bpo_coverage_ad_acct_quota,
        wait_for_sales_ops_forecast_territory_flat,
        bpo_coverage_ad_acct_quota_dq,
        bpo_coverage_ad_acct_dq_dim_check,
        wait_for_d_geo_country,
        bpo_coverage_asis_stg_emp_map,
        bpo_coverage_asis_stg_reduction,
        bpo_coverage_asis_stg_lwi_am_ind,
    ],
    documentation={
        "description": """Table Name:bpo_coverage_asis_stg_1
             dependency: wait_for_bpo_coverage_ad_acct_quota
             documentation: combines data from bpo_coverage_ad_acct_quota &
             sales_op_territory_flat into a mapping table."""
    },
    table="<TABLE:bpo_coverage_asis_stg_1>",
    create=stg1.get_create(),
    select=stg1.get_select(),
    fail_on_empty=True,
)


stg_1_dq = DqOperator(
    dep_list=[bpo_coverage_asis_stg_1],
    datasource="presto",
    date_id="<DATEID>",
    table_name="<TABLE:bpo_coverage_asis_stg_1>",
    check_name="stg1_check",
    where_clause="",
    notify=["task", "email"],
    task_pri="high",
    notify_cc=["mmazur@fb.com", "seannolan@fb.com"],
    user="mmazur",
    dq_do_not_stop_pipeline=state,
)

gms_daily_stg_1_signal = PrestoInsertOperator(
    dep_list=[stg_1_dq],
    documentation={
        "description": """Table Name:gms_daily_core_signal
             dependency: stg_4_fcast_dq
             documentation: this signal table will land when
             all data has been loaded AND dq checks have  passed
             this will tell downstream pipelines to proceed """
    },
    table="<TABLE:gms_daily_stg_1_signal>",
    create=stg1_sig.get_create(),
    select=stg1_sig.get_select(),
    fail_on_empty=True,
)


stg_1_dq.add_metric(
    metric_name="gms_optimal_target",
    aggregator="MAX(gms_optimal_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="gms_liquidity_target",
    aggregator="MAX(gms_liquidity_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="gso_optimal_target",
    aggregator="MAX(gso_optimal_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="gso_liquidity_target",
    aggregator="MAX(gso_liquidity_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="smb_optimal_target",
    aggregator="MAX(smb_optimal_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="smb_liquidity_target",
    aggregator="MAX(smb_liquidity_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="l4_optimal_target",
    aggregator="MAX(l4_optimal_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="l4_liquidity_target",
    aggregator="MAX(l4_liquidity_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="l8_optimal_target",
    aggregator="MAX(l8_optimal_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="l8_liquidity_target",
    aggregator="MAX(l8_liquidity_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="l10_optimal_target",
    aggregator="MAX(l10_optimal_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="l10_liquidity_target",
    aggregator="MAX(l10_liquidity_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="l12_optimal_target",
    aggregator="MAX(l12_optimal_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="l12_liquidity_target",
    aggregator="MAX(l12_liquidity_target)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="advertiser_quota",
    aggregator="SUM(adv_quota)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="agency_quota",
    aggregator="SUM(agency_quota)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="ad_account_count",
    aggregator="COUNT(ad_account_id)",
    anomaly_score_tolerance="low",
)

stg_1_dq.add_metric(
    metric_name="sales_fcast",
    aggregator="sum(sales_forecast)",
    anomaly_score_tolerance="low",
)


stg_1_dq.add_metric(
    metric_name="sales_fcast_prior",
    aggregator="sum(sales_forecast_prior)",
    anomaly_score_tolerance="low",
)


bpo_coverage_asis_stg_2 = PrestoInsertOperator(
    dep_list=[stg_1_dq],
    documentation={
        "description": """Table Name:bpo_coverage_asis_stg_2
             dependency: stg_1_dq
             documentation: cross joins bpo_coverage_asis_stg_1
             with Current quarter dates """
    },
    table="<TABLE:bpo_coverage_asis_stg_2>",
    create=stg2.get_create(),
    select=stg2.get_select(),
    fail_on_empty=True,
)

bpo_coverage_asis_stg_3 = PrestoInsertOperator(
    dep_list=[bpo_coverage_asis_stg_2, wait_for_bpt_fct_l4_gms_rec_rev_data],
    documentation={
        "description": """Table Name:bpo_coverage_asis_stg_3
             dependency: bpo_coverage_asis_stg_2
             documentation: joins current quarter
             revenue from bpo_forecast_as_is_dsc to stg_2 mapping table
             """
    },
    large_batch_mode=True,
    table="<TABLE:bpo_coverage_asis_stg_3>",
    create=stg3.get_create(),
    select=stg3.get_select(),
    fail_on_empty=True,
)

bpo_coverage_asis_stg_3_dq_check_revenue = MixedSourceDqOperator(
    dep_list=[
        bpo_coverage_asis_stg_3,
    ],
    source_datasource="presto",
    target_datasource="presto",
    dq_do_not_stop_pipeline=False,
    source_query="""
    SELECT
    SUM(asis_rec_rev) spend

FROM staging_bpt_fct_l4_gms_rec_rev_data upstream

JOIN (
    SELECT
        ad_account_id
    FROM <TABLE:bpo_coverage_Asis_stg_3>
    WHERE ds = '<DATEID>'
        AND DATE_TRUNC('quarter', date(date_id)) IN (date '<quarter_id>')
        AND NOT is_gpa
        AND is_fcast_eligible
        AND fraud_ind = '0'
        AND cq_revenue > 0
        AND advertiser_name IS NOT NULL
    GROUP BY 1
) downstream on downstream.ad_account_id = upstream.account_id
WHERE
    ds = '<DATEID>'
    AND quarter_id in ('<quarter_id>')


    """,
    target_query="""
    SELECT
        SUM(cq_revenue) spend
    FROM <TABLE:bpo_coverage_asis_stg_3>
    WHERE
        ds = '<VALID_DATEID:<TABLE:bpo_coverage_asis_stg_3>>'
        AND DATE_TRUNC('quarter', date(date_id)) IN (date '<quarter_id>')
        AND NOT is_gpa
        AND is_fcast_eligible
        AND fraud_ind = '0'
        AND cq_revenue > 0
        AND advertiser_name IS NOT NULL
        --AND id_d_customer_account_adv != -999
    """,
    notify=["task"],
    threshold=1,
)
bpo_coverage_asis_stg_3_dq_check_quota = MixedSourceDqOperator(
    dep_list=[bpo_coverage_asis_stg_3],
    source_datasource="presto",
    target_datasource="presto",
    dq_do_not_stop_pipeline=state,
    source_query="""
    SELECT
        COALESCE(SUM(adv_quota), 0) adv_quota,
        COALESCE(SUM(agency_quota), 0) agency_quota
    FROM bpo_coverage_ad_acct_quota
    WHERE
        ds = '<DATEID>'
    """,
    target_query="""
    SELECT
        SUM(CASE
            WHEN date_id = '<DATEID>' THEN advertiser_quota
            ELSE 0
        END) adv_quota,
        SUM(CASE
            WHEN date_id = '<DATEID>' THEN agency_quota
            ELSE 0
        END) agency_quota
    FROM <TABLE:bpo_coverage_asis_stg_3>
    WHERE
        ds = '<DATEID>'
    """,
    notify=["task"],
    threshold=[1, 1],
)

bpo_coverage_asis_stg_3_dq_check_fcast = MixedSourceDqOperator(
    dep_list=[bpo_coverage_asis_stg_3],
    source_datasource="presto",
    target_datasource="presto",
    dq_do_not_stop_pipeline=state,
    source_query="""
    SELECT
        COALESCE(SUM(sales_forecast), 0) sales_fcast,
        COALESCE(SUM(sales_forecast_prior), 0) sales_fcast_prior
    FROM bpo_coverage_ad_acct_quota
    WHERE
        ds = '<VALID_DATEID:bpo_coverage_ad_acct_quota>'
    """,
    target_query="""
    SELECT
        COALESCE(SUM(sales_forecast), 0) sales_fcast,
        COALESCE(SUM(sales_forecast_prior), 0) sales_fcast_prior
    FROM <TABLE:bpo_coverage_asis_stg_3>
    WHERE
        ds = '<VALID_DATEID:<TABLE:bpo_coverage_asis_stg_3>>'
        AND date_id = '<VALID_DATEID:<TABLE:bpo_coverage_asis_stg_3>>'
    """,
    notify=["task"],
    threshold=[1, 1],
)


bpo_coverage_asis_stg_4 = PrestoInsertOperator(
    dep_list=[
        bpo_coverage_asis_stg_3_dq_check_revenue,
        bpo_coverage_asis_stg_3_dq_check_quota,
        bpo_coverage_asis_stg_3_dq_check_fcast,
    ],
    documentation={
        "description": """Table Name:bpo_coverage_asis_stg_4
             dependency: bpo_coverage_asis_stg_3_dq_check_revenue
             documentation: rolls up stage 3 data to advertiser level"""
    },
    table="<TABLE:bpo_coverage_asis_stg_4>",
    create=stg4.get_create(),
    select=stg4.get_select(),
    fail_on_empty=True,
)


stg_4_fcast_qouta_dq = DqOperator(
    dep_list=[bpo_coverage_asis_stg_4],
    datasource="presto",
    date_id="<DATEID>",
    table_name="<TABLE:bpo_coverage_asis_stg_4>",
    check_name="stg_4_qouta_check",
    where_clause="date_id='<DATEID>'",
    notify=["task"],
    task_pri="high",
    notify_cc=["mmazur@fb.com", "achappeta@fb.com"],
    user="mmazur",
    dq_do_not_stop_pipeline=state,
)

stg_4_fcast_qouta_dq.add_metric(
    metric_name="advertiser_quota",
    aggregator="SUM(advertiser_quota)",
    anomaly_score_tolerance="low",
)

stg_4_fcast_qouta_dq.add_metric(
    metric_name="agency_quota",
    aggregator="SUM(agency_quota)",
    anomaly_score_tolerance="low",
)

stg_4_fcast_qouta_dq.add_metric(
    metric_name="sales_forecast",
    aggregator="SUM(sales_forecast)",
    anomaly_score_tolerance="low",
)

stg_4_fcast_qouta_dq.add_metric(
    metric_name="sales_forecast_prior",
    aggregator="SUM(sales_forecast_prior)",
    anomaly_score_tolerance="low",
)

stg_4_fcast_qouta_dq.add_metric(
    metric_name="sbg_quota", aggregator="SUM(sbg_quota)", anomaly_score_tolerance="low"
)

stg_4_fcast_qouta_dq.add_metric(
    metric_name="optimal_quota",
    aggregator="SUM(optimal_quota)",
    anomaly_score_tolerance="low",
)

stg_4_fcast_qouta_dq.add_metric(
    metric_name="liquidity_quota",
    aggregator="SUM(liquidity_quota)",
    anomaly_score_tolerance="low",
)


gms_daily_core_signal = PrestoInsertOperator(
    dep_list=[stg_4_fcast_qouta_dq],
    documentation={
        "description": """Table Name:gms_daily_core_signal
             dependency: stg_4_fcast_dq
             documentation: this signal table will land when
             all data has been loaded AND dq checks have  passed
             this will tell downstream pipelines to proceed """
    },
    table="<TABLE:gms_daily_core_signal>",
    create=sig.get_create(),
    select=sig.get_select(),
    fail_on_empty=True,
)
