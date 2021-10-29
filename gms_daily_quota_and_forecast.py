#!/usr/bin/env python3
# @lint-ignore-every DSFMT [do not copy-paste]. Format the file to remove this comment. https://fburl.com/wiki/mmzh332m
from dataswarm.operators import (
    DqOperator,
    EmailOperator,
    GlobalDefaults,
    MixedSourceDqOperator,
    PrestoInsertOperator,
    PrestoOperator,
    TableauPublishOperator,
    WaitForHiveOperator,
    DummyOperator,
)
from dataswarm.operators.runcontext import is_test
from dataswarm.operators.txtmacro import ExecuteWithMacros
from dataswarm_commons.operators import PrestoInsertOperatorWithSchema
from dataswarm_commons.operators.schema import (
    Table,
    Column,
    VARCHAR,
    DOUBLE,
    BIGINT,
    INTEGER,
    HiveAnon,
    MAP,
    BOOLEAN,
    ARRAY,
)
from edw_bir01.bi_secure.revenue.etl.weighted_facts.common import Common

from .config import (
    BpoGmsQuotaAndForecast,
    BpoGmsQuotaAndForecastFast,
    BpoGmsQuotaAndForecastFastDaily,
    BpoGmsQuotaAndForecastSnapshot,
    BpoGmsQuotaAndForecastUberFast,
)

extraemails = "sori@fb.com"
date = "<DATEID>"

snap = BpoGmsQuotaAndForecastSnapshot()
fcast = BpoGmsQuotaAndForecast()
fast = BpoGmsQuotaAndForecastFast()
fast_daily = BpoGmsQuotaAndForecastFastDaily()
uber_fast = BpoGmsQuotaAndForecastUberFast()

c = Common(oncall=None, extra_emails=[])

GlobalDefaults.set(
    user="gms_central_analytics",
    schedule="@daily",
    num_retries=1,
    depends_on_past=True,
    data_project_acl="gms_tech_data_eng",
    oncall="bp_t_gms_tech_de_bp_o",
    tag=["gms-dashboard", "seannolan", "gms-ca"],
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


bpo_gms_stg_ci = PrestoInsertOperatorWithSchema(
    dep_list=[],
    table="<TABLE:bpo_gms_dashboard_stg_ci>",
    partition={"ds": "<DATEID>"},
    create=Table(
        cols=[
            Column("interaction_date", VARCHAR, "", HiveAnon.NONE),
            Column("client_id", BIGINT, "", HiveAnon.NONE),
            Column("account_manager_fbid", BIGINT, "", HiveAnon.NONE),
            Column("total_interactions", BIGINT, "", HiveAnon.NONE),
            Column("creative", BIGINT, "", HiveAnon.NONE),
            Column("measurement", BIGINT, "", HiveAnon.NONE),
            Column("solutions_engineering", BIGINT, "", HiveAnon.NONE),
            Column("agency_opportunity", BIGINT, "", HiveAnon.NONE),
            Column("agency_planning_call", BIGINT, "", HiveAnon.NONE),
            Column("agency_planning_check_in", BIGINT, "", HiveAnon.NONE),
            Column("face_to_face", BIGINT, "", HiveAnon.NONE),
            Column("dpa", BIGINT, "", HiveAnon.NONE),
            Column("daba", BIGINT, "", HiveAnon.NONE),
        ],
        partitions=[Column("ds", VARCHAR, " Datestamp")],
        retention=2,
    ),
    select="""
        WITH pci AS (
                   SELECT DISTINCT
                       rep_id AS account_manager_fbid,
                       org_id AS client_id,
                       interaction_id,
                       interaction_date,
                       ci_topic AS contact_topic_detail,
                       contact_status AS contact_method,
                       xfn_partner AS xfn_partners_detail
                   FROM fct_gbg_scaled_pitch_detail:ad_metrics
                   WHERE ds = '<LATEST_DS:fct_gbg_scaled_pitch_detail:ad_metrics>'
                 )
                   SELECT
                      interaction_date,
                      client_id,
                      account_manager_fbid,
                      COUNT(DISTINCT(interaction_id)) AS total_interactions,
                      COUNT(
                          DISTINCT(
                                  CASE
                                    WHEN contact_topic_detail LIKE '%creative%' THEN interaction_id
                                    ELSE NULL
                                  END
                            )
                           ) AS creative,
                     COUNT(
                        DISTINCT(
                              CASE
                                WHEN LOWER(contact_topic_detail) = 'attribution_models'
                                OR LOWER(contact_topic_detail) = 'atlas' OR LOWER(contact_topic_detail)
                                = 'brand_lift_effect_insights_study' OR LOWER(contact_topic_detail)
                                = 'online_conversion_lift_study' OR LOWER(contact_topic_detail)
                                = 'dlx_roi_study' OR LOWER(contact_topic_detail)
                                = 'facebook_analytics_for_apps' OR LOWER(contact_topic_detail)
                                = 'nielsen_dar_tar_study' OR LOWER(contact_topic_detail) LIKE '%alpha_beta%'
                                OR LOWER(contact_topic_detail) = 'split_testing'
                                OR LOWER(contact_topic_detail) = 'advanced_measurement_tool'
                                OR LOWER(contact_topic_detail) = 'third_party_sales_conversion_study'
                                OR LOWER(contact_topic_detail) = 'brand_health_check'
                                OR LOWER(contact_topic_detail) = 'creative_compass'
                                OR LOWER(contact_topic_detail) = 'measurement_offline_conversion_lift'
                                OR LOWER(contact_topic_detail) = 'measurement_test_and_learn'
                                OR LOWER(contact_topic_detail) LIKE '%third_party_measurement%'
                                OR LOWER(contact_topic_detail) = 'multi_cell_offline_sales_studies'
                                OR LOWER(contact_topic_detail) = 'brand_lift_study_multi_cell'
                                OR LOWER(contact_topic_detail) = 'brand_lift_study_single_cell'
                                OR LOWER(contact_topic_detail)
                                    = 'online_conversion_lift_study_single_cell'
                                OR LOWER(contact_topic_detail) = 'online_conversion_lift_study_multi_cell'
                                OR LOWER(contact_topic_detail) = 'offline_sales_study_oracle_nielsen'
                                OR LOWER(contact_topic_detail)
                                    = 'measurement_test_and_learn_brand_lift_study' THEN interaction_id
                            ELSE NULL
                            END
                            )
                        ) AS measurement,
                    COUNT(
                        DISTINCT(
                              CASE
                                WHEN xfn_partners_detail = 'solutions_eng client_facing_support'
                                THEN interaction_id
                                ELSE NULL
                              END
                        )
                    ) AS solutions_engineering,
                    COUNT(
                        DISTINCT(
                              CASE
                                WHEN contact_topic_detail = 'agency_opportunity_qbr' THEN interaction_id
                              ELSE NULL
                             END
                       )
                    ) AS agency_opportunity,
                    COUNT(
                       DISTINCT(
                             CASE
                                WHEN contact_topic_detail = 'agency_planning_planning_and_pipeline'
                                THEN interaction_id
                              ELSE NULL
                              END
                       )
                    ) AS agency_planning_call,
                    COUNT(
                       DISTINCT(
                             CASE
                               WHEN contact_topic_detail = 'agency_opportunity_mid_quarter_check_in'
                               THEN interaction_id
                             ELSE NULL
                             END
                       )
                    ) AS agency_planning_check_in,
                    COUNT(
                       DISTINCT(
                             CASE
                               WHEN contact_method = 'In-Person Meeting' THEN interaction_id
                              ELSE NULL
                             END
                        )
                    ) AS face_to_face,
                    COUNT(
                        DISTINCT(
                              CASE
                                WHEN LOWER(contact_topic_detail) LIKE '%dynamic_product_ads%'
                                OR LOWER(contact_topic_detail) LIKE '%dynamic_travel_ads%'
                                OR LOWER(contact_topic_detail) LIKE '%dynamic_ads%' THEN interaction_id
                               ELSE NULL
                              END
                         )
                    ) AS dpa,
                    COUNT(
                        DISTINCT(
                              CASE
                                WHEN LOWER(contact_topic_detail) = 'dynamic_ads_broad_audience'
                                OR LOWER(contact_topic_detail) LIKE 'daba_targeting_best_practices%'
                                THEN interaction_id
                              ELSE NULL
                              END
                        )
                    ) AS daba
             FROM pci
             WHERE
                CAST(DATE_TRUNC('quarter', CAST(interaction_date AS DATE)) AS VARCHAR) = '<quarter_id>'
             GROUP BY 1, 2, 3
    """,
)

wait_for_gms_daily_core_signal = WaitForHiveOperator(
    dep_list=[], table="gms_daily_core_signal"
)


bpo_gms_quota_and_forecast_snapshot = PrestoInsertOperator(
    dep_list=[wait_for_gms_daily_core_signal, bpo_gms_stg_ci],
    documentation={
        "description": """Table Name:bpo_gms_quota_and_forecast_snapshot
             dependency: wait_fore_gms_daily_core_signal
             documentation: Snapshot table containing advertiser
             level revenue,forecast and quota data split by employee
             """
    },
    table="<TABLE:bpo_gms_quota_and_forecast_snapshot>",
    create=snap.get_create(),
    select=snap.get_select(),
    fail_on_empty=True,
)

snapshot_dq_goal = DqOperator(
    dep_list=[bpo_gms_quota_and_forecast_snapshot],
    documentation={
        "description": """daily dq check, tests for anomaly
                          detection between partitions """
    },
    datasource="presto",
    date_id="<DATEID>",
    table_name="<TABLE:bpo_gms_quota_and_forecast_snapshot>",
    check_name="snapshot_goaling_check",
    where_clause="date_id = '<DATEID>'",
    notify=["task", "email"],
    task_pri="high",
    user="seannolan",
    notify_cc=["mmazur@fb.com", "seannolan@fb.com"],
    column=["ds"],
)

snapshot_dq_goal.add_metric(
    metric_name="advertiser_quota",
    aggregator="SUM(advertiser_quota)",
    anomaly_score_tolerance="low",
)

snapshot_dq_goal.add_metric(
    metric_name="agency_quota",
    aggregator="SUM(agency_quota)",
    anomaly_score_tolerance="low",
)

snapshot_dq_goal.add_metric(
    metric_name="sales_forecast",
    aggregator="SUM(sales_forecast)",
    anomaly_score_tolerance="low",
)

snapshot_dq_goal.add_metric(
    metric_name="sales_forecast_prior",
    aggregator="SUM(sales_forecast_prior)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn = DqOperator(
    dep_list=[snapshot_dq_goal],
    documentation={
        "description": """daily dq check, tests for anomaly
                          detection between partitions """
    },
    datasource="presto",
    date_id="<DATEID>",
    table_name="<TABLE:bpo_gms_quota_and_forecast_snapshot>",
    check_name="snapshot_revenue_check",
    notify=["task", "email"],
    task_pri="high",
    user="seannolan",
    notify_cc=["mmazur@fb.com", "seannolan@fb.com"],
    column=["ds"],
)
snapshot_dq_revn.add_metric(
    metric_name="cq_revenue",
    aggregator="SUM(cq_revenue)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="cq_optimal",
    aggregator="SUM(cq_optimal)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="cq_liquidity",
    aggregator="SUM(cq_liquidity)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="pq_revenue",
    aggregator="SUM(pq_revenue)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="pq_optimal",
    aggregator="SUM(pq_optimal)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="pq_liquidity",
    aggregator="SUM(pq_liquidity)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="ly_revenue",
    aggregator="SUM(ly_revenue)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="ly_optimal",
    aggregator="SUM(ly_optimal)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="ly_liquidity",
    aggregator="SUM(ly_liquidity)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="lyq_revenue",
    aggregator="SUM(lyq_revenue)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="lyq_optimal",
    aggregator="SUM(lyq_optimal)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="lyq_liquidity",
    aggregator="SUM(lyq_liquidity)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="total_interactions_am",
    aggregator="SUM(total_interactions_am)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="creative_am",
    aggregator="SUM(creative_am)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="measurement_am",
    aggregator="SUM(measurement_am)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="solutions_engineering_am",
    aggregator="SUM(solutions_engineering_am)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="agency_opportunity_am",
    aggregator="SUM(agency_opportunity_am)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="agency_planning_call_am",
    aggregator="SUM(agency_planning_call_am)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="agency_planning_check_in_am",
    aggregator="SUM(agency_planning_check_in_am)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="face_to_face_am",
    aggregator="SUM(face_to_face_am)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="dpa_am", aggregator="SUM(dpa_am)", anomaly_score_tolerance="low"
)

snapshot_dq_revn.add_metric(
    metric_name="daba_am", aggregator="SUM(daba_am)", anomaly_score_tolerance="low"
)

snapshot_dq_revn.add_metric(
    metric_name="total_interactions_pm",
    aggregator="SUM(total_interactions_pm)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="creative_pm",
    aggregator="SUM(creative_pm)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="measurement_pm",
    aggregator="SUM(measurement_pm)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="solutions_engineering_pm",
    aggregator="SUM(solutions_engineering_pm)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="agency_opportunity_pm",
    aggregator="SUM(agency_opportunity_pm)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="agency_planning_call_pm",
    aggregator="SUM(agency_planning_call_pm)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="agency_planning_check_in_pm",
    aggregator="SUM(agency_planning_check_in_pm)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="face_to_face_pm",
    aggregator="SUM(face_to_face_pm)",
    anomaly_score_tolerance="low",
)

snapshot_dq_revn.add_metric(
    metric_name="dpa_pm", aggregator="SUM(dpa_pm)", anomaly_score_tolerance="low"
)

snapshot_dq_revn.add_metric(
    metric_name="daba_pm", aggregator="SUM(daba_pm)", anomaly_score_tolerance="low"
)

bpo_gms_quota_and_forecast = PrestoInsertOperator(
    dep_list=[snapshot_dq_revn],
    documentation={
        "description": """Table Name:bpo_gms_quota_and_forecast
             dependency: bpo_gms_quota_and_forecast_snapshot
             documentation:Staging table that stores a single partition of data
             to be loaded into Tableau """
    },
    table="<TABLE:bpo_gms_quota_and_forecast>",
    create=fcast.get_create(),
    select=fcast.get_select(),
    fail_on_empty=True,
)

delete_bpo_gms_quota_and_forecast = PrestoOperator(
    dep_list=[bpo_gms_quota_and_forecast],
    documentation={
        "Description": """ deletes all partitions from
                           bpo_gms_quota_and_forecast that are NOT  equal to
                           '<DATEID>'"""
    },
    query=fcast.get_delete(),
)

bpo_gms_quota_and_forecast_fast = PrestoInsertOperatorWithSchema(
    dep_list=[snapshot_dq_revn],
    documentation={
        "description": """Table Name:bpo_gms_quota_and_forecast_fast
             dependency: bpo_gms_quota_and_forecast_snapshot
             documentation: lite table designed for frontend use """
    },
    table="<TABLE:bpo_gms_quota_and_forecast_fast>",
    create=fast.get_create(),
    select=fast.get_select(),
    fail_on_empty=True,
)

delete_bpo_gms_quota_and_forecast_fast = PrestoOperator(
    dep_list=[bpo_gms_quota_and_forecast_fast],
    documentation={
        "description": """deletes all partitions from bpo_gms_quota_and_forecast_fast
                       that are NOT equal to '<DATEID>' """
    },
    query=fast.get_delete(),
)

bpo_gms_quota_and_forecast_uber_fast = PrestoInsertOperatorWithSchema(
    dep_list=[
        bpo_gms_quota_and_forecast_fast,
    ],
    documentation={
        "description": """Table Name:bpo_gms_quota_and_forecast_fast
             dependency: bpo_gms_quota_and_forecast_snapshot
             documentation: lite table designed for frontend use """
    },
    table="<TABLE:bpo_gms_quota_and_forecast_uber_fast>",
    create=uber_fast.get_create(),
    select=uber_fast.get_select(),
    fail_on_empty=True,
)

delete_bpo_gms_quota_and_forecast_uber_fast = PrestoOperator(
    dep_list=[bpo_gms_quota_and_forecast_uber_fast],
    documentation={
        "description": """deletes all partitions from bpo_gms_quota_and_forecast_fast
                       that are NOT equal to '<DATEID>' """
    },
    query=uber_fast.get_delete(),
)

bpo_gms_quota_and_forecast_fast_daily = PrestoInsertOperator(
    dep_list=[snapshot_dq_revn],
    documentation={
        "description": """Table Name:bpo_gms_quota_and_forecast_fast_daily
        dependency: bpo_gms_quota_and_forecast_snapshot
        documentation: daily level lite table for quota,forecast and revenue data"""
    },
    table="<TABLE:bpo_gms_quota_and_forecast_fast_daily>",
    create=fast_daily.get_create(),
    select=fast_daily.get_select(),
    fail_on_empty=True,
)


delete_bpo_gms_quota_and_forecast_fast_daily = PrestoOperator(
    dep_list=[bpo_gms_quota_and_forecast_fast_daily],
    documentation={
        "description": """deletes all partitions from bpo_gms_quota_and_forecast_fast_daily
                that are NOT equal to '<DATEID>' """
    },
    query=fast_daily.get_delete(),
)

revenue_segment = [
    "GBG In-Market",
    "GBG Unmanaged",
    "SBG Outsourced Engaged",
    "GPA Unmanaged",
    "SBG Outsourced Eligible Net",
    "GPA Managed",
    "SBG Ineligible",
    "GBG Scaled",
    "SBG Longtail Net",
]

daily_part = {}
for segment in revenue_segment:

    clean_segment = segment.replace(
        " ", "_"
    ).lower()  # stripped of whitespace and capital letters for task name

    daily_part[
        "insert_bpo_gms_quota_and_forecast_fast_daily_part_{rev_seg}".format(
            rev_seg=clean_segment
        )
    ] = PrestoInsertOperatorWithSchema(
        dep_list=[delete_bpo_gms_quota_and_forecast_fast_daily],
        table="<TABLE:bpo_gms_quota_and_forecast_fast_daily_part>",
        partition={"ds": "<DATEID>", "revenue_segment": segment},
        create=Table(
            cols=[
                Column("date_id", "VARCHAR", "", HiveAnon.NONE),
                Column("advertiser_coverage_model_daa", "VARCHAR", "", HiveAnon.NONE),
                Column("advertiser_program_daa", "VARCHAR", "", HiveAnon.NONE),
                Column("l12_reporting_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l10_reporting_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l8_reporting_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l6_reporting_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l4_reporting_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l2_reporting_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l12_reporting_terr_mgr", "VARCHAR", "", HiveAnon.NONE),
                Column("l10_reporting_terr_mgr", "VARCHAR", "", HiveAnon.NONE),
                Column("l8_reporting_terr_mgr", "VARCHAR", "", HiveAnon.NONE),
                Column("l6_reporting_terr_mgr", "VARCHAR", "", HiveAnon.NONE),
                Column("l12_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l10_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l8_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l6_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l4_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l2_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("advertiser_vertical", "VARCHAR", "", HiveAnon.NONE),
                Column("cp_username", "VARCHAR", "", HiveAnon.NONE),
                Column("cp_manager_username", "VARCHAR", "", HiveAnon.NONE),
                Column("am_username", "VARCHAR", "", HiveAnon.NONE),
                Column("am_manager_username", "VARCHAR", "", HiveAnon.NONE),
                Column("pm_username", "VARCHAR", "", HiveAnon.NONE),
                Column("pm_manager_username", "VARCHAR", "", HiveAnon.NONE),
                Column("ap_username", "VARCHAR", "", HiveAnon.NONE),
                Column("ap_manager_username", "VARCHAR", "", HiveAnon.NONE),
                Column("asofdate", "VARCHAR", "", HiveAnon.NONE),
                Column("cq_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("pq_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("ly_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("l2y_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("lyq_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("l2yq_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("advertiser_quota", "DOUBLE", "", HiveAnon.NONE),
                Column("agency_quota", "DOUBLE", "", HiveAnon.NONE),
                Column("sales_forecast", "DOUBLE", "", HiveAnon.NONE),
                Column("sales_forecast_prior", "DOUBLE", "", HiveAnon.NONE),
                Column("segmentation", "VARCHAR", "", HiveAnon.NONE),
                Column("l12_manager_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l10_manager_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l8_manager_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l6_manager_agency_territory", "VARCHAR", "", HiveAnon.NONE),
                Column(
                    "csm_username",
                    "VARCHAR",
                    "UnixName for Client Solutions Manager",
                    HiveAnon.NONE,
                ),
                Column(
                    "csm_manager_username",
                    "VARCHAR",
                    "UnixName for a CSMs manager ",
                    HiveAnon.NONE,
                ),
                Column("asm_username", "VARCHAR", "UnixName for ASM", HiveAnon.NONE),
                Column(
                    "asm_manager_username",
                    "VARCHAR",
                    "UnixName for a ASMs manager",
                    HiveAnon.NONE,
                ),
                Column(
                    "rp_username",
                    "VARCHAR",
                    "UnixName for reseller partner",
                    HiveAnon.NONE,
                ),
                Column(
                    "rp_manager_username",
                    "VARCHAR",
                    "UnixName of an RPs manager",
                    HiveAnon.NONE,
                ),
                Column("sales_adv_country_group", "VARCHAR", "", HiveAnon.NONE),
                Column("sales_adv_subregion", "VARCHAR", "", HiveAnon.NONE),
                Column("sales_adv_region", "VARCHAR", "", HiveAnon.NONE),
                Column("market", "VARCHAR", "", HiveAnon.NONE),
                Column("advertiser_country", "VARCHAR", "", HiveAnon.NONE),
                Column("l12_reseller_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l10_reseller_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l8_reseller_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l6_reseller_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l4_reseller_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l2_reseller_territory", "VARCHAR", "", HiveAnon.NONE),
                Column("l12_reseller_terr_mgr", "VARCHAR", "", HiveAnon.NONE),
                Column("l10_reseller_terr_mgr", "VARCHAR", "", HiveAnon.NONE),
                Column("l8_reseller_terr_mgr", "VARCHAR", "", HiveAnon.NONE),
                Column("l6_reseller_terr_mgr", "VARCHAR", "", HiveAnon.NONE),
                Column("reseller_quota", "DOUBLE", "", HiveAnon.NONE),
                Column("adv_exclusion", "INTEGER", "", HiveAnon.NONE),
                Column("cq_optimal", "DOUBLE", "", HiveAnon.NONE),
                Column("pq_optimal", "DOUBLE", "", HiveAnon.NONE),
                Column("ly_optimal", "DOUBLE", "", HiveAnon.NONE),
                Column("lyq_optimal", "DOUBLE", "", HiveAnon.NONE),
                Column("optimal_quota", "DOUBLE", "", HiveAnon.NONE),
                Column("agc_optimal_quota", "DOUBLE", "", HiveAnon.NONE),
                Column("ts", "VARCHAR", "", HiveAnon.NONE),
                Column("l7d_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("l7d_revenue_prior", "DOUBLE", "", HiveAnon.NONE),
                Column("l7d_avg_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("l7d_avg_revenue_prior", "DOUBLE", "", HiveAnon.NONE),
                Column("legacy_advertiser_vertical", "VARCHAR", "", HiveAnon.NONE),
                Column(
                    "ultimate_parent_vertical_name_v2", "VARCHAR", "", HiveAnon.NONE
                ),
                Column("program", "VARCHAR", "", HiveAnon.NONE),
                Column("dr_resilience_goal", "DOUBLE", "", HiveAnon.NONE),
                Column("dr_resilient_cq", "DOUBLE", "", HiveAnon.NONE),
                Column("dr_resilient_pq", "DOUBLE", "", HiveAnon.NONE),
                Column("dr_resilient_ly", "DOUBLE", "", HiveAnon.NONE),
                Column("dr_resilient_lyq", "DOUBLE", "", HiveAnon.NONE),
                Column("cq_product_resilient_rec_rev", "DOUBLE", "", HiveAnon.NONE),
                Column("cq_ebr_usd_rec_rev", "DOUBLE", "", HiveAnon.NONE),
                Column("cq_capi_ebr_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("pq_product_resilient_rec_rev", "DOUBLE", "", HiveAnon.NONE),
                Column("pq_ebr_usd_rec_rev", "DOUBLE", "", HiveAnon.NONE),
                Column("pq_capi_ebr_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("ly_product_resilient_rec_rev", "DOUBLE", "", HiveAnon.NONE),
                Column("ly_ebr_usd_rec_rev", "DOUBLE", "", HiveAnon.NONE),
                Column("ly_capi_ebr_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("lyq_product_resilient_rec_rev", "DOUBLE", "", HiveAnon.NONE),
                Column("lyq_ebr_usd_rec_rev", "DOUBLE", "", HiveAnon.NONE),
                Column("lyq_capi_ebr_revenue", "DOUBLE", "", HiveAnon.NONE),
                Column("subsegment", "VARCHAR", "", HiveAnon.NONE),
                Column("sales_forecast_prior_2w", "DOUBLE", "", HiveAnon.NONE),
            ],
            partitions=[
                Column("ds", "VARCHAR", "", HiveAnon.NONE),
                Column("revenue_segment", "VARCHAR", "", HiveAnon.NONE),
            ],
            comment="",
            initial_retention=90,
        ),
        select="""
            SELECT
                    date_id ,
                    advertiser_coverage_model_daa ,
                    advertiser_program_daa ,
                    l12_reporting_territory ,
                    l10_reporting_territory ,
                    l8_reporting_territory ,
                    l6_reporting_territory ,
                    l4_reporting_territory ,
                    l2_reporting_territory ,
                    l12_reporting_terr_mgr ,
                    l10_reporting_terr_mgr ,
                    l8_reporting_terr_mgr ,
                    l6_reporting_terr_mgr ,
                    l12_agency_territory ,
                    l10_agency_territory ,
                    l8_agency_territory ,
                    l6_agency_territory ,
                    l4_agency_territory ,
                    l2_agency_territory ,
                    advertiser_vertical ,
                    cp_username ,
                    cp_manager_username ,
                    am_username ,
                    am_manager_username ,
                    pm_username ,
                    pm_manager_username ,
                    ap_username ,
                    ap_manager_username ,
                    asofdate ,
                    cq_revenue ,
                    pq_revenue ,
                    ly_revenue ,
                    l2y_revenue ,
                    lyq_revenue ,
                    l2yq_revenue ,
                    advertiser_quota ,
                    agency_quota ,
                    sales_forecast ,
                    sales_forecast_prior ,
                    segmentation ,
                    l12_manager_agency_territory ,
                    l10_manager_agency_territory ,
                    l8_manager_agency_territory ,
                    l6_manager_agency_territory ,
                    csm_username ,
                    csm_manager_username ,
                    asm_username ,
                    asm_manager_username ,
                    rp_username ,
                    rp_manager_username  ,
                    sales_adv_country_group ,
                    sales_adv_subregion ,
                    sales_adv_region ,
                    market ,
                    advertiser_country ,
                    l12_reseller_territory ,
                    l10_reseller_territory ,
                    l8_reseller_territory ,
                    l6_reseller_territory ,
                    l4_reseller_territory ,
                    l2_reseller_territory ,
                    l12_reseller_terr_mgr ,
                    l10_reseller_terr_mgr ,
                    l8_reseller_terr_mgr ,
                    l6_reseller_terr_mgr ,
                    reseller_quota ,
                    adv_exclusion ,
                    cq_optimal ,
                    pq_optimal ,
                    ly_optimal ,
                    lyq_optimal ,
                    optimal_quota ,
                    agc_optimal_quota ,
                    ts ,
                    l7d_revenue ,
                    l7d_revenue_prior ,
                    l7d_avg_revenue ,
                    l7d_avg_revenue_prior ,
                    NULL as legacy_advertiser_vertical ,
                    ultimate_parent_vertical_name_v2,
                    program,
                    dr_resilience_goal,
                    dr_resilient_cq ,
                    dr_resilient_pq ,
                    dr_resilient_ly ,
                    dr_resilient_lyq,
                    cq_product_resilient_rec_rev,
                    cq_ebr_usd_rec_rev,
                    cq_capi_ebr_revenue,
                    pq_product_resilient_rec_rev,
                    pq_ebr_usd_rec_rev,
                    pq_capi_ebr_revenue,
                    ly_product_resilient_rec_rev,
                    ly_ebr_usd_rec_rev,
                    ly_capi_ebr_revenue,
                    lyq_product_resilient_rec_rev,
                    lyq_ebr_usd_rec_rev,
                    lyq_capi_ebr_revenue,
                    subsegment,
                    sales_forecast_prior_2w,
                    ds,
                    revenue_segment


                            FROM <TABLE:bpo_gms_quota_and_forecast_fast_daily>

                        WHERE ds ='<DATEID>'

                        and revenue_segment = '{rev_seg}'
        """.format(
            rev_seg=segment
        ),
    )


delete_bpo_gms_quota_and_forecast_fast_daily_part = PrestoOperator(
    dep_list=[daily_part],
    documentation={
        "description": """deletes all partitions from bpo_gms_quota_and_forecast_fast_daily
                that are NOT equal to '<DATEID>' """
    },
    query="""
            DELETE FROM <TABLE:bpo_gms_quota_and_forecast_fast_daily_part>

            where ds <> '<DATEID>'



             """,
)

if is_test():
    tableau_refresh_bpo_gms_quota_and_forecast_fast_daily = DummyOperator(
        dep_list=[delete_bpo_gms_quota_and_forecast_fast_daily_part]
    )
else:
    tableau_refresh_bpo_gms_quota_and_forecast_fast_daily = TableauPublishOperator(
        dep_list=[
            delete_bpo_gms_quota_and_forecast_fast_daily_part,
        ],
        refresh_cfg_id=28277,
        extra_emails=[
            extraemails,
        ],
        num_retries=10,
    )

if is_test():
    tableau_refresh_bpo_gms_quota_and_forecast_fast = DummyOperator(
        dep_list=[
            delete_bpo_gms_quota_and_forecast_fast,
            tableau_refresh_bpo_gms_quota_and_forecast_fast_daily,
        ]
    )
else:
    tableau_refresh_bpo_gms_quota_and_forecast_fast = TableauPublishOperator(
        dep_list=[
            delete_bpo_gms_quota_and_forecast_fast,
            tableau_refresh_bpo_gms_quota_and_forecast_fast_daily,
        ],
        refresh_cfg_id=28251,
        extra_emails=[
            extraemails,
        ],
        num_retries=10,
    )

if is_test():
    tableau_refresh_bpo_gms_quota_and_forecast_uber_fast = DummyOperator(
        dep_list=[
            delete_bpo_gms_quota_and_forecast_uber_fast,
            tableau_refresh_bpo_gms_quota_and_forecast_fast,
        ]
    )
else:
    tableau_refresh_bpo_gms_quota_and_forecast_uber_fast = TableauPublishOperator(
        dep_list=[
            delete_bpo_gms_quota_and_forecast_uber_fast,
            tableau_refresh_bpo_gms_quota_and_forecast_fast,
        ],
        refresh_cfg_id=28501,
        extra_emails=[
            extraemails,
        ],
        num_retries=10,
    )
