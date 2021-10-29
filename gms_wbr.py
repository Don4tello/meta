#!/usr/bin/env python3
# @lint-ignore-every DSFMT [do not copy-paste]. Format the file to remove this comment. https://fburl.com/wiki/mmzh332m

# See https://our.intern.facebook.com/intern/dex/dataswarm-operators/
# for a list of all dataswarm operators and usage information.
from dataswarm.operators import (
    GlobalDefaults,
    PrestoInsertOperator,
    PrestoOperator,
    TableauPublishOperator,
    WaitForHiveOperator,
)
from dataswarm.operators.runcontext import is_test
from dataswarm.operators.txtmacro import ExecuteWithMacros
from edw_bir01.bi_secure.revenue.etl.weighted_facts.common import Common


GlobalDefaults.set(
    schedule="@daily",
    data_project_acl="gms_central_analytics",
    user="mmazur",
    oncall="gms_central_analytics",
    depends_on_past=False,
    fail_on_empty=True,
    max_concurrent=99,
)

c = Common(
    oncall="gms_central_analytics",
    secure_group="gms_central_analytics",
    extra_emails=[],
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

wait_for_bpo_coverage_asis_stg_1 = WaitForHiveOperator(
    dep_list=[],
    table="bpo_coverage_asis_stg_1",
    use_future=False,
    fail_on_future=False,
)

wait_for_staging_bpt_fct_l4_gms_rec_rev_data = WaitForHiveOperator(
    dep_list=[],
    table="staging_bpt_fct_l4_gms_rec_rev_data",
    use_future=False,
    fail_on_future=False,
)

wait_for_bpo_as_is_coverage = WaitForHiveOperator(
    dep_list=[],
    table="bpo_as_is_coverage",
    use_future=False,
    fail_on_future=False,
)

wait_for_acdp_dim_l45_organization = WaitForHiveOperator(
    dep_list=[],
    table="acdp_dim_l45_organization:ad_reporting",
    use_future=False,
    fail_on_future=False,
)

wait_for_d_geo_country = WaitForHiveOperator(
    dep_list=[],
    table="d_geo_country_signal",
    use_future=False,
    fail_on_future=False,
)

wait_for_organization_to_customer_asset_managers = WaitForHiveOperator(
    dep_list=[],
    table="organization_to_customer_asset_managers:bi",
    use_future=False,
    fail_on_future=False,
)

gms_wbr_flat = PrestoInsertOperator(
    dep_list=[
        wait_for_bpo_coverage_asis_stg_1,
        wait_for_staging_bpt_fct_l4_gms_rec_rev_data,
        wait_for_bpo_as_is_coverage,
        wait_for_acdp_dim_l45_organization,
        wait_for_d_geo_country,
        wait_for_organization_to_customer_asset_managers,
    ],
    table="<TABLE:gms_wbr_flat>",
    partition="ds=<DATEID>",
    documentation={"description": """Staging Table prior to RTM Split"""},
    create=r"""
    CREATE TABLE IF NOT EXISTS <TABLE:gms_wbr_flat> (
        ultimate_parent_fbid VARCHAR,
        ultimate_parent_name VARCHAR,
        planning_agency_ult_fbid VARCHAR,
        planning_agency_ult_name VARCHAR,
        revenue_segment VARCHAR,
        program_agency VARCHAR,
        vertical VARCHAR,
        sub_vertical VARCHAR,
        l12_agency_territory VARCHAR,
        l10_agency_territory VARCHAR,
        l8_agency_territory VARCHAR,
        l6_agency_territory VARCHAR,
        l4_agency_territory VARCHAR,
        l12_reporting_territory VARCHAR,
        l10_reporting_territory VARCHAR,
        l8_reporting_territory VARCHAR,
        l6_reporting_territory VARCHAR,
        l4_reporting_territory VARCHAR,
        l12_reseller_territory VARCHAR,
        l10_reseller_territory VARCHAR,
        l8_reseller_territory VARCHAR,
        l6_reseller_territory VARCHAR,
        l4_reseller_territory VARCHAR,
        asofdate VARCHAR,
        quarter_id VARCHAR,
        next_quarter_id VARCHAR,
        days_left_in_quarter BIGINT,
        days_left_in_quarter_prior BIGINT,
        days_total_in_quarter BIGINT,
        days_closed_in_quarter BIGINT,
        cq_revenue DOUBLE,
        cq_revenue_prior DOUBLE,
        l7d_cq_revenue DOUBLE,
        l7d_cq_revenue_prior DOUBLE,
        pq_revenue DOUBLE,
        pq_revenue_qtd DOUBLE,
        pq_revenue_qtd_prior DOUBLE,
        ly_revenue_qtd DOUBLE,
        ly_revenue_qtd_prior DOUBLE,
        ly_revenue DOUBLE,
        L7d_ly_revenue DOUBLE,
        L7d_ly_revenue_prior DOUBLE,
        advertiser_quota DOUBLE,
        agency_quota DOUBLE,
        reseller_quota DOUBLE,
        sales_forecast DOUBLE,
        sales_forecast_prior DOUBLE,
        run_rate_forecast DOUBLE,
        run_rate_forecast_prior DOUBLE,
        straightline_forecast DOUBLE,
        straightline_forecast_prior DOUBLE,
        cq_optimal DOUBLE,
        cq_optimal_prior DOUBLE,
        L7d_cq_optimal DOUBLE,
        L7d_cq_optimal_prior DOUBLE,
        ly_optimal_qtd DOUBLE,
        ly_optimal_qtd_prior DOUBLE,
        ly_optimal DOUBLE,
        L7d_ly_optimal DOUBLE,
        L7d_ly_optimal_prior DOUBLE,
        optimal_goal DOUBLE,
        agc_optimal_goal DOUBLE,
        legal_revn DOUBLE,
        resilient_legal_revn DOUBLE,
        hq_country_adv VARCHAR,
        hq_region_adv VARCHAR,
        is_gcc BOOLEAN,
        is_gcm BOOLEAN,
        dr_resilience_goal DOUBLE,
        cq_resilient DOUBLE,
        cq_resilient_prior DOUBLE,
        L7d_cq_resilient DOUBLE,
        L7d_cq_resilient_prior DOUBLE,
        cq_ebr_revenue DOUBLE,
        cq_ebr_revenue_prior DOUBLE,
        L7d_cq_ebr_revenue DOUBLE,
        L7d_cq_ebr_revenue_prior DOUBLE,
        cq_capi_ebr_revenue DOUBLE,
        cq_capi_ebr_revenue_prior DOUBLE,
        L7d_cq_capi_ebr_revenue DOUBLE,
        L7d_cq_capi_ebr_revenue_prior DOUBLE,
        ly2_revenue_qtd DOUBLE,
        ly2_revenue_qtd_prior DOUBLE,
        ly2_revenue DOUBLE,
        L7d_ly2_revenue DOUBLE,
        L7d_ly2_revenue_prior DOUBLE,
        ds VARCHAR
    )
    WITH (
        format = 'DWRF',
        oncall = 'gms_ca',
        partitioned_by = ARRAY['ds'],
        retention_days = <RETENTION:90>
    )
    """,
    select=r"""
    WITH

    quarter_date AS (
        SELECT
            CAST('<quarter_id>' AS DATE) AS quarter_id,
            (CAST('<quarter_id>' AS DATE) + INTERVAL '3' MONTH) next_quarter_id,
            MAX(CAST(date_id AS DATE)) asofdate

        FROM staging_bpt_fct_l4_gms_rec_rev_data
        WHERE
            asis_rec_rev IS NOT NULL
            AND ds = '<DATEID>'
        GROUP BY
            1, 2
    ),
    quarter_dates AS (
        SELECT

            quarter_id,
            next_quarter_id,
            asofdate,
            DATE_DIFF('day',(asofdate), next_quarter_id - INTERVAL '1' DAY) days_left_in_quarter,
            DATE_DIFF(
                'day',
                (asofdate - INTERVAL '7' DAY),
                next_quarter_id - INTERVAL '1' DAY
            ) days_left_in_quarter_prior,
            DATE_DIFF('day', quarter_id, next_quarter_id) days_total_in_quarter,
            DATE_DIFF('day', quarter_id, asofdate + INTERVAL '1' DAY) days_closed_in_quarter,
            DATE_DIFF('day', quarter_id, asofdate - INTERVAL '7' DAY) days_closed_in_quarter_prior

        FROM quarter_date
    ),
    fraud_rev AS (

        SELECT
            ad_account_id,
            'true' fraud
        FROM bpo_as_is_coverage

        WHERE
            is_fcast_eligible = FALSE
            AND ds = '<DATEID>'

    )

    ,
    cq_rev AS (
        SELECT
            rev.account_id ad_account_id,

            -- CQ REV
            SUM(asis_rec_rev) AS cq_revenue,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '6' DAY
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) L7d_cq_revenue,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) cq_revenue_prior,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '13' DAY
                    ) AND CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) L7d_cq_revenue_prior,

            -- CQ OPT REV
            SUM(asis_optimal_revn) AS cq_optimal,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '6' DAY
                    ) THEN asis_optimal_revn
                    ELSE NULL
                END
            ) L7d_cq_optimal,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN asis_optimal_revn
                    ELSE NULL
                END
            ) cq_optimal_prior,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '13' DAY
                    ) AND CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN asis_optimal_revn
                    ELSE NULL
                END
            ) L7d_cq_optimal_prior,

            -- CQ RES REV
            SUM(product_resilient_rec_rev) cq_resilient,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '6' DAY
                    ) THEN product_resilient_rec_rev
                    ELSE NULL
                END
            ) L7d_cq_resilient,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN product_resilient_rec_rev
                    ELSE NULL
                END
            ) cq_resilient_prior,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '13' DAY
                    ) AND CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN product_resilient_rec_rev
                    ELSE NULL
                END
            ) L7d_cq_resilient_prior,

            -- CQ EBR REV
            SUM(ebr_usd_rec_rev) cq_ebr_revenue,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '6' DAY
                    ) THEN ebr_usd_rec_rev
                    ELSE NULL
                END
            ) L7d_cq_ebr_revenue,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN ebr_usd_rec_rev
                    ELSE NULL
                END
            ) cq_ebr_revenue_prior,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '13' DAY
                    ) AND CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN ebr_usd_rec_rev
                    ELSE NULL
                END
            ) L7d_cq_ebr_revenue_prior,

            -- CQ CAPI EBR REV
            SUM(capi_ebr_revenue) cq_capi_ebr_revenue,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '6' DAY
                    ) THEN capi_ebr_revenue
                    ELSE NULL
                END
            ) L7d_cq_capi_ebr_revenue,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN capi_ebr_revenue
                    ELSE NULL
                END
            ) cq_capi_ebr_revenue_prior,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '13' DAY
                    ) AND CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY
                    ) THEN capi_ebr_revenue
                    ELSE NULL
                END
            ) L7d_cq_capi_ebr_revenue_prior

        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id
        WHERE
            ds = '<DATEID>'
            AND quarter_id = '<quarter_id>'
            AND fraud_rev.fraud IS NULL
        GROUP BY
            1
    ),
    pq_rev AS (
        SELECT
            rev.account_id ad_account_id,
            SUM(asis_rec_rev) AS pq_revenue,
            SUM(
                CASE
                    WHEN date_id <= CAST(
                        (CAST('<DATEID>' AS DATE) - INTERVAL '3' MONTH) AS VARCHAR
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) pq_revenue_qtd,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) <= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '3' MONTH) - INTERVAL '7' DAY
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) pq_revenue_qtd_prior

        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id

        WHERE
            ds = '<DATEID>'
            AND quarter_id = CAST(
                (CAST('<quarter_id>' AS DATE) - INTERVAL '3' MONTH) AS VARCHAR
            )
            AND fraud_rev.fraud IS NULL
        GROUP BY
            1
    ),
    ly_rev AS (
        SELECT
            rev.account_id ad_account_id,
            SUM(asis_rec_rev) AS ly_revenue,
            SUM(
                CASE
                    WHEN date_id <= CAST(
                        (CAST('<DATEID>' AS DATE) - INTERVAL '12' MONTH) AS VARCHAR
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) AS ly_revenue_qtd,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) <= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY) - INTERVAL '12' MONTH
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) ly_revenue_qtd_prior,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '6' DAY) - INTERVAL '12' MONTH
                    ) AND CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '12' MONTH
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) L7d_ly_revenue,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '13' DAY) - INTERVAL '12' MONTH
                    ) AND CAST(date_id AS DATE) <= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY) - INTERVAL '12' MONTH
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) L7d_ly_revenue_prior,
            SUM(asis_optimal_revn) AS ly_optimal,
            SUM(
                CASE
                    WHEN date_id <= CAST(
                        (CAST('<DATEID>' AS DATE) - INTERVAL '12' MONTH) AS VARCHAR
                    ) THEN asis_optimal_revn
                    ELSE NULL
                END
            ) AS ly_optimal_qtd,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) <= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY) - INTERVAL '12' MONTH
                    ) THEN asis_optimal_revn
                    ELSE NULL
                END
            ) ly_optimal_qtd_prior,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '6' DAY) - INTERVAL '12' MONTH
                    ) AND CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '12' MONTH
                    ) THEN asis_optimal_revn
                    ELSE NULL
                END
            ) L7d_ly_optimal,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '13' DAY) - INTERVAL '12' MONTH
                    ) AND CAST(date_id AS DATE) <= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY) - INTERVAL '12' MONTH
                    ) THEN asis_optimal_revn
                    ELSE NULL
                END
            ) L7d_ly_optimal_prior
        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id

        WHERE
            ds = '<DATEID>'
            AND quarter_id = CAST(
                (CAST('<quarter_id>' AS DATE) - INTERVAL '1' YEAR) AS VARCHAR
            )
            AND fraud_rev.fraud IS NULL
        GROUP BY
            1
    ),

    lyq_rev AS (
        SELECT
            rev.account_id ad_account_id,
            SUM(
                CASE
                    WHEN date_id <= CAST(
                        (CAST('<DATEID>' AS DATE) - INTERVAL '12' MONTH) AS VARCHAR
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) AS lyq_revenue
        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id

        WHERE
            ds = '<DATEID>'
            AND quarter_id = CAST(
                (CAST('<quarter_id>' AS DATE) - INTERVAL '1' YEAR) AS VARCHAR
            )
            AND fraud_rev.fraud IS NULL

        GROUP BY
            1
    ),
    gcc AS (
        SELECT
            ultimate_parent_fbid,
            territory_l4_name l4_reporting_territory,
            TRUE is_gcc
        FROM gcc_quarterly_l8_view_presto
        WHERE
            quarter_id = '<quarter_id>'
        GROUP BY
            1, 2, 3

    ),
    gcm AS (
        SELECT
            ultimate_parent_fbid,
            territory_l4_name l4_reporting_territory,
            TRUE is_gcm
        FROM gcm_milestone_model_4qtr_0_view_presto
        WHERE
            quarter_id = '<quarter_id>'
        GROUP BY
            1, 2, 3

    )

    ,
    hq AS (
        SELECT
            a.org_fbid,
            ult_hq.fb_subregion AS hq_country_adv,
            ult_hq.sales_adv_region AS hq_region_adv,
            a.ds
        FROM acdp_dim_l45_organization:ad_reporting a
        LEFT JOIN (
            SELECT DISTINCT
                a.org_fbid,
                a.org_primary_billing_address_country,
                a.org_hq_address_country
            FROM acdp_dim_l45_organization:ad_reporting a
            WHERE
                a.ds = '<DATEID>'
        ) f
            ON a.ultimate_parent_org_fbid = f.org_fbid

        LEFT JOIN d_geo_country org_billing
            ON LOWER(TRIM(a.org_primary_billing_address_country)) = LOWER(
                TRIM(org_billing.country_abbr)
            )
            AND org_billing.ds = '<DATEID>'
        LEFT JOIN d_geo_country org_hq
            ON LOWER(TRIM(a.org_hq_address_country)) = LOWER(
                TRIM(org_hq.country_abbr)
            )
            AND org_hq.ds = '<DATEID>'
        LEFT JOIN d_geo_country gms
            ON UPPER(
                TRIM(
                    IF(
                        UPPER(a.org_hq_address_country) <> 'NOT AVAIL',
                        a.org_hq_address_country,
                        a.org_primary_billing_address_country
                    )
                )
            ) = UPPER(TRIM(gms.country_abbr))
            AND gms.ds = '<DATEID>'
        LEFT JOIN d_geo_country ult_billing
            ON LOWER(TRIM(f.org_primary_billing_address_country)) = LOWER(
                TRIM(ult_billing.country_abbr)
            )
            AND ult_billing.ds = '<DATEID>'
        LEFT JOIN d_geo_country ult_hq
            ON LOWER(TRIM(f.org_hq_address_country)) = LOWER(
                TRIM(ult_hq.country_abbr)
            )
            AND ult_hq.ds = '<DATEID>'

        WHERE
            a.ds = '<DATEID>'
            AND f.org_hq_address_country IS NOT NULL

    ),
    ly2_rev AS (
        SELECT
            rev.account_id ad_account_id,
            SUM(asis_rec_rev) AS ly2_revenue,
            SUM(
                CASE
                    WHEN date_id <= CAST(
                        (CAST('<DATEID>' AS DATE) - INTERVAL '24' MONTH) AS VARCHAR
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) AS ly2_revenue_qtd,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) <= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY) - INTERVAL '24' MONTH
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) ly2_revenue_qtd_prior,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '6' DAY) - INTERVAL '24' MONTH
                    ) AND CAST(date_id AS DATE) <= (
                        CAST('<DATEID>' AS DATE) - INTERVAL '24' MONTH
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) L7d_ly2_revenue,
            SUM(
                CASE
                    WHEN CAST(date_id AS DATE) >= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '13' DAY) - INTERVAL '24' MONTH
                    ) AND CAST(date_id AS DATE) <= (
                        (CAST('<DATEID>' AS DATE) - INTERVAL '7' DAY) - INTERVAL '24' MONTH
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) L7d_ly2_revenue_prior

        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id

        WHERE
            ds = '<DATEID>'
            AND quarter_id = CAST(
                (CAST('<quarter_id>' AS DATE) - INTERVAL '2' YEAR) AS VARCHAR
            )
            AND fraud_rev.fraud IS NULL
        GROUP BY
            1
    ),

    lyq2_rev AS (
        SELECT
            rev.account_id ad_account_id,
            SUM(
                CASE
                    WHEN date_id <= CAST(
                        (CAST('<DATEID>' AS DATE) - INTERVAL '24' MONTH) AS VARCHAR
                    ) THEN asis_rec_rev
                    ELSE NULL
                END
            ) AS lyq2_revenue
        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id

        WHERE
            ds = '<DATEID>'
            AND quarter_id = CAST(
                (CAST('<quarter_id>' AS DATE) - INTERVAL '2' YEAR) AS VARCHAR
            )
            AND fraud_rev.fraud IS NULL

        GROUP BY
            1
    )

    SELECT
        coverage.ultimate_parent_fbid,
        coverage.ultimate_parent_name,
        coverage.planning_agency_ult_fbid,
        coverage.planning_agency_ult_name,
        COALESCE(coverage.revenue_segment, coverage.program) revenue_segment,
        coverage.program_agency,
        coverage.advertiser_vertical vertical,
        coverage.advertiser_sub_vertical sub_vertical,
        coverage.l12_agency_territory,
        coverage.l10_agency_territory,
        coverage.l8_agency_territory,
        coverage.l6_agency_territory,
        coverage.l4_agency_territory,
        coverage.l12_reporting_territory,
        coverage.l10_reporting_territory,
        coverage.l8_reporting_territory,
        coverage.l6_reporting_territory,
        coverage.l4_reporting_territory,
        coverage.l12_reseller_territory,
        coverage.l10_reseller_territory,
        coverage.l8_reseller_territory,
        coverage.l6_reseller_territory,
        coverage.l4_reseller_territory

        -- quarter dates
        , CAST(asofdate AS VARCHAR) asofdate,
        CAST(quarter_id AS VARCHAR) quarter_id,
        CAST(next_quarter_id AS VARCHAR) next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter

        -- 04-02-2021
        , hq.hq_country_adv,
        hq.hq_region_adv,
        COALESCE(gcc.is_gcc, FALSE) is_gcc,
        COALESCE(gcm.is_gcm, FALSE) is_gcm

        --CQ Metrics
        , SUM(cq_rev.cq_revenue * coverage.split) cq_revenue,
        SUM(cq_rev.cq_revenue_prior * coverage.split) cq_revenue_prior,
        SUM(cq_rev.L7d_cq_revenue * coverage.split) L7d_cq_revenue,
        SUM(cq_rev.L7d_cq_revenue_prior * coverage.split) L7d_cq_revenue_prior

        ,
        SUM(cq_rev.cq_optimal * coverage.split) cq_optimal,
        SUM(cq_rev.cq_optimal_prior * coverage.split) cq_optimal_prior,
        SUM(cq_rev.L7d_cq_optimal * coverage.split) L7d_cq_optimal,
        SUM(cq_rev.L7d_cq_optimal_prior * coverage.split) L7d_cq_optimal_prior

        -- PQ Metrics
        , SUM(pq_rev.pq_revenue * coverage.split) pq_revenue,
        SUM(pq_rev.pq_revenue_qtd * coverage.split) pq_revenue_qtd,
        SUM(pq_rev.pq_revenue_qtd_prior * coverage.split) pq_revenue_qtd_prior
        -- LYQ Metrics
        , SUM(ly_rev.ly_revenue_qtd * coverage.split) ly_revenue_qtd,
        SUM(ly_rev.ly_revenue_qtd_prior * coverage.split) ly_revenue_qtd_prior,
        SUM(ly_rev.ly_revenue * coverage.split) ly_revenue,
        SUM(ly_rev.L7d_ly_revenue * coverage.split) L7d_ly_revenue,
        SUM(ly_rev.L7d_ly_revenue_prior * coverage.split) L7d_ly_revenue_prior

        ,
        SUM(ly_rev.ly_optimal_qtd * coverage.split) ly_optimal_qtd,
        SUM(ly_rev.ly_optimal_qtd_prior * coverage.split) ly_optimal_qtd_prior,
        SUM(ly_rev.ly_optimal * coverage.split) ly_optimal,
        SUM(ly_rev.L7d_ly_optimal * coverage.split) L7d_ly_optimal,
        SUM(ly_rev.L7d_ly_optimal_prior * coverage.split) L7d_ly_optimal_prior

        -- Quotas
        , SUM(coverage.adv_quota) advertiser_quota,
        SUM(coverage.agency_quota) agency_quota,
        SUM(coverage.reseller_quota) reseller_quota,
        SUM(
            CASE
                WHEN rep_fbid_cp IS NOT NULL THEN inm_optimal_goal
                ELSE optimal_goal
            END
        ) optimal_goal,
        SUM(
            CASE
                WHEN rep_fbid_ap IS NOT NULL THEN agc_optimal_goal
                WHEN planning_agency_fbid IS NOT NULL THEN optimal_goal
                ELSE NULL
            END
        ) agc_optimal_goal,
        -- Forecasts
        SUM(coverage.sales_forecast) sales_forecast,
        SUM(coverage.sales_forecast_prior) sales_forecast_prior,
        SUM(cq_rev.cq_revenue * coverage.split) + (
            SUM(cq_rev.L7d_cq_revenue * coverage.split) / 7 * days_left_in_quarter
        ) run_rate_forecast,
        SUM(cq_rev.cq_revenue_prior * coverage.split) + (
            SUM(cq_rev.L7d_cq_revenue_prior * coverage.split) / 7
                * days_left_in_quarter_prior
        ) run_rate_forecast_prior,
        SUM(
            (
                (
                    (cq_rev.cq_revenue * coverage.split) / days_closed_in_quarter
                ) * days_total_in_quarter
            )
        ) straightline_forecast,
        SUM(
            (
                (
                    (cq_rev.cq_revenue_prior * coverage.split)
                        / days_closed_in_quarter_prior
                ) * days_total_in_quarter
            )
        ) straightline_forecast_prior

        --Resilient & Legal ( Absolute now, zeroed out)
        , SUM(0) legal_revn,
        SUM(0) resilient_legal_revn

        -- Resilient Goal
        , SUM(
            (
                CASE
                    WHEN revenue_segment = 'GBG In-Market' THEN adv_quota
                        * inm_dr_resilience_goal -- get dollar value of quota that will be goaled as dr resilient
                    WHEN revenue_segment = 'GBG Scaled' THEN scaled_dr_resilience_goal -- dollar value divide by Scaled quota to get pct
                    ELSE 0
                END
            )
        ) dr_resilience_goal,

        -- CQ Resilient Revenue
        SUM(cq_rev.cq_resilient * coverage.split) cq_resilient,
        SUM(cq_rev.cq_resilient_prior * coverage.split) cq_resilient_prior,
        SUM(cq_rev.L7d_cq_resilient * coverage.split) L7d_cq_resilient,
        SUM(cq_rev.L7d_cq_resilient_prior * coverage.split) L7d_cq_resilient_prior,

        -- CQ EBR Revenue
        SUM(cq_rev.cq_ebr_revenue * coverage.split) cq_ebr_revenue,
        SUM(cq_rev.cq_ebr_revenue_prior * coverage.split) cq_ebr_revenue_prior,
        SUM(cq_rev.L7d_cq_ebr_revenue * coverage.split) L7d_cq_ebr_revenue,
        SUM(cq_rev.L7d_cq_ebr_revenue_prior * coverage.split) L7d_cq_ebr_revenue_prior,

        -- CQ CAPI EBR Revenue
        SUM(cq_rev.cq_capi_ebr_revenue * coverage.split) cq_capi_ebr_revenue,
        SUM(cq_rev.cq_capi_ebr_revenue_prior * coverage.split) cq_capi_ebr_revenue_prior,
        SUM(cq_rev.L7d_cq_capi_ebr_revenue * coverage.split) L7d_cq_capi_ebr_revenue,
        SUM(cq_rev.L7d_cq_capi_ebr_revenue_prior * coverage.split) L7d_cq_capi_ebr_revenue_prior,

        SUM(ly2_rev.ly2_revenue_qtd * coverage.split) ly2_revenue_qtd,
        SUM(ly2_rev.ly2_revenue_qtd_prior * coverage.split) ly2_revenue_qtd_prior,
        SUM(ly2_rev.ly2_revenue * coverage.split) ly2_revenue,
        SUM(ly2_rev.L7d_ly2_revenue * coverage.split) L7d_ly2_revenue,
        SUM(ly2_rev.L7d_ly2_revenue_prior * coverage.split) L7d_ly2_revenue_prior

    FROM bpo_coverage_asis_stg_1 coverage

    LEFT JOIN cq_rev
        ON coverage.ad_account_id = cq_rev.ad_account_id

    LEFT JOIN pq_rev
        ON coverage.ad_account_id = pq_rev.ad_account_id

    LEFT JOIN ly_rev
        ON coverage.ad_account_id = ly_rev.ad_account_id

    LEFT JOIN ly2_rev
        ON coverage.ad_account_id = ly2_rev.ad_account_id

    LEFT JOIN gcm
        ON

        gcm.ultimate_parent_fbid = CAST(coverage.ultimate_parent_fbid AS BIGINT)
        AND coverage.l4_reporting_territory = gcm.l4_reporting_territory

    LEFT JOIN gcc
        ON

        gcc.ultimate_parent_fbid = CAST(coverage.ultimate_parent_fbid AS BIGINT)
        AND coverage.l4_reporting_territory = gcc.l4_reporting_territory

    LEFT JOIN hq
        ON

        hq.org_fbid = CAST(coverage.ultimate_parent_fbid AS BIGINT)

    CROSS JOIN quarter_dates

    WHERE
        coverage.ds = '<DATEID>'

    GROUP BY

        coverage.ultimate_parent_fbid,
        coverage.ultimate_parent_name,
        coverage.planning_agency_ult_fbid,
        coverage.planning_agency_ult_name,
        COALESCE(coverage.revenue_segment, coverage.program),
        coverage.program_agency,
        coverage.advertiser_vertical,
        coverage.advertiser_sub_vertical,
        coverage.l12_agency_territory,
        coverage.l10_agency_territory,
        coverage.l8_agency_territory,
        coverage.l6_agency_territory,
        coverage.l4_agency_territory,
        coverage.l12_reporting_territory,
        coverage.l10_reporting_territory,
        coverage.l8_reporting_territory,
        coverage.l6_reporting_territory,
        coverage.l4_reporting_territory,
        coverage.l12_reseller_territory,
        coverage.l10_reseller_territory,
        coverage.l8_reseller_territory,
        coverage.l6_reseller_territory,
        coverage.l4_reseller_territory,
        CAST(asofdate AS VARCHAR),
        CAST(quarter_id AS VARCHAR),
        CAST(next_quarter_id AS VARCHAR),
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        hq.hq_country_adv,
        hq.hq_region_adv,
        COALESCE(gcc.is_gcc, FALSE),
        COALESCE(gcm.is_gcm, FALSE)
    """,
)


gms_wbr_rtm = PrestoInsertOperator(
    dep_list=[
        gms_wbr_flat,
    ],
    table="<TABLE:gms_wbr_rtm>",
    partition="ds=<DATEID>",
    documentation={"description": """Staging Table prior to RTM Split"""},
    create=r"""
    CREATE TABLE IF NOT EXISTS <TABLE:gms_wbr_rtm> (
        route_to_market VARCHAR,
        asofdate VARCHAR,
        quarter_id VARCHAR,
        next_quarter_id VARCHAR,
        days_left_in_quarter BIGINT,
        days_left_in_quarter_prior BIGINT,
        days_total_in_quarter BIGINT,
        days_closed_in_quarter BIGINT,
        ultimate_client_fbid VARCHAR,
        ultimate_client_name VARCHAR,
        revenue_segment VARCHAR,
        l12_territory VARCHAR,
        l10_territory VARCHAR,
        l8_territory VARCHAR,
        l6_territory VARCHAR,
        l4_territory VARCHAR,
        vertical VARCHAR,
        sub_vertical VARCHAR,
        cq_revenue DOUBLE,
        cq_revenue_prior DOUBLE,
        l7d_cq_revenue DOUBLE,
        l7d_cq_revenue_prior DOUBLE,
        pq_revenue DOUBLE,
        pq_revenue_qtd DOUBLE,
        pq_revenue_qtd_prior DOUBLE,
        ly_revenue_qtd DOUBLE,
        ly_revenue_qtd_prior DOUBLE,
        ly_revenue DOUBLE,
        l7d_ly_revenue DOUBLE,
        L7d_ly_revenue_prior DOUBLE,
        quota DOUBLE,
        sales_forecast DOUBLE,
        sales_forecast_prior DOUBLE,
        run_rate_forecast DOUBLE,
        run_rate_forecast_prior DOUBLE,
        straightline_forecast DOUBLE,
        straightline_forecast_prior DOUBLE,
        cq_optimal DOUBLE,
        cq_optimal_prior DOUBLE,
        L7d_cq_optimal DOUBLE,
        L7d_cq_optimal_prior DOUBLE,
        ly_optimal_qtd DOUBLE,
        ly_optimal_qtd_prior DOUBLE,
        ly_optimal DOUBLE,
        L7d_ly_optimal DOUBLE,
        L7d_ly_optimal_prior DOUBLE,
        optimal_goal DOUBLE,
        legal_revn DOUBLE,
        resilient_legal_revn DOUBLE,
        hq_country VARCHAR,
        hq_region VARCHAR,
        is_gcc BOOLEAN,
        is_gcm BOOLEAN,
        dr_resilience_goal DOUBLE,
        cq_resilient DOUBLE,
        cq_resilient_prior DOUBLE,
        L7d_cq_resilient DOUBLE,
        L7d_cq_resilient_prior DOUBLE,
        cq_ebr_revenue DOUBLE,
        cq_ebr_revenue_prior DOUBLE,
        L7d_cq_ebr_revenue DOUBLE,
        L7d_cq_ebr_revenue_prior DOUBLE,
        cq_capi_ebr_revenue DOUBLE,
        cq_capi_ebr_revenue_prior DOUBLE,
        L7d_cq_capi_ebr_revenue DOUBLE,
        L7d_cq_capi_ebr_revenue_prior DOUBLE,
        revenue_tier VARCHAR,
        ly2_revenue_qtd DOUBLE,
        ly2_revenue_qtd_prior DOUBLE,
        ly2_revenue DOUBLE,
        l7d_ly2_revenue DOUBLE,
        L7d_ly2_revenue_prior DOUBLE,
        ds VARCHAR
    )
    WITH (
        format = 'DWRF',
        oncall = 'gms_ca',
        partitioned_by = ARRAY['ds'],
        retention_days = <RETENTION:90>
    )
    """,
    select=r"""
    WITH rev_tier_adv AS (
        SELECT
            ultimate_parent_fbid,
            ultimate_parent_name,
            revenue_segment,
            CASE
                WHEN SUM(cq_revenue) >= 12500000 THEN 'A+'
                WHEN SUM(cq_revenue) >= 2500000 THEN 'A'
                WHEN SUM(cq_revenue) >= 500000 THEN 'B'
                WHEN SUM(cq_revenue) >= 150000 THEN 'C'
                WHEN SUM(cq_revenue) >= 25000 THEN 'D'
                ELSE 'Tail'
            END revenue_tier

        FROM <TABLE:gms_wbr_flat>

        WHERE
            revenue_segment LIKE 'GBG%'
            AND DS = '<DATEID>'

        GROUP BY
            ultimate_parent_fbid,
            ultimate_parent_name,
            revenue_segment
    ),

    rev_tier_agc AS (
        SELECT
            planning_agency_ult_fbid,
            planning_agency_ult_name,
            program_agency revenue_segment,
            CASE
                WHEN SUM(cq_revenue) >= 12500000 THEN 'A+'
                WHEN SUM(cq_revenue) >= 2500000 THEN 'A'
                WHEN SUM(cq_revenue) >= 500000 THEN 'B'
                WHEN SUM(cq_revenue) >= 150000 THEN 'C'
                WHEN SUM(cq_revenue) >= 25000 THEN 'D'
                ELSE 'Tail'
            END revenue_tier

        FROM <TABLE:gms_wbr_flat>

        WHERE
            program_agency LIKE 'GBG%'
            AND DS = '<DATEID>'

        GROUP BY
            planning_agency_ult_fbid,
            planning_agency_ult_name,
            program_agency
    )

    SELECT
        'Reporting' AS route_to_market,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        wbr_flat.ultimate_parent_fbid ultimate_client_fbid,
        wbr_flat.ultimate_parent_name ultimate_client_name,
        wbr_flat.revenue_segment,
        l12_reporting_territory l12_territory,
        l10_reporting_territory l10_territory,
        l8_reporting_territory l8_territory,
        l6_reporting_territory l6_territory,
        l4_reporting_territory l4_territory,
        vertical,
        sub_vertical,
        hq_country_adv hq_country,
        hq_region_adv hq_region,
        is_gcc,
        is_gcm,
        COALESCE(rev_tier_adv.revenue_tier, 'Tail') revenue_tier,
        SUM(cq_revenue) cq_revenue,
        SUM(cq_revenue_prior) cq_revenue_prior,
        SUM(l7d_cq_revenue) l7d_cq_revenue,
        SUM(l7d_cq_revenue_prior) l7d_cq_revenue_prior,
        SUM(pq_revenue) pq_revenue,
        SUM(pq_revenue_qtd) pq_revenue_qtd,
        SUM(pq_revenue_qtd_prior) pq_revenue_qtd_prior,
        SUM(ly_revenue_qtd) ly_revenue_qtd,
        SUM(ly_revenue_qtd_prior) ly_revenue_qtd_prior,
        SUM(ly_revenue) ly_revenue,
        SUM(l7d_ly_revenue) L7d_ly_revenue,
        SUM(L7d_ly_revenue_prior) L7d_ly_revenue_prior,
        SUM(Advertiser_quota) quota,
        SUM(sales_forecast) sales_forecast,
        SUM(sales_forecast_prior) sales_forecast_prior,
        SUM(run_rate_forecast) run_rate_forecast,
        SUM(run_rate_forecast_prior) run_rate_forecast_prior,
        SUM(straightline_forecast) straightline_forecast,
        SUM(straightline_forecast_prior) straightline_forecast_prior,
        SUM(cq_optimal) cq_optimal,
        SUM(cq_optimal_prior) cq_optimal_prior,
        SUM(L7d_cq_optimal) L7d_cq_optimal,
        SUM(L7d_cq_optimal_prior) L7d_cq_optimal_prior,
        SUM(ly_optimal_qtd) ly_optimal_qtd,
        SUM(ly_optimal_qtd_prior) ly_optimal_qtd_prior,
        SUM(ly_optimal) ly_optimal,
        SUM(L7d_ly_optimal) L7d_ly_optimal,
        SUM(L7d_ly_optimal_prior) L7d_ly_optimal_prior,
        SUM(optimal_goal) optimal_goal,
        SUM(legal_revn) legal_revn,
        SUM(resilient_legal_revn) resilient_legal_revn,
        SUM(dr_resilience_goal) dr_resilience_goal,
        SUM(cq_resilient) cq_resilient,
        SUM(cq_resilient_prior) cq_resilient_prior,
        SUM(L7d_cq_resilient) L7d_cq_resilient,
        SUM(L7d_cq_resilient_prior) L7d_cq_resilient_prior,
        SUM(cq_ebr_revenue) cq_ebr_revenue,
        SUM(cq_ebr_revenue_prior) cq_ebr_revenue_prior,
        SUM(L7d_cq_ebr_revenue) L7d_cq_ebr_revenue,
        SUM(L7d_cq_ebr_revenue_prior) L7d_cq_ebr_revenue_prior,
        SUM(cq_capi_ebr_revenue) cq_capi_ebr_revenue,
        SUM(cq_capi_ebr_revenue_prior) cq_capi_ebr_revenue_prior,
        SUM(L7d_cq_capi_ebr_revenue) L7d_cq_capi_ebr_revenue,
        SUM(L7d_cq_capi_ebr_revenue_prior) L7d_cq_capi_ebr_revenue_prior,
        SUM(ly2_revenue_qtd) ly2_revenue_qtd,
        SUM(ly2_revenue_qtd_prior) ly2_revenue_qtd_prior,
        SUM(ly2_revenue) ly2_revenue,
        SUM(l7d_ly2_revenue) L7d_ly2_revenue,
        SUM(L7d_ly2_revenue_prior) L7d_ly2_revenue_prior

    FROM <TABLE:gms_wbr_flat> wbr_flat
    LEFT JOIN

        rev_tier_adv
        ON rev_tier_adv.revenue_segment = wbr_flat.revenue_segment
        AND rev_tier_adv.ultimate_parent_fbid = wbr_flat.ultimate_parent_fbid

    WHERE
        wbr_flat.revenue_segment LIKE 'GBG%'
        AND DS = '<DATEID>'

    GROUP BY
        wbr_flat.ultimate_parent_fbid,
        wbr_flat.ultimate_parent_name,
        wbr_flat.revenue_segment,
        l12_reporting_territory,
        l10_reporting_territory,
        l8_reporting_territory,
        l6_reporting_territory,
        l4_reporting_territory,
        vertical,
        sub_vertical,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        hq_country_adv,
        hq_region_adv,
        is_gcc,
        is_gcm,
        COALESCE(rev_tier_adv.revenue_tier, 'Tail')

    UNION ALL

    SELECT
        'Advertiser' AS route_to_market,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        wbr_flat.ultimate_parent_fbid ultimate_client_fbid,
        wbr_flat.ultimate_parent_name ultimate_client_name,
        wbr_flat.revenue_segment,
        l12_reporting_territory l8_territory,
        l10_reporting_territory l8_territory,
        l8_reporting_territory l8_territory,
        l6_reporting_territory l6_territory,
        l4_reporting_territory l4_territory,
        vertical,
        sub_vertical,
        hq_country_adv hq_country,
        hq_region_adv hq_region,
        is_gcc,
        is_gcm,
        COALESCE(rev_tier_adv.revenue_tier, 'Tail') revenue_tier,
        SUM(cq_revenue) cq_revenue,
        SUM(cq_revenue_prior) cq_revenue_prior,
        SUM(l7d_cq_revenue) l7d_cq_revenue,
        SUM(l7d_cq_revenue_prior) l7d_cq_revenue_prior,
        SUM(pq_revenue) pq_revenue,
        SUM(pq_revenue_qtd) pq_revenue_qtd,
        SUM(pq_revenue_qtd_prior) pq_revenue_qtd_prior,
        SUM(ly_revenue_qtd) ly_revenue_qtd,
        SUM(ly_revenue_qtd_prior) ly_revenue_qtd_prior,
        SUM(ly_revenue) ly_revenue,
        SUM(l7d_ly_revenue) L7d_ly_revenue,
        SUM(L7d_ly_revenue_prior) L7d_ly_revenue_prior,
        SUM(Advertiser_quota) quota,
        SUM(sales_forecast) sales_forecast,
        SUM(sales_forecast_prior) sales_forecast_prior,
        SUM(run_rate_forecast) run_rate_forecast,
        SUM(run_rate_forecast_prior) run_rate_forecast_prior,
        SUM(straightline_forecast) straightline_forecast,
        SUM(straightline_forecast_prior) straightline_forecast_prior,
        SUM(cq_optimal) cq_optimal,
        SUM(cq_optimal_prior) cq_optimal_prior,
        SUM(L7d_cq_optimal) L7d_cq_optimal,
        SUM(L7d_cq_optimal_prior) L7d_cq_optimal_prior,
        SUM(ly_optimal_qtd) ly_optimal_qtd,
        SUM(ly_optimal_qtd_prior) ly_optimal_qtd_prior,
        SUM(ly_optimal) ly_optimal,
        SUM(L7d_ly_optimal) L7d_ly_optimal,
        SUM(L7d_ly_optimal_prior) L7d_ly_optimal_prior,
        SUM(optimal_goal) optimal_goal,
        SUM(legal_revn) legal_revn,
        SUM(resilient_legal_revn) resilient_legal_revn,
        SUM(dr_resilience_goal) dr_resilience_goal,
        SUM(cq_resilient) cq_resilient,
        SUM(cq_resilient_prior) cq_resilient_prior,
        SUM(L7d_cq_resilient) L7d_cq_resilient,
        SUM(L7d_cq_resilient_prior) L7d_cq_resilient_prior,
        SUM(cq_ebr_revenue) cq_ebr_revenue,
        SUM(cq_ebr_revenue_prior) cq_ebr_revenue_prior,
        SUM(L7d_cq_ebr_revenue) L7d_cq_ebr_revenue,
        SUM(L7d_cq_ebr_revenue_prior) L7d_cq_ebr_revenue_prior,
        SUM(cq_capi_ebr_revenue) cq_capi_ebr_revenue,
        SUM(cq_capi_ebr_revenue_prior) cq_capi_ebr_revenue_prior,
        SUM(L7d_cq_capi_ebr_revenue) L7d_cq_capi_ebr_revenue,
        SUM(L7d_cq_capi_ebr_revenue_prior) L7d_cq_capi_ebr_revenue_prior,
        SUM(ly2_revenue_qtd) ly2_revenue_qtd,
        SUM(ly2_revenue_qtd_prior) ly2_revenue_qtd_prior,
        SUM(ly2_revenue) ly2_revenue,
        SUM(l7d_ly2_revenue) L7d_ly2_revenue,
        SUM(L7d_ly2_revenue_prior) L7d_ly2_revenue_prior

    FROM <TABLE:gms_wbr_flat> wbr_flat
    LEFT JOIN

        rev_tier_adv
        ON rev_tier_adv.revenue_segment = wbr_flat.revenue_segment
        AND rev_tier_adv.ultimate_parent_fbid = wbr_flat.ultimate_parent_fbid

    WHERE
        wbr_flat.revenue_segment IN ('GBG In-Market', 'GBG Scaled')
        AND DS = '<DATEID>'

    GROUP BY
        wbr_flat.ultimate_parent_fbid,
        wbr_flat.ultimate_parent_name,
        wbr_flat.revenue_segment,
        l12_reporting_territory,
        l10_reporting_territory,
        l8_reporting_territory,
        l6_reporting_territory,
        l4_reporting_territory,
        vertical,
        sub_vertical,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        hq_country_adv,
        hq_region_adv,
        is_gcc,
        is_gcm,
        COALESCE(rev_tier_adv.revenue_tier, 'Tail')

    UNION ALL

    SELECT
        'Agency' AS route_to_market,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        wbr_flat.planning_agency_ult_fbid ultimate_client_fbid,
        wbr_flat.planning_agency_ult_name ultimate_client_name,
        wbr_flat.program_agency revenue_segment,
        l12_agency_territory l8_territory,
        l10_agency_territory l8_territory,
        l8_agency_territory l8_territory,
        l6_agency_territory l6_territory,
        l4_agency_territory l4_territory,
        vertical,
        sub_vertical,
        NULL AS hq_country,
        NULL AS hq_region,
        is_gcc,
        is_gcm,
        COALESCE(rev_tier_agc.revenue_tier, 'Tail') AS revenue_tier,
        SUM(cq_revenue) cq_revenue,
        SUM(cq_revenue_prior) cq_revenue_prior,
        SUM(l7d_cq_revenue) l7d_cq_revenue,
        SUM(l7d_cq_revenue_prior) l7d_cq_revenue_prior,
        SUM(pq_revenue) pq_revenue,
        SUM(pq_revenue_qtd) pq_revenue_qtd,
        SUM(pq_revenue_qtd_prior) pq_revenue_qtd_prior,
        SUM(ly_revenue_qtd) ly_revenue_qtd,
        SUM(ly_revenue_qtd_prior) ly_revenue_qtd_prior,
        SUM(ly_revenue) ly_revenue,
        SUM(l7d_ly_revenue) L7d_ly_revenue,
        SUM(L7d_ly_revenue_prior) L7d_ly_revenue_prior,
        SUM(agency_quota) quota,
        NULL sales_forecast,
        NULL sales_forecast_prior,
        SUM(run_rate_forecast) run_rate_forecast,
        SUM(run_rate_forecast_prior) run_rate_forecast_prior,
        SUM(straightline_forecast) straightline_forecast,
        SUM(straightline_forecast_prior) straightline_forecast_prior,
        SUM(cq_optimal) cq_optimal,
        SUM(cq_optimal_prior) cq_optimal_prior,
        SUM(L7d_cq_optimal) L7d_cq_optimal,
        SUM(L7d_cq_optimal_prior) L7d_cq_optimal_prior,
        SUM(ly_optimal_qtd) ly_optimal_qtd,
        SUM(ly_optimal_qtd_prior) ly_optimal_qtd_prior,
        SUM(ly_optimal) ly_optimal,
        SUM(L7d_ly_optimal) L7d_ly_optimal,
        SUM(L7d_ly_optimal_prior) L7d_ly_optimal_prior,
        SUM(agc_optimal_goal) optimal_goal,
        SUM(legal_revn) legal_revn,
        SUM(resilient_legal_revn) resilient_legal_revn,
        SUM(dr_resilience_goal) dr_resilience_goal,
        SUM(cq_resilient) cq_resilient,
        SUM(cq_resilient_prior) cq_resilient_prior,
        SUM(L7d_cq_resilient) L7d_cq_resilient,
        SUM(L7d_cq_resilient_prior) L7d_cq_resilient_prior,
        SUM(cq_ebr_revenue) cq_ebr_revenue,
        SUM(cq_ebr_revenue_prior) cq_ebr_revenue_prior,
        SUM(L7d_cq_ebr_revenue) L7d_cq_ebr_revenue,
        SUM(L7d_cq_ebr_revenue_prior) L7d_cq_ebr_revenue_prior,
        SUM(cq_capi_ebr_revenue) cq_capi_ebr_revenue,
        SUM(cq_capi_ebr_revenue_prior) cq_capi_ebr_revenue_prior,
        SUM(L7d_cq_capi_ebr_revenue) L7d_cq_capi_ebr_revenue,
        SUM(L7d_cq_capi_ebr_revenue_prior) L7d_cq_capi_ebr_revenue_prior,
        SUM(ly2_revenue_qtd) ly2_revenue_qtd,
        SUM(ly2_revenue_qtd_prior) ly2_revenue_qtd_prior,
        SUM(ly2_revenue) ly2_revenue,
        SUM(l7d_ly2_revenue) L7d_ly2_revenue,
        SUM(L7d_ly2_revenue_prior) L7d_ly2_revenue_prior

    FROM <TABLE:gms_wbr_flat> wbr_flat
    LEFT JOIN

        rev_tier_agc
        ON rev_tier_agc.revenue_segment = wbr_flat.revenue_segment
        AND rev_tier_agc.planning_agency_ult_fbid = wbr_flat.planning_agency_ult_fbid

    WHERE
        wbr_flat.program_agency IN ('GBG In-Market Agency', 'GBG Scaled Agency')
        AND DS = '<DATEID>'

    GROUP BY
        wbr_flat.planning_agency_ult_fbid,
        wbr_flat.planning_agency_ult_name,
        wbr_flat.program_agency,
        l12_agency_territory,
        l10_agency_territory,
        l8_agency_territory,
        l6_agency_territory,
        l4_agency_territory,
        vertical,
        sub_vertical,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        is_gcc,
        is_gcm,
        COALESCE(rev_tier_agc.revenue_tier, 'Tail')

    UNION ALL

    SELECT
        'Reseller' AS route_to_market,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        wbr_flat.ultimate_parent_fbid ultimate_client_fbid,
        wbr_flat.ultimate_parent_name ultimate_client_name,
        'Reseller' revenue_segment,
        l12_reseller_territory l8_territory,
        l10_reseller_territory l8_territory,
        l8_reseller_territory l8_territory,
        l6_reseller_territory l6_territory,
        l4_reseller_territory l4_territory,
        vertical,
        sub_vertical,
        NULL AS hq_country,
        NULL AS hq_region,
        is_gcc,
        is_gcm,
        COALESCE(rev_tier_adv.revenue_tier, 'Tail') revenue_tier,
        SUM(cq_revenue) cq_revenue,
        SUM(cq_revenue_prior) cq_revenue_prior,
        SUM(l7d_cq_revenue) l7d_cq_revenue,
        SUM(l7d_cq_revenue_prior) l7d_cq_revenue_prior,
        SUM(pq_revenue) pq_revenue,
        SUM(pq_revenue_qtd) pq_revenue_qtd,
        SUM(pq_revenue_qtd_prior) pq_revenue_qtd_prior,
        SUM(ly_revenue_qtd) ly_revenue_qtd,
        SUM(ly_revenue_qtd_prior) ly_revenue_qtd_prior,
        SUM(ly_revenue) ly_revenue,
        SUM(l7d_ly_revenue) L7d_ly_revenue,
        SUM(L7d_ly_revenue_prior) L7d_ly_revenue_prior,
        SUM(reseller_quota) quota,
        NULL sales_forecast,
        NULL sales_forecast_prior,
        SUM(run_rate_forecast) run_rate_forecast,
        SUM(run_rate_forecast_prior) run_rate_forecast_prior,
        SUM(straightline_forecast) straightline_forecast,
        SUM(straightline_forecast_prior) straightline_forecast_prior,
        SUM(cq_optimal) cq_optimal,
        SUM(cq_optimal_prior) cq_optimal_prior,
        SUM(L7d_cq_optimal) L7d_cq_optimal,
        SUM(L7d_cq_optimal_prior) L7d_cq_optimal_prior,
        SUM(ly_optimal_qtd) ly_optimal_qtd,
        SUM(ly_optimal_qtd_prior) ly_optimal_qtd_prior,
        SUM(ly_optimal) ly_optimal,
        SUM(L7d_ly_optimal) L7d_ly_optimal,
        SUM(L7d_ly_optimal_prior) L7d_ly_optimal_prior,
        SUM(optimal_goal) optimal_goal,
        SUM(legal_revn) legal_revn,
        SUM(resilient_legal_revn) resilient_legal_revn,
        SUM(dr_resilience_goal) dr_resilience_goal,
        SUM(cq_resilient) cq_resilient,
        SUM(cq_resilient_prior) cq_resilient_prior,
        SUM(L7d_cq_resilient) L7d_cq_resilient,
        SUM(L7d_cq_resilient_prior) L7d_cq_resilient_prior,
        SUM(cq_ebr_revenue) cq_ebr_revenue,
        SUM(cq_ebr_revenue_prior) cq_ebr_revenue_prior,
        SUM(L7d_cq_ebr_revenue) L7d_cq_ebr_revenue,
        SUM(L7d_cq_ebr_revenue_prior) L7d_cq_ebr_revenue_prior,
        SUM(cq_capi_ebr_revenue) cq_capi_ebr_revenue,
        SUM(cq_capi_ebr_revenue_prior) cq_capi_ebr_revenue_prior,
        SUM(L7d_cq_capi_ebr_revenue) L7d_cq_capi_ebr_revenue,
        SUM(L7d_cq_capi_ebr_revenue_prior) L7d_cq_capi_ebr_revenue_prior,
        SUM(ly2_revenue_qtd) ly2_revenue_qtd,
        SUM(ly2_revenue_qtd_prior) ly2_revenue_qtd_prior,
        SUM(ly2_revenue) ly2_revenue,
        SUM(l7d_ly2_revenue) L7d_ly2_revenue,
        SUM(L7d_ly2_revenue_prior) L7d_ly2_revenue_prior

    FROM <TABLE:gms_wbr_flat> wbr_flat
    LEFT JOIN

        rev_tier_adv
        ON rev_tier_adv.ultimate_parent_fbid = wbr_flat.ultimate_parent_fbid
                and rev_tier_adv.revenue_segment = wbr_flat.revenue_segment


    WHERE
        reseller_quota IS NOT NULL
        AND DS = '<DATEID>'

    GROUP BY
        wbr_flat.ultimate_parent_fbid,
        wbr_flat.ultimate_parent_name,
        l12_reseller_territory,
        l10_reseller_territory,
        l8_reseller_territory,
        l6_reseller_territory,
        l4_reseller_territory,
        vertical,
        sub_vertical,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        is_gcc,
        is_gcm,
        COALESCE(rev_tier_adv.revenue_tier, 'Tail')
    """,
)


gms_wbr_rtm_v = PrestoOperator(
    dep_list=[
        gms_wbr_rtm,
    ],
    query="""
    CREATE OR REPLACE VIEW <TABLE:gms_wbr_rtm_v> AS

    SELECT

        route_to_market,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        ultimate_client_fbid,
        ultimate_client_name,
        revenue_segment,
        l12_territory,
        l10_territory,
        l8_territory,
        l6_territory,
        l4_territory,
        vertical,
        sub_vertical,
        cq_revenue,
        cq_revenue_prior,
        l7d_cq_revenue,
        l7d_cq_revenue_prior,
        pq_revenue,
        pq_revenue_qtd,
        pq_revenue_qtd_prior,
        ly_revenue_qtd,
        ly_revenue_qtd_prior,
        ly_revenue,
        l7d_ly_revenue,
        L7d_ly_revenue_prior,
        quota,
        sales_forecast,
        sales_forecast_prior,
        run_rate_forecast,
        run_rate_forecast_prior,
        straightline_forecast,
        straightline_forecast_prior,
        cq_optimal,
        cq_optimal_prior,
        L7d_cq_optimal,
        L7d_cq_optimal_prior,
        ly_optimal_qtd,
        ly_optimal_qtd_prior,
        ly_optimal,
        L7d_ly_optimal,
        L7d_ly_optimal_prior,
        optimal_goal,
        legal_revn,
        resilient_legal_revn,
        hq_country,
        hq_region,
        is_gcc,
        is_gcm,
        dr_resilience_goal,
        cq_resilient,
        cq_resilient_prior,
        L7d_cq_resilient,
        L7d_cq_resilient_prior,
        cq_ebr_revenue,
        cq_ebr_revenue_prior,
        L7d_cq_ebr_revenue,
        L7d_cq_ebr_revenue_prior,
        cq_capi_ebr_revenue,
        cq_capi_ebr_revenue_prior,
        L7d_cq_capi_ebr_revenue,
        L7d_cq_capi_ebr_revenue_prior,
        COALESCE(revenue_tier, 'Tail') revenue_tier,
        ly2_revenue_qtd,
        ly2_revenue_qtd_prior,
        ly2_revenue,
        L7d_ly2_revenue,
        L7d_ly2_revenue_prior,
        ds

    FROM <TABLE:gms_wbr_rtm>

    WHERE
        ds = '<DATEID>'
    """,
)


gms_wbr_rtm_5q_v = PrestoOperator(
    dep_list=[
        gms_wbr_rtm_v,
    ],
    query="""
    CREATE OR REPLACE VIEW <TABLE:gms_wbr_rtm_5q_v> AS

    SELECT

        route_to_market,
        asofdate,
        quarter_id,
        next_quarter_id,
        days_left_in_quarter,
        days_left_in_quarter_prior,
        days_total_in_quarter,
        days_closed_in_quarter,
        ultimate_client_fbid,
        ultimate_client_name,
        revenue_segment,
        l12_territory,
        l10_territory,
        l8_territory,
        l6_territory,
        l4_territory,
        vertical,
        sub_vertical,
        cq_revenue,
        cq_revenue_prior,
        l7d_cq_revenue,
        l7d_cq_revenue_prior,
        pq_revenue,
        pq_revenue_qtd,
        pq_revenue_qtd_prior,
        ly_revenue_qtd,
        ly_revenue_qtd_prior,
        ly_revenue,
        l7d_ly_revenue,
        L7d_ly_revenue_prior,
        quota,
        sales_forecast,
        sales_forecast_prior,
        run_rate_forecast,
        run_rate_forecast_prior,
        straightline_forecast,
        straightline_forecast_prior,
        cq_optimal,
        cq_optimal_prior,
        L7d_cq_optimal,
        L7d_cq_optimal_prior,
        ly_optimal_qtd,
        ly_optimal_qtd_prior,
        ly_optimal,
        L7d_ly_optimal,
        L7d_ly_optimal_prior,
        optimal_goal,
        legal_revn,
        resilient_legal_revn,
        hq_country,
        hq_region,
        is_gcc,
        is_gcm,
        dr_resilience_goal,
        cq_resilient,
        cq_resilient_prior,
        L7d_cq_resilient,
        L7d_cq_resilient_prior,
        cq_ebr_revenue,
        cq_ebr_revenue_prior,
        L7d_cq_ebr_revenue,
        L7d_cq_ebr_revenue_prior,
        cq_capi_ebr_revenue,
        cq_capi_ebr_revenue_prior,
        L7d_cq_capi_ebr_revenue,
        L7d_cq_capi_ebr_revenue_prior,
        COALESCE(revenue_tier, 'Tail') revenue_tier,
        ly2_revenue_qtd,
        ly2_revenue_qtd_prior,
        ly2_revenue,
        L7d_ly2_revenue,
        L7d_ly2_revenue_prior,
        ds

    FROM <TABLE:gms_wbr_rtm>

    WHERE
        (
            ds = '<DATEID>'
            OR

            ds LIKE '%12-31'
            OR ds LIKE '%09-30'
            OR ds LIKE '%06-30'
            OR ds LIKE '%03-31'
        )
    """,
)


gms_wbr_stg_1_daily = PrestoInsertOperator(
    dep_list=[
        wait_for_bpo_coverage_asis_stg_1,
        wait_for_staging_bpt_fct_l4_gms_rec_rev_data,
        wait_for_bpo_as_is_coverage,
        wait_for_acdp_dim_l45_organization,
        wait_for_d_geo_country,
        wait_for_organization_to_customer_asset_managers,
    ],
    table="<TABLE:gms_wbr_stg_1_daily>",
    partition="ds=<DATEID>",
    documentation={"description": """Staging Table prior to RTM Split"""},
    create=r"""
    CREATE TABLE IF NOT EXISTS <TABLE:gms_wbr_stg_1_daily> (
        ad_account_id BIGINT,
        date_id VARCHAR,
        advertiser_quota DOUBLE,
        agency_quota DOUBLE,
        reseller_quota DOUBLE,
        optimal_goal DOUBLE,
        agc_optimal_goal DOUBLE,
        sales_forecast DOUBLE,
        sales_forecast_prior DOUBLE,
        cq_revenue DOUBLE,
        cq_optimal DOUBLE,
        pq_revenue DOUBLE,
        pq_optimal DOUBLE,
        ly_revenue DOUBLE,
        ly_optimal DOUBLE,
        lyq_revenue DOUBLE,
        lyq_optimal DOUBLE,
        ly2q_revenue DOUBLE,
        ly2_revenue DOUBLE,
        ds VARCHAR
    )
    WITH (
        format = 'DWRF',
        oncall = 'gms_ca',
        partitioned_by = ARRAY['ds'],
        retention_days = <RETENTION:90>
    )
    """,
    select=r"""
    WITH DATES AS (
        SELECT
            date_id
        FROM d_date
        WHERE
            quarter_id = '<quarter_id>'
            AND DS = '<LATEST_DS:d_date>'
    ),
    coverage AS (
        SELECT
            ad_account_id,
            SUM(adv_quota) advertiser_quota,
            SUM(agency_quota) agency_quota,
            SUM(reseller_quota) reseller_quota,
            SUM(
                (
                    CASE
                        WHEN rep_fbid_cp IS NOT NULL THEN inm_optimal_goal
                        ELSE optimal_goal
                    END
                )
            ) optimal_goal,
            SUM(
                CASE
                    WHEN rep_fbid_ap IS NOT NULL THEN agc_optimal_goal
                    WHEN planning_agency_fbid IS NOT NULL THEN optimal_goal
                    ELSE NULL
                END
            ) agc_optimal_goal,

            -- Forecasts
            SUM(sales_forecast) sales_forecast,
            SUM(sales_forecast_prior) sales_forecast_prior

        FROM bpo_coverage_asis_stg_1

        WHERE
            ds = '<DATEID>'
        GROUP BY
            1
    ),
    date_map AS (
        SELECT
            date_id,
            ad_account_id,
            advertiser_quota,
            agency_quota,
            reseller_quota,
            optimal_goal,
            agc_optimal_goal,

            -- Forecasts
            sales_forecast,
            sales_forecast_prior

        FROM coverage

        CROSS JOIN dates

    ),
    fraud_rev AS (

        SELECT
            ad_account_id,
            'true' fraud
        FROM bpo_as_is_coverage

        WHERE
            is_fcast_eligible = FALSE
            AND ds = '<DATEID>'
        GROUP BY
            1, 2

    ),

    cq_rev AS (
        SELECT
            date_id,
            rev.account_id ad_account_id,
            SUM(asis_rec_rev) AS cq_revenue,
            SUM(asis_optimal_revn) AS cq_optimal,
            SUM(product_resilient_rec_rev) cq_resilient

        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id
        WHERE
            ds = '<DATEID>'
            AND quarter_id = '<quarter_id>'
            AND fraud_rev.fraud IS NULL
        GROUP BY
            1, 2
    ),
    pq_rev AS (
        SELECT
            CAST((CAST(date_id AS DATE) + INTERVAL '3' MONTH) AS VARCHAR) AS date_id,
            rev.account_id ad_account_id,
            SUM(asis_rec_rev) AS pq_revenue,
            SUM(asis_optimal_revn) AS pq_optimal,
            SUM(product_resilient_rec_rev) cq_resilient
        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id
        WHERE
            ds = '<DATEID>'
            AND quarter_id = '<prev_quarter_id>'
            AND fraud_rev.fraud IS NULL
        GROUP BY
            1, 2
    ),
    ly_rev AS (
        SELECT
            CAST((CAST(date_id AS DATE) + INTERVAL '12' MONTH) AS VARCHAR) AS date_id,
            rev.account_id ad_account_id,
            SUM(asis_rec_rev) AS ly_revenue,
            SUM(asis_optimal_revn) AS ly_optimal
        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id
        WHERE
            ds = '<DATEID>'
            AND quarter_id = CAST(
                (CAST('<quarter_id>' AS DATE) - INTERVAL '1' YEAR) AS VARCHAR
            )
            AND fraud_rev.fraud IS NULL
        GROUP BY
            1, 2
    ),

    lyq_rev AS (
        SELECT
            ad_account_id,
            SUM(ly_revenue) AS lyq_revenue,
            SUM(Ly_optimal) AS lyq_optimal

        FROM ly_rev

        GROUP BY
            1

    ),
    ly2_rev AS (
        SELECT
            CAST((CAST(date_id AS DATE) + INTERVAL '24' MONTH) AS VARCHAR) AS date_id,
            rev.account_id ad_account_id,
            SUM(asis_rec_rev) AS ly2_revenue
        FROM staging_bpt_fct_l4_gms_rec_rev_data rev
        LEFT JOIN fraud_rev
            ON

            fraud_rev.ad_account_id = rev.account_id
        WHERE
            ds = '<DATEID>'
            AND quarter_id = CAST(
                (CAST('<quarter_id>' AS DATE) - INTERVAL '2' YEAR) AS VARCHAR
            )
            AND fraud_rev.fraud IS NULL
        GROUP BY
            1, 2
    ),

    ly2q_rev AS (
        SELECT
            ad_account_id,
            SUM(ly2_revenue) AS ly2q_revenue

        FROM ly2_rev

        GROUP BY
            1

    )

    SELECT
        date_map.ad_account_id,
        date_map.date_id,
        advertiser_quota,
        agency_quota,
        reseller_quota,
        optimal_goal,
        agc_optimal_goal,
        sales_forecast,
        sales_forecast_prior,
        (cq_rev.cq_revenue) cq_revenue,
        (cq_rev.cq_optimal) cq_optimal,
        (pq_rev.pq_revenue) pq_revenue,
        (pq_rev.pq_optimal) pq_optimal,
        (ly_rev.ly_revenue) ly_revenue,
        (ly_rev.ly_optimal) ly_optimal,
        (lyq_rev.lyq_revenue) lyq_revenue,
        (lyq_rev.lyq_optimal) lyq_optimal,
        ly2q_rev.ly2q_revenue ly2q_revenue,
        ly2_rev.ly2_revenue ly2_revenue

    FROM date_map

    LEFT JOIN cq_rev
        ON date_map.ad_account_id = cq_rev.ad_account_id
        AND cq_rev.date_id = date_map.date_id

    LEFT JOIN pq_rev
        ON date_map.ad_account_id = pq_rev.ad_account_id
        AND pq_rev.date_id = date_map.date_id

    LEFT JOIN ly_rev
        ON date_map.ad_account_id = ly_rev.ad_account_id
        AND ly_rev.date_id = date_map.date_id

    LEFT JOIN lyq_rev
        ON date_map.ad_account_id = lyq_rev.ad_account_id

    LEFT JOIN ly2_rev
        ON date_map.ad_account_id = ly2_rev.ad_account_id
        AND ly2_rev.date_id = date_map.date_id

    LEFT JOIN ly2q_rev
        ON date_map.ad_account_id = ly2q_rev.ad_account_id
    """,
)

wait_for_gms_wbr_flat = WaitForHiveOperator(
    dep_list=[],
    table="gms_wbr_flat",
    use_future=False,
    fail_on_future=True,
)


gms_wbr_flat_daily_stg_1 = PrestoInsertOperator(
    dep_list=[
        gms_wbr_stg_1_daily,
        wait_for_gms_wbr_flat,
    ],
    table="<TABLE:gms_wbr_flat_daily_stg_1>",
    partition="ds=<DATEID>",
    documentation={"description": """Staging Table prior to RTM Split"""},
    create=r"""
    CREATE TABLE IF NOT EXISTS <TABLE:gms_wbr_flat_daily_stg_1> (
        ad_account_id BIGINT,
        revenue_segment VARCHAR,
        program_agency VARCHAR,
        vertical VARCHAR,
        sub_vertical VARCHAR,
        l12_agency_territory VARCHAR,
        l10_agency_territory VARCHAR,
        l8_agency_territory VARCHAR,
        l6_agency_territory VARCHAR,
        l4_agency_territory VARCHAR,
        l12_reporting_territory VARCHAR,
        l10_reporting_territory VARCHAR,
        l8_reporting_territory VARCHAR,
        l6_reporting_territory VARCHAR,
        l4_reporting_territory VARCHAR,
        l12_reseller_territory VARCHAR,
        l10_reseller_territory VARCHAR,
        l8_reseller_territory VARCHAR,
        l6_reseller_territory VARCHAR,
        l4_reseller_territory VARCHAR,
        hq_country_adv VARCHAR,
        hq_region_adv VARCHAR,
        is_gcc BOOLEAN,
        is_gcm BOOLEAN,
        revenue_tier_adv VARCHAR,
        revenue_tier_agc VARCHAR,
        split DOUBLE,
        advertiser_quota DOUBLE,
        agency_quota DOUBLE,
        reseller_quota DOUBLE,
        optimal_goal DOUBLE,
        agc_optimal_goal DOUBLE,
        sales_forecast DOUBLE,
        sales_forecast_prior DOUBLE,
        ds VARCHAR
    )
    WITH (
        format = 'DWRF',
        oncall = 'gms_ca',
        partitioned_by = ARRAY['ds'],
        retention_days = 7
    )
    """,
    select=r"""
    WITH gcm_gcc_stg_1 AS (
        SELECT
            c.ultimate_parent_org_fbid,
            brand_team_type
        FROM acdp_dim_l45_commission_split_v2:ad_reporting a
        INNER JOIN inc_dim_organization_to_brand_team_crm b
            ON a.brand_team_id = b.id2
            AND b.ds = '<DATEID>'
        INNER JOIN acdp_dim_l45_organization:ad_reporting c
            ON b.id1 = c.org_fbid
            AND c.ds = '<DATEID>'
        WHERE
            a.ds = '<DATEID>'
            AND '<DATEID>' BETWEEN a.commission_split_start_date AND COALESCE(
                a.commission_split_end_date,
                '2099-12-31'
            )
            AND brand_team_type IN ('Global Accounts', 'Global Coordinator')
        GROUP BY
            1, 2
    ),
    gcc AS (
        SELECT
            ultimate_parent_fbid,
            territory_l4_name l4_reporting_territory,
            TRUE is_gcc
        FROM gcc_quarterly_l8_qe_view_presto
        WHERE
            quarter_id = '<quarter_id>'
        GROUP BY
            1, 2, 3

    ),
    gcm AS (
        SELECT
            ultimate_parent_fbid,
            territory_l4_name l4_reporting_territory,
            TRUE is_gcm
        FROM gcm_milestone_model_4qtr_0_qe_view_presto
        WHERE
            quarter_id = '<quarter_id>'
        GROUP BY
            1, 2, 3

    ),
    hq AS (
        SELECT
            a.org_fbid,
            MAX(ult_hq.fb_subregion) AS hq_country_adv,
            MAX(ult_hq.sales_adv_region) AS hq_region_adv

        FROM acdp_dim_l45_organization:ad_reporting a
        LEFT JOIN (
            SELECT
                organization_id AS org_fbid,
                customer_asset_manager_id AS business_manager_id
            FROM (
                SELECT
                    organization_id,
                    customer_asset_manager_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            organization_id
                        ORDER BY
                            time DESC
                    ) AS rn
                FROM organization_to_customer_asset_managers:bi
                WHERE
                    ds = '<DATEID>'
                    AND customer_asset_manager_type = 1378
            )
            WHERE
                rn = 1
        ) c
            ON a.org_fbid = c.org_fbid
        LEFT JOIN (
            SELECT DISTINCT
                a.org_fbid,
                a.org_primary_billing_address_country,
                a.org_hq_address_country
            FROM acdp_dim_l45_organization:ad_reporting a
            WHERE
                a.ds = '<DATEID>'
        ) f
            ON a.ultimate_parent_org_fbid = f.org_fbid
        LEFT JOIN d_geo_country org_billing
            ON LOWER(TRIM(a.org_primary_billing_address_country)) = LOWER(
                TRIM(org_billing.country_abbr)
            )
            AND org_billing.ds = '<DATEID>'
        LEFT JOIN d_geo_country org_hq
            ON LOWER(TRIM(a.org_hq_address_country)) = LOWER(
                TRIM(org_hq.country_abbr)
            )
            AND org_hq.ds = '<DATEID>'
        LEFT JOIN d_geo_country gms
            ON UPPER(
                TRIM(
                    IF(
                        UPPER(a.org_hq_address_country) <> 'NOT AVAIL',
                        a.org_hq_address_country,
                        a.org_primary_billing_address_country
                    )
                )
            ) = UPPER(TRIM(gms.country_abbr))
            AND gms.ds = '<DATEID>'
        LEFT JOIN d_geo_country ult_billing
            ON LOWER(TRIM(f.org_primary_billing_address_country)) = LOWER(
                TRIM(ult_billing.country_abbr)
            )
            AND ult_billing.ds = '<DATEID>'
        LEFT JOIN d_geo_country ult_hq
            ON LOWER(TRIM(f.org_hq_address_country)) = LOWER(
                TRIM(ult_hq.country_abbr)
            )
            AND ult_hq.ds = '<DATEID>'

        WHERE
            a.ds = '<DATEID>'
            AND f.org_hq_address_country IS NOT NULL
        GROUP BY
            1

    ),
    rev_tier_adv AS (
        SELECT
            ultimate_parent_fbid,
            ultimate_parent_name,
            revenue_segment,
            CASE
                WHEN SUM(cq_revenue) >= 12500000 THEN 'A+'
                WHEN SUM(cq_revenue) >= 2500000 THEN 'A'
                WHEN SUM(cq_revenue) >= 500000 THEN 'B'
                WHEN SUM(cq_revenue) >= 150000 THEN 'C'
                WHEN SUM(cq_revenue) >= 25000 THEN 'D'
                ELSE 'Tail'
            END revenue_tier

        FROM <TABLE:gms_wbr_flat>

        WHERE
            revenue_segment LIKE 'GBG%'
            AND DS = '<DATEID>'

        GROUP BY
            ultimate_parent_fbid,
            ultimate_parent_name,
            revenue_segment
    ),

    rev_tier_agc AS (
        SELECT
            planning_agency_ult_fbid,
            planning_agency_ult_name,
            program_agency revenue_segment,
            CASE
                WHEN SUM(cq_revenue) >= 12500000 THEN 'A+'
                WHEN SUM(cq_revenue) >= 2500000 THEN 'A'
                WHEN SUM(cq_revenue) >= 500000 THEN 'B'
                WHEN SUM(cq_revenue) >= 150000 THEN 'C'
                WHEN SUM(cq_revenue) >= 25000 THEN 'D'
                ELSE 'Tail'
            END revenue_tier

        FROM <TABLE:gms_wbr_flat>

        WHERE
            program_agency LIKE 'GBG%'
            AND DS = '<DATEID>'

        GROUP BY
            planning_agency_ult_fbid,
            planning_agency_ult_name,
            program_agency
    )

    SELECT
        ad_account_id,
        coverage.revenue_segment,
        program_agency,
        advertiser_vertical vertical,
        advertiser_sub_vertical sub_vertical,
        l12_agency_territory,
        l10_agency_territory,
        l8_agency_territory,
        l6_agency_territory,
        l4_agency_territory,
        l12_reporting_territory,
        l10_reporting_territory,
        l8_reporting_territory,
        l6_reporting_territory,
        coverage.l4_reporting_territory,
        l12_reseller_territory,
        l10_reseller_territory,
        l8_reseller_territory,
        l6_reseller_territory,
        l4_reseller_territory,
        hq.hq_country_adv,
        hq.hq_region_adv,
        COALESCE(gcc.is_gcc, FALSE) is_gcc,
        COALESCE(gcm.is_gcm, FALSE) is_gcm,
        COALESCE(rev_tier_adv.revenue_tier, 'Tail') revenue_tier_adv,
        COALESCE(rev_tier_agc.revenue_tier, 'Tail') revenue_tier_agc,
        split,
        (adv_quota) advertiser_quota,
        (agency_quota) agency_quota,
        (reseller_quota) reseller_quota,
        (
            CASE
                WHEN rep_fbid_cp IS NOT NULL THEN inm_optimal_goal
                ELSE optimal_goal
            END
        ) optimal_goal,
        (
            CASE
                WHEN rep_fbid_ap IS NOT NULL THEN agc_optimal_goal
                WHEN planning_agency_fbid IS NOT NULL THEN optimal_goal
                ELSE NULL
            END
        ) agc_optimal_goal,

        -- Forecasts
        (sales_forecast) sales_forecast,
        (sales_forecast_prior) sales_forecast_prior

    FROM bpo_coverage_asis_stg_1 coverage

    LEFT JOIN gcm
        ON

        gcm.ultimate_parent_fbid = CAST(coverage.ultimate_parent_fbid AS BIGINT)
        AND coverage.l4_reporting_territory = gcm.l4_reporting_territory

    LEFT JOIN gcc
        ON

        gcc.ultimate_parent_fbid = CAST(coverage.ultimate_parent_fbid AS BIGINT)
        AND coverage.l4_reporting_territory = gcc.l4_reporting_territory

    LEFT JOIN hq
        ON

        hq.org_fbid = CAST(coverage.ultimate_parent_fbid AS BIGINT)

    LEFT JOIN rev_tier_adv
        ON rev_tier_adv.ultimate_parent_fbid = coverage.ultimate_parent_fbid
        AND rev_tier_adv.revenue_segment = coverage.revenue_segment

    LEFT JOIN rev_tier_agc
        ON rev_tier_agc.planning_agency_ult_fbid = coverage.planning_agency_ult_fbid
        AND rev_tier_agc.revenue_segment = coverage.program_agency

    WHERE
        coverage.ds = '<DATEID>'
    """,
)

gms_wbr_flat_daily = PrestoInsertOperator(
    dep_list=[
        gms_wbr_flat_daily_stg_1,
    ],
    table="<TABLE:gms_wbr_flat_daily>",
    partition="ds=<DATEID>",
    documentation={"description": """Staging Table prior to RTM Split"""},
    create=r"""
    CREATE TABLE IF NOT EXISTS <TABLE:gms_wbr_flat_daily> (
        ad_account_id BIGINT,
        date_id VARCHAR,
        revenue_segment VARCHAR,
        program_agency VARCHAR,
        vertical VARCHAR,
        sub_vertical VARCHAR,
        l12_agency_territory VARCHAR,
        l10_agency_territory VARCHAR,
        l8_agency_territory VARCHAR,
        l6_agency_territory VARCHAR,
        l4_agency_territory VARCHAR,
        l12_reporting_territory VARCHAR,
        l10_reporting_territory VARCHAR,
        l8_reporting_territory VARCHAR,
        l6_reporting_territory VARCHAR,
        l4_reporting_territory VARCHAR,
        l12_reseller_territory VARCHAR,
        l10_reseller_territory VARCHAR,
        l8_reseller_territory VARCHAR,
        l6_reseller_territory VARCHAR,
        l4_reseller_territory VARCHAR,
        advertiser_quota DOUBLE,
        agency_quota DOUBLE,
        reseller_quota DOUBLE,
        optimal_goal DOUBLE,
        agc_optimal_goal DOUBLE,
        sales_forecast DOUBLE,
        sales_forecast_prior DOUBLE,
        cq_revenue DOUBLE,
        cq_optimal DOUBLE,
        pq_revenue DOUBLE,
        pq_optimal DOUBLE,
        ly_revenue DOUBLE,
        ly_optimal DOUBLE,
        lyq_revenue DOUBLE,
        lyq_optimal DOUBLE,
        hq_country_adv VARCHAR,
        hq_region_adv VARCHAR,
        is_gcc BOOLEAN,
        is_gcm BOOLEAN,
        revenue_tier_adv VARCHAR,
        revenue_tier_agc VARCHAR,
        ly2_revenue DOUBLE,
        ly2q_revenue DOUBLE,
        ds VARCHAR
    )
    WITH (
        format = 'DWRF',
        oncall = 'gms_ca',
        partitioned_by = ARRAY['ds'],
        retention_days = 7
    )
    """,
    select=r"""
    SELECT
        coverage.ad_account_id,
        COALESCE(date_id, '<quarter_id>') date_id,
        revenue_segment,
        program_agency,
        vertical,
        sub_vertical,
        l12_agency_territory,
        l10_agency_territory,
        l8_agency_territory,
        l6_agency_territory,
        l4_agency_territory,
        l12_reporting_territory,
        l10_reporting_territory,
        l8_reporting_territory,
        l6_reporting_territory,
        l4_reporting_territory,
        l12_reseller_territory,
        l10_reseller_territory,
        l8_reseller_territory,
        l6_reseller_territory,
        l4_reseller_territory,
        hq_country_adv,
        hq_region_adv,
        is_gcc,
        is_gcm,
        revenue_tier_adv,
        revenue_tier_agc,
        -- Quotas
        (coverage.advertiser_quota) advertiser_quota,
        (coverage.agency_quota) agency_quota,
        (coverage.reseller_quota) reseller_quota,
        (coverage.optimal_goal) optimal_goal,
        (coverage.agc_optimal_goal) agc_optimal_goal,

        -- Forecasts
        (coverage.sales_forecast) sales_forecast,
        (coverage.sales_forecast_prior) sales_forecast_prior,

        (rev.cq_revenue * split) cq_revenue,
        (rev.cq_optimal * split) cq_optimal,
        (rev.pq_revenue * split) pq_revenue,
        (rev.pq_optimal * split) pq_optimal,
        (rev.ly_revenue * split) ly_revenue,
        (rev.ly_optimal * split) ly_optimal,
        (rev.lyq_revenue * split) lyq_revenue,
        (rev.lyq_optimal * split) lyq_optimal,
        (rev.ly2_revenue * split) ly2_revenue,
        (rev.ly2q_revenue * split) ly2q_revenue

    FROM <TABLE:gms_wbr_flat_daily_stg_1> coverage

    LEFT JOIN <TABLE:gms_wbr_stg_1_daily> rev

        ON rev.ad_account_id = coverage.ad_account_id

        AND rev.ds = '<DATEID>'

    WHERE
        coverage.ds = '<DATEID>'
    """,
)


gms_wbr_flat_daily_stg_2 = PrestoInsertOperator(
    dep_list=[
        gms_wbr_flat_daily,
    ],
    table="<TABLE:gms_wbr_flat_daily_stg_2>",
    partition="ds=<DATEID>",
    documentation={"description": """Staging Table prior to RTM Split"""},
    create=r"""
    CREATE TABLE IF NOT EXISTS <TABLE:gms_wbr_flat_daily_stg_2> (
        date_id VARCHAR,
        revenue_segment VARCHAR,
        program_agency VARCHAR,
        vertical VARCHAR,
        sub_vertical VARCHAR,
        l12_agency_territory VARCHAR,
        l10_agency_territory VARCHAR,
        l8_agency_territory VARCHAR,
        l6_agency_territory VARCHAR,
        l4_agency_territory VARCHAR,
        l12_reporting_territory VARCHAR,
        l10_reporting_territory VARCHAR,
        l8_reporting_territory VARCHAR,
        l6_reporting_territory VARCHAR,
        l4_reporting_territory VARCHAR,
        l12_reseller_territory VARCHAR,
        l10_reseller_territory VARCHAR,
        l8_reseller_territory VARCHAR,
        l6_reseller_territory VARCHAR,
        l4_reseller_territory VARCHAR,
        advertiser_quota DOUBLE,
        agency_quota DOUBLE,
        reseller_quota DOUBLE,
        optimal_goal DOUBLE,
        agc_optimal_goal DOUBLE,
        sales_forecast DOUBLE,
        sales_forecast_prior DOUBLE,
        cq_revenue DOUBLE,
        cq_optimal DOUBLE,
        pq_revenue DOUBLE,
        pq_optimal DOUBLE,
        ly_revenue DOUBLE,
        ly_optimal DOUBLE,
        lyq_revenue DOUBLE,
        lyq_optimal DOUBLE,
        hq_country_adv VARCHAR,
        hq_region_adv VARCHAR,
        is_gcc BOOLEAN,
        is_gcm BOOLEAN,
        revenue_tier_adv VARCHAR,
        revenue_tier_agc VARCHAR,
        ly2_revenue DOUBLE,
        ly2q_revenue DOUBLE,
        ds VARCHAR
    )
    WITH (
        format = 'DWRF',
        oncall = 'gms_ca',
        partitioned_by = ARRAY['ds'],
        retention_days = 7
    )
    """,
    select=r"""
    SELECT

        date_id,
        revenue_segment,
        program_agency,
        vertical,
        sub_vertical,
        l12_agency_territory,
        l10_agency_territory,
        l8_agency_territory,
        l6_agency_territory,
        l4_agency_territory,
        l12_reporting_territory,
        l10_reporting_territory,
        l8_reporting_territory,
        l6_reporting_territory,
        l4_reporting_territory,
        l12_reseller_territory,
        l10_reseller_territory,
        l8_reseller_territory,
        l6_reseller_territory,
        l4_reseller_territory,
        hq_country_adv,
        hq_region_adv,
        is_gcc,
        is_gcm,
        revenue_tier_adv,
        revenue_tier_agc,
        -- Quotas
        SUM(advertiser_quota) advertiser_quota,
        SUM(agency_quota) agency_quota,
        SUM(reseller_quota) reseller_quota,
        SUM(optimal_goal) optimal_goal,
        SUM(agc_optimal_goal) agc_optimal_goal,

        -- Forecasts
        SUM(sales_forecast) sales_forecast,
        SUM(sales_forecast_prior) sales_forecast_prior,

        SUM(cq_revenue) cq_revenue,
        SUM(cq_optimal) cq_optimal,
        SUM(pq_revenue) pq_revenue,
        SUM(pq_optimal) pq_optimal,
        SUM(ly_revenue) ly_revenue,
        SUM(ly_optimal) ly_optimal,
        SUM(lyq_revenue) lyq_revenue,
        SUM(lyq_optimal) lyq_optimal,
        SUM(ly2_revenue) ly2_revenue,
        SUM(ly2q_revenue) ly2q_revenue

    FROM <TABLE:gms_wbr_flat_daily> rev

    WHERE
        ds = '<DATEID>'

    GROUP BY

        date_id,
        revenue_segment,
        program_agency,
        vertical,
        sub_vertical,
        l12_agency_territory,
        l10_agency_territory,
        l8_agency_territory,
        l6_agency_territory,
        l4_agency_territory,
        l12_reporting_territory,
        l10_reporting_territory,
        l8_reporting_territory,
        l6_reporting_territory,
        l4_reporting_territory,
        l12_reseller_territory,
        l10_reseller_territory,
        l8_reseller_territory,
        l6_reseller_territory,
        l4_reseller_territory,
        hq_country_adv,
        hq_region_adv,
        is_gcc,
        is_gcm,
        revenue_tier_adv,
        revenue_tier_agc
    """,
)


gms_wbr_rtm_daily = PrestoInsertOperator(
    dep_list=[
        gms_wbr_flat_daily_stg_2,
    ],
    table="<TABLE:gms_wbr_rtm_daily>",
    partition="ds=<DATEID>",
    documentation={"description": """Staging Table prior to RTM Split"""},
    create=r"""
    CREATE TABLE IF NOT EXISTS <TABLE:gms_wbr_rtm_daily> (
        date_id VARCHAR,
        route_to_market VARCHAR,
        revenue_segment VARCHAR,
        l8_territory VARCHAR,
        l6_territory VARCHAR,
        l4_territory VARCHAR,
        quota DOUBLE,
        optimal_goal DOUBLE,
        sales_forecast DOUBLE,
        sales_forecast_prior DOUBLE,
        cq_revenue DOUBLE,
        cq_optimal DOUBLE,
        pq_revenue DOUBLE,
        pq_optimal DOUBLE,
        ly_revenue DOUBLE,
        ly_optimal DOUBLE,
        lyq_revenue DOUBLE,
        lyq_optimal DOUBLE,
        hq_country VARCHAR,
        hq_region VARCHAR,
        is_gcc BOOLEAN,
        is_gcm BOOLEAN,
        revenue_tier VARCHAR,
        ly2_revenue DOUBLE,
        ly2q_revenue DOUBLE,
        ds VARCHAR
    )
    WITH (
        format = 'DWRF',
        oncall = 'gms_ca',
        partitioned_by = ARRAY['ds'],
        retention_days = 7
    )
    """,
    select=r"""
    SELECT

        date_id,
        'Reporting' AS route_to_market,
        revenue_segment,
        l8_reporting_territory l8_territory,
        l6_reporting_territory l6_territory,
        l4_reporting_territory l4_territory,
        hq_country_adv hq_country,
        hq_region_adv hq_region,
        is_gcc,
        is_gcm,
        revenue_tier_adv revenue_tier,
        SUM(advertiser_quota) quota,
        SUM(optimal_goal) optimal_goal,
        SUM(sales_forecast) sales_forecast,
        SUM(sales_forecast_prior) sales_forecast_prior,
        SUM(cq_revenue) cq_revenue,
        SUM(cq_optimal) cq_optimal,
        SUM(pq_revenue) pq_revenue,
        SUM(pq_optimal) pq_optimal,
        SUM(ly_revenue) ly_revenue,
        SUM(ly_optimal) ly_optimal,
        SUM(lyq_revenue) lyq_revenue,
        SUM(lyq_optimal) lyq_optimal,
        SUM(ly2_revenue) ly2_revenue,
        SUM(ly2q_revenue) ly2q_revenue

    FROM <TABLE:gms_wbr_flat_daily_stg_2>

    WHERE
        ds = '<DATEID>'
        AND revenue_segment IN ('GBG In-Market', 'GBG Scaled', 'GBG Unmanaged')

    GROUP BY

        date_id,
        revenue_segment,
        l8_reporting_territory,
        l6_reporting_territory,
        l4_reporting_territory,
        hq_country_adv,
        hq_region_adv,
        is_gcc,
        is_gcm,
        revenue_tier_adv

    UNION ALL

    SELECT

        date_id,
        'Advertiser' AS route_to_market,
        revenue_segment,
        l8_reporting_territory l8_territory,
        l6_reporting_territory l6_territory,
        l4_reporting_territory l4_territory,
        hq_country_adv hq_country,
        hq_region_adv hq_region,
        is_gcc,
        is_gcm,
        revenue_tier_adv revenue_tier,
        SUM(advertiser_quota) quota,
        SUM(optimal_goal) optimal_goal,
        SUM(sales_forecast) sales_forecast,
        SUM(sales_forecast_prior) sales_forecast_prior,
        SUM(cq_revenue) cq_revenue,
        SUM(cq_optimal) cq_optimal,
        SUM(pq_revenue) pq_revenue,
        SUM(pq_optimal) pq_optimal,
        SUM(ly_revenue) ly_revenue,
        SUM(ly_optimal) ly_optimal,
        SUM(lyq_revenue) lyq_revenue,
        SUM(lyq_optimal) lyq_optimal,
        SUM(ly2_revenue) ly2_revenue,
        SUM(ly2q_revenue) ly2q_revenue

    FROM <TABLE:gms_wbr_flat_daily_stg_2>

    WHERE
        ds = '<DATEID>'
        AND revenue_segment IN ('GBG In-Market', 'GBG Scaled')

    GROUP BY

        date_id,
        revenue_segment,
        l8_reporting_territory,
        l6_reporting_territory,
        l4_reporting_territory,
        hq_country_adv,
        hq_region_adv,
        is_gcc,
        is_gcm,
        revenue_tier_adv

    UNION ALL

    SELECT

        date_id,
        'Agency' AS route_to_market,
        program_agency revenue_segment,
        l8_agency_territory l8_territory,
        l6_agency_territory l6_territory,
        l4_agency_territory l4_territory,
        NULL hq_country,
        NULL hq_region,
        NULL is_gcc,
        NULL is_gcm,
        revenue_tier_agc revenue_tier,
        SUM(agency_quota) quota,
        SUM(agc_optimal_goal) optimal_goal,
        SUM(0) sales_forecast,
        SUM(0) sales_forecast_prior,
        SUM(cq_revenue) cq_revenue,
        SUM(cq_optimal) cq_optimal,
        SUM(pq_revenue) pq_revenue,
        SUM(pq_optimal) pq_optimal,
        SUM(ly_revenue) ly_revenue,
        SUM(ly_optimal) ly_optimal,
        SUM(lyq_revenue) lyq_revenue,
        SUM(lyq_optimal) lyq_optimal,
        SUM(ly2_revenue) ly2_revenue,
        SUM(ly2q_revenue) ly2q_revenue

    FROM <TABLE:gms_wbr_flat_daily_stg_2>

    WHERE
        ds = '<DATEID>'
        AND program_agency IN ('GBG In-Market Agency', 'GBG Scaled Agency')

    GROUP BY

        date_id,
        program_agency,
        l8_agency_territory,
        l6_agency_territory,
        l4_agency_territory,
        revenue_tier_agc

    UNION ALL

    SELECT

        date_id,
        'Reseller' route_to_market,
        'Reseller' revenue_segment,
        l8_reseller_territory l8_territory,
        l6_reseller_territory l6_territory,
        l4_reseller_territory l4_territory,
        NULL hq_country,
        NULL hq_region,
        NULL is_gcc,
        NULL is_gcm,
        revenue_tier_adv revenue_tier,
        SUM(reseller_quota) quota,
        SUM(optimal_goal) optimal_goal,
        SUM(0) sales_forecast,
        SUM(0) sales_forecast_prior,
        SUM(cq_revenue) cq_revenue,
        SUM(cq_optimal) cq_optimal,
        SUM(pq_revenue) pq_revenue,
        SUM(pq_optimal) pq_optimal,
        SUM(ly_revenue) ly_revenue,
        SUM(ly_optimal) ly_optimal,
        SUM(lyq_revenue) lyq_revenue,
        SUM(lyq_optimal) lyq_optimal,
        SUM(ly2_revenue) ly2_revenue,
        SUM(ly2q_revenue) ly2q_revenue

    FROM <TABLE:gms_wbr_flat_daily_stg_2>

    WHERE
        ds = '<DATEID>'
        AND reseller_quota IS NOT NULL

    GROUP BY

        date_id,
        l8_reseller_territory,
        l6_reseller_territory,
        l4_reseller_territory,
        revenue_tier_adv
    """,
)


gms_wbr_rtm_daily_v = PrestoOperator(
    dep_list=[
        gms_wbr_rtm_daily,
    ],
    query="""
    CREATE OR REPLACE VIEW <TABLE:gms_wbr_rtm_daily_v> AS
    SELECT
        date_id,
        route_to_market,
        revenue_segment,
        l8_territory,
        l6_territory,
        l4_territory,
        quota,
        optimal_goal,
        sales_forecast,
        sales_forecast_prior,
        cq_revenue,
        cq_optimal,
        pq_revenue,
        pq_optimal,
        ly_revenue,
        ly_optimal,
        lyq_revenue,
        lyq_optimal,
        hq_country,
        hq_region,
        is_gcc,
        is_gcm,
        revenue_tier,
        ly2_revenue,
        ly2q_revenue,
        ds

    FROM <TABLE:gms_wbr_rtm_daily>

    WHERE
        ds = '<DATEID>'
    """,
)

if is_test():
    pass
else:

    tableau_refresh_gms_wbr_rtm = TableauPublishOperator(
        dep_list=[
            gms_wbr_rtm_v,
            gms_wbr_rtm_daily_v,
        ],
        refresh_cfg_id=29320,
        num_retries=2,
    )


if is_test():
    pass
else:

    tableau_refresh_gms_wbr_rtm_5q = TableauPublishOperator(
        dep_list=[
            gms_wbr_rtm_5q_v,
        ],
        refresh_cfg_id=29348,
        num_retries=2,
    )
