#!/usr/bin/env python3

from __future__ import absolute_import, division, print_function

from dataswarm.ds_utils.schema import Column, HiveAnon, Table
from dataswarm.operators import (
    GlobalDefaults,
    PrestoInsertOperatorWithSchema,
    WaitForHiveOperator,
)


GlobalDefaults.set(
    schedule="@daily",
    data_project_acl="gms_central_analytics",
    user="mmazur",
    oncall="gms_central_analytics",
    depends_on_past=False,
    fail_on_empty=True,
    max_concurrent=99,
)

wait_for_staging_bpt_cq_fct_rev_forecast_quota = WaitForHiveOperator(
    dep_list=[],
    table="staging_bpt_cq_fct_rev_forecast_quota",
    use_future=False,
    fail_on_future=True,
    num_retries=2000,
)

wait_for_staging_bpt_cq_dim_cov_comm = WaitForHiveOperator(
    dep_list=[],
    table="staging_bpt_cq_dim_cov_comm",
    use_future=False,
    fail_on_future=True,
    num_retries=2000,
)

wait_for_staging_bpt_cq_dim_territory_detail = WaitForHiveOperator(
    dep_list=[],
    table="staging_bpt_cq_dim_territory_detail",
    use_future=False,
    fail_on_future=True,
    num_retries=2000,
)

wait_for_staging_bpt_revenue_bob_forecast_quota_dash = WaitForHiveOperator(
    dep_list=[],
    table="staging_bpt_revenue_bob_forecast_quota_dash",
    use_future=False,
    fail_on_future=True,
    num_retries=2000,
)

wait_for_staging_bpt_dim_cov_comm_dash_as_was = WaitForHiveOperator(
    dep_list=[],
    table="staging_bpt_dim_cov_comm_dash_as_was",
    use_future=False,
    fail_on_future=True,
    num_retries=2000,
)

wait_for_staging_bpt_dim_ad_account_detail_dash_as_was = WaitForHiveOperator(
    dep_list=[],
    table="staging_bpt_dim_ad_account_detail_dash_as_was",
    use_future=False,
    fail_on_future=True,
    num_retries=2000,
)


wait_for_staging_bpt_dim_territory_detail_dash_as_was = WaitForHiveOperator(
    dep_list=[],
    table="staging_bpt_dim_territory_detail_dash_as_was",
    use_future=False,
    fail_on_future=True,
    num_retries=2000,
)

analytics_hub_playground_revenue_asis = PrestoInsertOperatorWithSchema(
    dep_list=[
    wait_for_staging_bpt_cq_fct_rev_forecast_quota,
    wait_for_staging_bpt_cq_dim_cov_comm,
    wait_for_staging_bpt_cq_dim_territory_detail,
    ],
    table="<TABLE:analytics_hub_playground_revenue_asis>",
    partition="ds=<DATEID>",
    documentation={
        "description": """Creates Analytics Playground Hub Revenue ASIS Data for Thoughtspot DQ Checks."""
    },
    create=Table(
        cols=[
            Column("l12_territory_name", "VARCHAR", ""),
            Column("l10_territory_name", "VARCHAR", ""),
            Column("l8_territory_name", "VARCHAR", ""),
            Column("l6_territory_name", "VARCHAR", ""),
            Column("l4_territory_name", "VARCHAR", ""),
            Column("l2_territory_name", "VARCHAR", ""),
            Column("revenue_segment", "VARCHAR", ""),
            Column("qtd_rev", "DOUBLE", ""),
            Column("ly_qtd_rev", "DOUBLE", ""),
            Column("l2y_qtd_rev", "DOUBLE", ""),
            Column("qtd_rev_lw", "DOUBLE", ""),
            Column("l2y_qtd_rev_lw", "DOUBLE", ""),
            Column("ly_qtd_rev_lw", "DOUBLE", ""),
            Column("lyq_rev", "DOUBLE", ""),
            Column("l2yq_rev", "DOUBLE", ""),
            Column("sales_forecast", "DOUBLE", ""),
            Column("sales_forecast_lw", "DOUBLE", ""),
            Column("optimal_revn", "DOUBLE", ""),
            Column("quota", "DOUBLE", ""),
        ],
        partitions=[
            Column("ds", "VARCHAR", ""),
        ],
        comment="",
        initial_retention=89,
    ),
    select=r"""

SELECT
    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."ds"
        AS "ds",
    "reporting_territories"."l10_territory_name" AS "l10_territory_name",
    "reporting_territories"."l12_territory_name" AS "l12_territory_name",
    "reporting_territories"."l2_territory_name" AS "l2_territory_name",
    "reporting_territories"."l4_territory_name" AS "l4_territory_name",
    "reporting_territories"."l6_territory_name" AS "l6_territory_name",
    "reporting_territories"."l8_territory_name" AS "l8_territory_name",
    "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."revenue_segment"
        AS "revenue_segment",
    SUM(
        (
            CASE
                WHEN 100 = 0 THEN CAST(NULL AS DOUBLE)
                ELSE (
                    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."qtd_rev"
                        * "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."split_pct"
                ) / 100
            END
        )
    ) AS "qtd_rev",
    SUM(
        (
            CASE
                WHEN 100 = 0 THEN CAST(NULL AS DOUBLE)
                ELSE (
                    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."qtd_rev_lw"
                        * "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."split_pct"
                ) / 100
            END
        )
    ) AS "qtd_rev_lw",
    SUM(
        (
            CASE
                WHEN 100 = 0 THEN CAST(NULL AS DOUBLE)
                ELSE (
                    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."ly_qtd_rev"
                        * "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."split_pct"
                ) / 100
            END
        )
    ) AS "ly_qtd_rev",
    SUM(
        (
            CASE
                WHEN 100 = 0 THEN CAST(NULL AS DOUBLE)
                ELSE (
                    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."l2y_qtd_rev"
                        * "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."split_pct"
                ) / 100
            END
        )
    ) AS "l2y_qtd_rev",
    SUM(
        (
            CASE
                WHEN 100 = 0 THEN CAST(NULL AS DOUBLE)
                ELSE (
                    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."lyq_rev"
                        * "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."split_pct"
                ) / 100
            END
        )
    ) AS "lyq_rev",
    SUM(
        (
            CASE
                WHEN 100 = 0 THEN CAST(NULL AS DOUBLE)
                ELSE (
                    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."l2y_qtd_rev_lw"
                        * "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."split_pct"
                ) / 100
            END
        )
    ) AS "l2y_qtd_rev_lw",
    SUM(
        (
            CASE
                WHEN 100 = 0 THEN CAST(NULL AS DOUBLE)
                ELSE (
                    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."ly_qtd_rev_lw"
                        * "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."split_pct"
                ) / 100
            END
        )
    ) AS "ly_qtd_rev_lw",
    SUM(
        (
            CASE
                WHEN 100 = 0 THEN CAST(NULL AS DOUBLE)
                ELSE (
                    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."l2yq_rev"
                        * "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."split_pct"
                ) / 100
            END
        )
    ) AS "l2yq_rev",
    SUM(
        "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."qtd_optimal_revn"
    ) AS "optimal_revn",
    SUM(
        "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."quota"
    ) AS "quota",
    SUM(
        "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."sales_forecast"
    ) AS "sales_forecast",
    SUM(
        "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."sales_forecast_lw"
    ) AS "sales_forecast_lw"
FROM "edw_bir01"."staging_bpt_cq_fct_rev_forecast_quota" "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"
INNER JOIN "edw_bir01"."staging_bpt_cq_dim_cov_comm" "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"
    ON (
        (
            "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."cov_com_join"
                = "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."fact_join"
        )
        AND (
            "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."ds"
                = "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."ds"
        )
    )
INNER JOIN "edw_bir01"."staging_bpt_cq_dim_territory_detail" "reporting_territories"
    ON (
        (
            "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."reporting_territory_join"
                = "reporting_territories"."l12_territory_fbid"
        )
        AND (
            "staging_bpt_cq_dim_cov_comm (edw_bir01.staging_bpt_cq_dim_cov_comm)"."ds"
                = "reporting_territories"."ds"
        )
    )
WHERE
    (
        (
            CASE
                WHEN (
                    "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."ds"
                        = "staging_bpt_cq_fct_rev_forecast_quota (edw_bir01.staging_bpt_cq_fct_rev_forecast_quota)"."date_id"
                ) THEN 'Latest date'
                ELSE 'Other Dates'
            END
        ) = 'Latest date'
    )
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8

    """,
)


analytics_hub_revenue_playground_historical = PrestoInsertOperatorWithSchema(
    dep_list=[
    wait_for_staging_bpt_revenue_bob_forecast_quota_dash,
    wait_for_staging_bpt_dim_cov_comm_dash_as_was,
    wait_for_staging_bpt_dim_ad_account_detail_dash_as_was,
    wait_for_staging_bpt_dim_territory_detail_dash_as_was,
    ],
    table="<TABLE:analytics_hub_revenue_playground_historical>",
    partition="ds=<DATEID>",
    documentation={
        "description": """Creates Analytics Playground Hub Revenue ASIS Data for Thoughtspot DQ Checks."""
    },
    create=Table(
        cols=[
            Column("l12_territory_name", "VARCHAR", ""),
            Column("l10_territory_name", "VARCHAR", ""),
            Column("l8_territory_name", "VARCHAR", ""),
            Column("l6_territory_name", "VARCHAR", ""),
            Column("l4_territory_name", "VARCHAR", ""),
            Column("l2_territory_name", "VARCHAR", ""),
            Column("commission_split", "VARCHAR", ""),
            Column("quarter_id", "VARCHAR", ""),
            Column("asis_rec_rev", "DOUBLE", ""),
            Column("sales_forecast", "DOUBLE", ""),
            Column("quota", "DOUBLE", ""),
        ],
        partitions=[
            Column("ds", "VARCHAR", ""),
        ],
        comment="",
        initial_retention=89,
    ),
    select=r"""

SELECT
    "staging_bpt_snapshot_bob_day_dim_dash"."reporting_ds"  AS "ds",
    "staging_bpt_dim_cov_comm_dash_as_was"."commission_split_filter" AS "commission_split",
    "staging_bpt_dim_territory_detail_dash_as_was"."l10_territory_name" AS "l10_territory_name",
    "staging_bpt_dim_territory_detail_dash_as_was"."l12_territory_name" AS "l12_territory_name",
    "staging_bpt_dim_territory_detail_dash_as_was"."l2_territory_name" AS "l2_territory_name",
    "staging_bpt_dim_territory_detail_dash_as_was"."l4_territory_name" AS "l4_territory_name",
    "staging_bpt_dim_territory_detail_dash_as_was"."l6_territory_name" AS "l6_territory_name",
    "staging_bpt_dim_territory_detail_dash_as_was"."l8_territory_name" AS "l8_territory_name",
    "staging_bpt_snapshot_bob_day_dim_dash"."quarter_id" AS "quarter_id",
    SUM("staging_bpt_revenue_bob_forecast_quota_dash"."quota") AS "quota",
    SUM("staging_bpt_revenue_bob_forecast_quota_dash"."sales_forecast") AS "sales_forecast",
    SUM(
        (
            CASE
                WHEN 100 = 0 THEN CAST(NULL AS DOUBLE)
                ELSE (
                    "staging_bpt_revenue_bob_forecast_quota_dash"."asis_rec_rev"
                        * "staging_bpt_dim_cov_comm_dash_as_was"."split_pct"
                ) / 100
            END
        )
    ) AS "asis_rec_rev"
FROM "edw_bir01"."staging_bpt_revenue_bob_forecast_quota_dash" "staging_bpt_revenue_bob_forecast_quota_dash"
INNER JOIN "edw_bir01"."staging_bpt_snapshot_bob_day_dim_dash" "staging_bpt_snapshot_bob_day_dim_dash"
    ON (
        "staging_bpt_revenue_bob_forecast_quota_dash"."reporting_ds_join"
            = "staging_bpt_snapshot_bob_day_dim_dash"."reporting_ds_join"
    )
INNER JOIN "edw_bir01"."staging_bpt_dim_cov_comm_dash_as_was" "staging_bpt_dim_cov_comm_dash_as_was"
    ON (
        (
            "staging_bpt_revenue_bob_forecast_quota_dash"."cov_com_join"
                = "staging_bpt_dim_cov_comm_dash_as_was"."fact_join"
        )
        AND (
            "staging_bpt_snapshot_bob_day_dim_dash"."reporting_ds"
                = "staging_bpt_dim_cov_comm_dash_as_was"."reporting_ds"
        )
    )
INNER JOIN "edw_bir01"."staging_bpt_dim_ad_account_detail_dash_as_was" "staging_bpt_dim_ad_account_detail_dash_as_was"
    ON (
        (
            "staging_bpt_revenue_bob_forecast_quota_dash"."account_id"
                = "staging_bpt_dim_ad_account_detail_dash_as_was"."account_id"
        )
        AND (
            "staging_bpt_snapshot_bob_day_dim_dash"."reporting_ds"
                = "staging_bpt_dim_ad_account_detail_dash_as_was"."reporting_ds"
        )
    )
INNER JOIN "edw_bir01"."staging_bpt_dim_territory_detail_dash_as_was" "staging_bpt_dim_territory_detail_dash_as_was"
    ON (
        (
            "staging_bpt_dim_cov_comm_dash_as_was"."territory_join"
                = "staging_bpt_dim_territory_detail_dash_as_was"."l12_territory_fbid"
        )
        AND (
            "staging_bpt_snapshot_bob_day_dim_dash"."reporting_ds"
                = "staging_bpt_dim_territory_detail_dash_as_was"."reporting_ds"
        )
    )
WHERE


            CAST("staging_bpt_snapshot_bob_day_dim_dash"."reporting_ds" AS DATE)
                = CAST('<DATEID>' AS DATE)

        AND
            "staging_bpt_dim_cov_comm_dash_as_was"."commission_split_filter" IN (
                'Client Partner',
                'SMB Account Strategist AM' )


        AND cast("staging_bpt_snapshot_bob_day_dim_dash"."quarter_id" as date) in (

date_trunc('quarter',  CAST('<DATEID>' AS DATE) ),                          --quarter_id,
date_trunc('quarter',  CAST('<DATEID>' AS DATE) ) - interval '3' month,     --quarter_id_minus_1,
date_trunc('quarter',  CAST('<DATEID>' AS DATE) ) - interval '6' month,     --quarter_id_minus_2,
date_trunc('quarter',  CAST('<DATEID>' AS DATE) ) - interval '9' month,     --quarter_id_minus_3,
date_trunc('quarter',  CAST('<DATEID>' AS DATE) ) - interval '12' month,    --quarter_id_minus_4,
date_trunc('quarter',  CAST('<DATEID>' AS DATE) ) - interval '15' month    --quarter_id_minus_5

        )


GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9

    """,
)
