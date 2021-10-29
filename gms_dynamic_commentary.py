#!/usr/bin/env python3
# @lint-ignore-every DSFMT [do not copy-paste]. Format the file to remove this comment. https://fburl.com/wiki/mmzh332m

from __future__ import absolute_import, division, print_function

from dataswarm.ds_utils.schema import Column, HiveAnon, Table
from dataswarm.operators import (
    GlobalDefaults,
    PrestoInsertOperatorWithSchema,
    WaitForHiveOperator,
    TableauPublishOperator,
)
from dataswarm.operators.runcontext import is_test


GlobalDefaults.set(
    user="mmazur",
    schedule="@daily",
    num_retries=1,
    depends_on_past=False,
    macros={
        "QUARTERID": "CAST(DATE_TRUNC('quarter',DATE('<DATEID>')) AS VARCHAR)",
    },
    secure_group="gms_tech_data_eng",
    oncall="bp_t_gms_tech_de_bp_o",
    partition="ds=<DATEID>",
    extra_emails=[],
)

wait_for_bpo_quota_and_forecast_fast = WaitForHiveOperator(
    dep_list=[],
    table="bpo_gms_quota_and_forecast_fast",
    use_future=False,
    fail_on_future=True,
    num_retries=2000,
)

gms_dynamic_comments_stg_1_advertiser = PrestoInsertOperatorWithSchema(
    dep_list=[wait_for_bpo_quota_and_forecast_fast],
    table="<TABLE:gms_dynamic_comments_stg_1>",
    partition="ds=<DATEID>/route_to_market=Advertiser",
    documentation={
        "description": """Creates Dynamic Commentary for Overall Stats of
          L2-L12 Territories for Sales Forecast, Seasonal Forecast, Run Rate
          Forecast & Straightline Forecast.""",
    },
    create=Table(
        cols=[
            Column("level_type", "VARCHAR", ""),
            Column("level", "VARCHAR", ""),
            Column("level_comments_sales_forecast", "VARCHAR", ""),
            Column("level_comments_seasonal_forecast", "VARCHAR", ""),
            Column("level_comments_run_rate_forecast", "VARCHAR", ""),
            Column("level_comments_straightline_forecast", "VARCHAR", ""),
            Column("required_run_rate", "VARCHAR", ""),
        ],
        partitions=[
            Column("ds", "VARCHAR", ""),
            Column("route_to_market", "VARCHAR", ""),
        ],
        comment="",
        initial_retention=1,
    ),
    select=r"""
    WITH comments_stg_1_adv AS (
        SELECT
            c2.level_type,
            c2.level,
            asofdate,
            SUM(lyq_revenue) lyq_revenue,
            SUM(lyq_revenue_qtd) lyq_revenue_qtd,
            SUM(pq_revenue) pq_revenue,
            SUM(pq_revenue_qtd) pq_revenue_qtd,
            SUM(advertiser_quota) quota,
            SUM(l7d_avg_revenue) daily_run_rate,
            SUM(l7d_avg_revenue_prior) daily_run_rate_prior,
            SUM(
                CASE
                    WHEN revenue_segment = 'GBG In-Market' THEN sales_forecast
                    ELSE ((cq_revenue) / (days_closed_in_quarter))
                        * (days_total_in_quarter)
                END
            ) sales_forecast,
            SUM(
                CASE
                    WHEN revenue_segment = 'GBG In-Market' THEN sales_forecast_prior
                    ELSE ((cq_revenue_qtd_prior) / (days_closed_in_quarter - 7))
                        * (days_total_in_quarter)
                END
            ) sales_forecast_prior,
            SUM(run_rate_forecast) run_rate_forecast,
            SUM(run_rate_forecast_prior) run_rate_forecast_prior,
            SUM(cq_revenue) cq_revenue,
            SUM(cq_revenue_qtd_prior) cq_revenue_qtd_prior,
            SUM(lyq_revenue_qtd) / SUM(lyq_revenue) Ly_delivery,
            SUM(lyq_revenue_qtd_prior) / SUM(lyq_revenue) Ly_delivery_prior,
            SUM(advertiser_quota) * (SUM(lyq_revenue_qtd) / SUM(lyq_revenue)) quota_qtd,
            SUM(advertiser_quota) * (
                SUM(lyq_revenue_qtd_prior) / SUM(lyq_revenue)
            ) quota_qtd_prior,
            (SUM(cq_revenue) / MIN(days_closed_in_quarter)) * MIN(
                days_total_in_quarter
            ) straightline_forecast,
            (SUM(cq_revenue_qtd_prior) / MIN(days_closed_in_quarter - 7))
                * MIN(days_total_in_quarter) straightline_forecast_prior,
            (SUM(advertiser_quota) - SUM(cq_revenue)) / DATE_DIFF(
                'day',
                CAST(<QUARTERID> AS date) - INTERVAL '1' DAY,
                CAST(asofdate AS date)
            ) AS required_run_rate
        FROM bpo_gms_quota_and_forecast_fast
        CROSS JOIN UNNEST(
                ARRAY[
                    'Level 2 Advertiser',
                    'Level 4 Advertiser',
                    'Level 6 Advertiser',
                    'Level 8 Advertiser',
                    'Level 10 Advertiser',
                    'Level 12 Advertiser'
                ],
                ARRAY[
                    l2_reporting_territory,
                    l4_reporting_territory,
                    l6_reporting_territory,
                    l8_reporting_territory,
                    l10_reporting_territory,
                    l12_reporting_territory
                ]
            ) c2 (level_type, level)
        WHERE
            ds = '<DATEID>'
            AND l2_reporting_territory NOT IN ('Reseller', 'GBG Agency')
            and revenue_segment not in ('GPA Managed','GPA Unmanaged')
        GROUP BY
            1, 2, 3
    ),
    comments_stg_2_adv AS (
        SELECT
            level,
            level_type,
            quota,
            cq_revenue,
            pq_revenue,
            lyq_revenue,
            (cq_revenue / quota) attainment_qtd,
            (cq_revenue / lyq_revenue_qtd) - 1 yoy_perc_revenue,
            (cq_revenue / pq_revenue_qtd) - 1 qoq_perc_revenue,
            (cq_revenue / quota) - (cq_revenue_qtd_prior / quota) wow_perc_quota,
            sales_forecast,
            sales_forecast_prior,
            sales_forecast / quota perc_sales_forecast,
            sales_forecast_prior / quota perc_sales_forecast_prior,
            (sales_forecast / lyq_revenue) - 1 yoy_perc_sales_forecast,
            (sales_forecast / lyq_revenue) - 1 qoq_perc_sales_forecast,
            (sales_forecast / sales_forecast_prior) - 1 wow_perc_sales_forecast,
            (cq_revenue / quota_qtd) * quota seasonal_forecast,
            (cq_revenue_qtd_prior / quota_qtd_prior) * quota seasonal_forecast_prior,
            ((cq_revenue / quota_qtd) * quota) / quota perc_seasonal_forecast,
            ((cq_revenue_qtd_prior / quota_qtd_prior) * quota) / quota perc_seasonal_forecast_prior,
            (((cq_revenue / quota_qtd) * quota) / lyq_revenue) - 1 qoq_perc_seasonal_forecast,
            (((cq_revenue / quota_qtd) * quota) / lyq_revenue) - 1 yoy_perc_seasonal_forecast,
            (
                ((cq_revenue / quota_qtd) * quota) / (
                    (cq_revenue_qtd_prior / quota_qtd_prior) * quota
                )
            ) - 1 wow_perc_seasonal_forecast,
            run_rate_forecast,
            run_rate_forecast / quota perc_run_rate_forecast,
            run_rate_forecast_prior / quota perc_run_rate_forecast_prior,
            (run_rate_forecast_prior / quota) - (run_rate_forecast / quota) wow_pp_run_rate_forecast,
            (run_rate_forecast / run_rate_forecast_prior) - 1 wow_perc_run_rate_forecast,
            daily_run_rate,
            daily_run_rate_prior,
            required_run_rate,
            (daily_run_rate / daily_run_rate_prior) - 1 wow_perc_daily_run_rate,
            straightline_forecast,
            straightline_forecast_prior,
            straightline_forecast / quota perc_straightline_forecast,
            straightline_forecast_prior / quota perc_straightline_forecast_prior,
            (straightline_forecast / lyq_revenue) - 1 yoy_perc_straightline_forecast,
            (straightline_forecast / lyq_revenue) - 1 qoq_perc_straightline_forecast,
            (straightline_forecast / straightline_forecast_prior) - 1 wow_perc_straightline_forecast,
            asofdate ds,
            asofdate
        FROM comments_stg_1_adv
    ),
comments_stg_3_adv AS (
    SELECT
        level,
        level_type,
        quota,
        cq_revenue,
        pq_revenue,
        lyq_revenue,
        attainment_qtd,
        yoy_perc_revenue,
        qoq_perc_revenue,
        wow_perc_quota,
        sales_forecast,
        sales_forecast_prior,
        perc_sales_forecast,
        perc_sales_forecast_prior,
        yoy_perc_sales_forecast,
        qoq_perc_sales_forecast,
        wow_perc_sales_forecast,
        seasonal_forecast,
        seasonal_forecast_prior,
        perc_seasonal_forecast,
        perc_seasonal_forecast_prior,
        qoq_perc_seasonal_forecast,
        yoy_perc_seasonal_forecast,
        wow_perc_seasonal_forecast,
        run_rate_forecast,
        perc_run_rate_forecast,
        perc_run_rate_forecast_prior,
        wow_pp_run_rate_forecast,
        wow_perc_run_rate_forecast,
        daily_run_rate,
        daily_run_rate_prior,
        required_run_rate,
        wow_perc_daily_run_rate,
        straightline_forecast,
        straightline_forecast_prior,
        perc_straightline_forecast,
        perc_straightline_forecast_prior,
        yoy_perc_straightline_forecast,
        qoq_perc_straightline_forecast,
        wow_perc_straightline_forecast,
        (perc_seasonal_forecast - perc_seasonal_forecast_prior) pp_wow_seasonal_forecast,
        (perc_sales_forecast - perc_sales_forecast_prior) pp_wow_sales_forecast,
        (perc_straightline_forecast - perc_straightline_forecast_prior) pp_wow_straightline_forecast,
        (perc_run_rate_forecast - perc_run_rate_forecast_prior) pp_wow_run_rate_forecast,
        asofdate ds,
        asofdate
    FROM comments_stg_2_adv
),
    comments_stg_4_adv AS (
        SELECT
            level,
            level_type,
            quota,
            cq_revenue,
            pq_revenue,
            lyq_revenue,
            attainment_qtd,
            yoy_perc_revenue,
            qoq_perc_revenue,
            wow_perc_quota,
            sales_forecast,
            sales_forecast_prior,
            perc_sales_forecast,
            perc_sales_forecast_prior,
            yoy_perc_sales_forecast,
            qoq_perc_sales_forecast,
            wow_perc_sales_forecast,
            seasonal_forecast,
            seasonal_forecast_prior,
            perc_seasonal_forecast,
            perc_seasonal_forecast_prior,
            qoq_perc_seasonal_forecast,
            yoy_perc_seasonal_forecast,
            wow_perc_seasonal_forecast,
            run_rate_forecast,
            perc_run_rate_forecast,
            perc_run_rate_forecast_prior,
            wow_pp_run_rate_forecast,
            wow_perc_run_rate_forecast,
            daily_run_rate,
            daily_run_rate_prior,
            required_run_rate,
            wow_perc_daily_run_rate,
            straightline_forecast,
            straightline_forecast_prior,
            perc_straightline_forecast,
            perc_straightline_forecast_prior,
            yoy_perc_straightline_forecast,
            qoq_perc_straightline_forecast,
            wow_perc_straightline_forecast,
        CASE
            WHEN pp_wow_sales_forecast > 0.05
                 THEN 'a strong increase of '
            WHEN pp_wow_sales_forecast BETWEEN 0.02
                AND 0.05 THEN 'good growth of '
            WHEN pp_wow_sales_forecast BETWEEN 0.001
                AND 0.02 THEN 'slight growth of '
            WHEN pp_wow_sales_forecast BETWEEN 0
                 AND 0.001 THEN 'no change of '
            WHEN pp_wow_sales_forecast < -0.05
                THEN 'a strong decrease of '
            WHEN pp_wow_sales_forecast BETWEEN -0.05
            AND -0.02    THEN 'a slight decline of '
            WHEN pp_wow_sales_forecast BETWEEN -0.02
                AND -0.001 THEN 'a small loss of '
            WHEN pp_wow_sales_forecast BETWEEN -0.001
                AND 0 THEN 'no change of '
            ELSE ''
        END wow_perc_sales_forecast_comments,
        CASE
            WHEN pp_wow_seasonal_forecast > 0.05
                THEN 'a significant increase of '
            WHEN pp_wow_seasonal_forecast BETWEEN 0.02
                AND 0.05
                THEN 'a great upward move of '
            WHEN pp_wow_seasonal_forecast BETWEEN 0.001
                AND 0.02
                THEN 'slight increase of '
            WHEN pp_wow_seasonal_forecast BETWEEN 0
                 AND 0.001
                THEN 'a stagnant '
            WHEN pp_wow_seasonal_forecast < -0.05
                THEN 'a decrease of '
            WHEN pp_wow_seasonal_forecast BETWEEN -0.05
            AND -0.02 THEN 'a drop of '
            WHEN pp_wow_seasonal_forecast BETWEEN -0.02
                AND -0.001 THEN 'a small drop of '
            WHEN pp_wow_seasonal_forecast BETWEEN -0.001
                AND 0 THEN 'a stagnant '
            ELSE ''
        END wow_perc_seasonal_forecast_comments,
        CASE
            WHEN pp_wow_straightline_forecast > 0.05
                THEN 'a significant increase of '
            WHEN pp_wow_straightline_forecast BETWEEN 0.02
                AND 0.05
                THEN 'a great upward move of '
            WHEN pp_wow_straightline_forecast BETWEEN 0.001
                AND 0.02
                THEN 'slight increase of '
            WHEN pp_wow_straightline_forecast BETWEEN 0
                 AND 0.001
                THEN 'a stagnant '
            WHEN pp_wow_straightline_forecast < -0.05
                THEN 'a decrease of '
            WHEN pp_wow_straightline_forecast BETWEEN -0.05
            AND -0.02
                THEN 'a drop of '
            WHEN pp_wow_straightline_forecast BETWEEN -0.02
                AND -0.001
                THEN 'a small drop of '
            WHEN pp_wow_straightline_forecast BETWEEN -0.001
                AND 0 THEN 'a stagnant '
            ELSE ''
        END wow_perc_straightline_forecast_comments,
        CASE
            WHEN pp_wow_run_rate_forecast > 0.05
                THEN 'a strong increase of '
            WHEN pp_wow_run_rate_forecast BETWEEN 0.02
                AND 0.05
                THEN 'good growth of '
            WHEN pp_wow_run_rate_forecast BETWEEN 0.001
                AND 0.02
                THEN 'slight growth of '
            WHEN pp_wow_run_rate_forecast BETWEEN 0
                 AND 0.001
                THEN 'no change of '
            WHEN pp_wow_run_rate_forecast < -0.05
                THEN 'a strong decrease of '
            WHEN pp_wow_run_rate_forecast BETWEEN -0.05
            AND -0.02
                THEN 'a slight decline of '
            WHEN pp_wow_run_rate_forecast BETWEEN -0.02
                AND -0.001
                THEN 'a small loss of '
            WHEN pp_wow_run_rate_forecast BETWEEN -0.001
                AND 0 THEN 'no change of '
            ELSE ''
        END wow_perc_run_rate_forecast_comments,
            pp_wow_seasonal_forecast,
            pp_wow_sales_forecast,
            pp_wow_straightline_forecast,
            pp_wow_run_rate_forecast,
            asofdate ds,
            asofdate
        FROM comments_stg_3_adv
    )
SELECT
        level_type,
        level,
        'Sales Fcst for ' || level || ' is currently at '
            || CAST(ROUND(100 * perc_sales_forecast, 1) AS VARCHAR) || '%. This represents ' || wow_perc_sales_forecast_comments
            || CAST(ROUND(100 * pp_wow_sales_forecast, 1) AS VARCHAR)
            || ' pp for Sales Fcst WoW. '
            AS level_comments_sales_forecast,
        'Seasonal Fcst for ' || level || ' is currently at '
            || CAST(ROUND(100 * perc_seasonal_forecast, 1) AS VARCHAR)
            || '% respectively. This represents '
            || WOW_PERC_SEASONAL_FORECAST_comments
            || CAST(ROUND(100 * pp_wow_seasonal_forecast, 1) AS VARCHAR)
            || ' pp Seasonal Fcst WoW. ' as level_comments_seasonal_forecast,

     'Run Rate Fcst for ' || level || ' is currently at '
            || CAST(ROUND(100 * perc_run_rate_forecast, 1) AS VARCHAR) || '%. This represents '
            || wow_perc_run_rate_forecast_comments
            || CAST(ROUND(100 * pp_wow_run_rate_forecast, 1) AS VARCHAR)
            || ' pp for Run Rate Fcst WoW. ' as level_comments_run_rate_forecast,
            'Straightline Fcst for ' || level || ' is currently at '
            || CAST(ROUND(100 * perc_run_rate_forecast, 1) AS VARCHAR) || '%. This represents '
            || CAST(ROUND(100 * pp_wow_straightline_forecast, 1) AS VARCHAR)
            || ' pp Straightline Fcst WoW. ' as level_comments_straightline_forecast,
            'The Daily Run Rate is currently at '
            || CAST(ROUND((DAILY_RUN_RATE / 1000000), 2) AS VARCHAR) || 'M ' || (
            CASE
                WHEN wow_perc_daily_run_rate > 0.001 THEN (
                    '(up by ' || CAST(
                        ROUND(100 * wow_perc_daily_run_rate, 1) AS VARCHAR
                    ) || '% WoW).'
                )
                WHEN wow_perc_daily_run_rate < 0.001 THEN (
                    '(down by ' || CAST(
                        ROUND(100 * wow_perc_daily_run_rate, 1) AS VARCHAR
                    ) || '% WoW).'
                )
                ELSE '(No real change from last Week.)'
            END ) || Case when required_run_rate > daily_run_rate then ' Running ' || CAST(ROUND((-(daily_run_rate - required_run_rate) / 1000000), 2) AS VARCHAR) ||'M over RR.' else  ' Required Run Rate is '
|| CAST(ROUND((required_run_rate / 1000000), 2) AS VARCHAR) || 'M.' end as required_run_rate,
        ds
    FROM comments_stg_4_adv
    """,
)

gms_dynamic_comments_stg_1_agency = PrestoInsertOperatorWithSchema(
    dep_list=[wait_for_bpo_quota_and_forecast_fast],
    table="<TABLE:gms_dynamic_comments_stg_1>",
    partition="ds=<DATEID>/route_to_market=<Agency>",
    documentation={
        "description": """Creates Dynamic Commentary for Overall Stats of
          L2-L12 Territories for Sales Forecast, Seasonal Forecast, Run Rate
          Forecast & Straightline Forecast."""
    },
    create=Table(
        cols=[
            Column("level_type", "VARCHAR", ""),
            Column("level", "VARCHAR", ""),
            Column("level_comments_sales_forecast", "VARCHAR", ""),
            Column("level_comments_seasonal_forecast", "VARCHAR", ""),
            Column("level_comments_run_rate_forecast", "VARCHAR", ""),
            Column("level_comments_straightline_forecast", "VARCHAR", ""),
            Column("required_run_rate", "VARCHAR", ""),
        ],
        partitions=[
            Column("ds", "VARCHAR", ""),
            Column("route_to_market", "VARCHAR", ""),
        ],
        comment="",
        initial_retention=1,
    ),
    select=r"""
    WITH comments_stg_1_adv AS (
        SELECT
            c2.level_type,
            c2.level,
            asofdate,
            SUM(lyq_revenue) lyq_revenue,
            SUM(lyq_revenue_qtd) lyq_revenue_qtd,
            SUM(pq_revenue) pq_revenue,
            SUM(pq_revenue_qtd) pq_revenue_qtd,
            SUM(agency_quota) quota,
            SUM(l7d_avg_revenue) daily_run_rate,
            SUM(l7d_avg_revenue_prior) daily_run_rate_prior,
            SUM(((cq_revenue) / (days_closed_in_quarter))
                * (days_total_in_quarter)
            ) sales_forecast,
            SUM(((cq_revenue_qtd_prior) / (days_closed_in_quarter - 7))
                * (days_total_in_quarter)
            ) sales_forecast_prior,
            SUM(run_rate_forecast) run_rate_forecast,
            SUM(run_rate_forecast_prior) run_rate_forecast_prior,
            SUM(cq_revenue) cq_revenue,
            SUM(cq_revenue_qtd_prior) cq_revenue_qtd_prior,
            SUM(lyq_revenue_qtd) / SUM(lyq_revenue) Ly_delivery,
            SUM(lyq_revenue_qtd_prior) / SUM(lyq_revenue) Ly_delivery_prior,
            SUM(advertiser_quota) * (SUM(lyq_revenue_qtd) / SUM(lyq_revenue)) quota_qtd,
            SUM(advertiser_quota) * (
                SUM(lyq_revenue_qtd_prior) / SUM(lyq_revenue)
            ) quota_qtd_prior,
            (SUM(cq_revenue) / MIN(days_closed_in_quarter)) * MIN(
                days_total_in_quarter
            ) straightline_forecast,
            (SUM(cq_revenue_qtd_prior) / MIN(days_closed_in_quarter - 7))
                * MIN(days_total_in_quarter) straightline_forecast_prior,
            (SUM(advertiser_quota) - SUM(cq_revenue)) / DATE_DIFF(
                'day',
                CAST(<QUARTERID> AS date) - INTERVAL '1' DAY,
                CAST(asofdate AS date)
            ) AS required_run_rate
        FROM bpo_gms_quota_and_forecast_fast
        CROSS JOIN UNNEST(
                ARRAY[
                    'Level 2 Agency',
                    'Level 4 Agency',
                    'Level 6 Agency',
                    'Level 8 Agency',
                    'Level 10 Agency',
                    'Level 12 Agency'
                ],
                ARRAY[
                    l2_agency_territory,
                    l4_agency_territory,
                    l6_agency_territory,
                    l8_agency_territory,
                    l10_agency_territory,
                    l12_agency_territory
                ]
            ) c2 (level_type, level)
        WHERE
            ds = '<DATEID>'
            AND l2_agency_territory = 'GBG Agency'
            and revenue_segment not in ('GPA Managed','GPA Unmanaged')
        GROUP BY
            1, 2, 3
    ),
    comments_stg_2_adv AS (
        SELECT
            level,
            level_type,
            quota,
            cq_revenue,
            pq_revenue,
            lyq_revenue,
            (cq_revenue / quota) attainment_qtd,
            (cq_revenue / lyq_revenue_qtd) - 1 yoy_perc_revenue,
            (cq_revenue / pq_revenue_qtd) - 1 qoq_perc_revenue,
            (cq_revenue / quota) - (cq_revenue_qtd_prior / quota) wow_perc_quota,
            (COALESCE(sales_forecast,straightline_forecast)) sales_forecast,
            (COALESCE(sales_forecast_prior,straightline_forecast_prior)) sales_forecast_prior,
            (COALESCE(sales_forecast,straightline_forecast)) / quota perc_sales_forecast,
            (COALESCE(sales_forecast_prior,straightline_forecast_prior)) / quota perc_sales_forecast_prior,
            ((COALESCE(sales_forecast,straightline_forecast)) / lyq_revenue) - 1 yoy_perc_sales_forecast,
            ((COALESCE(sales_forecast,straightline_forecast)) / lyq_revenue) - 1 qoq_perc_sales_forecast,
            ((COALESCE(sales_forecast,straightline_forecast)) / (COALESCE(sales_forecast_prior,straightline_forecast_prior))) - 1 wow_perc_sales_forecast,
            (cq_revenue / quota_qtd) * quota seasonal_forecast,
            (cq_revenue_qtd_prior / quota_qtd_prior) * quota seasonal_forecast_prior,
            ((cq_revenue / quota_qtd) * quota) / quota perc_seasonal_forecast,
            ((cq_revenue_qtd_prior / quota_qtd_prior) * quota) / quota perc_seasonal_forecast_prior,
            (((cq_revenue / quota_qtd) * quota) / lyq_revenue) - 1 qoq_perc_seasonal_forecast,
            (((cq_revenue / quota_qtd) * quota) / lyq_revenue) - 1 yoy_perc_seasonal_forecast,
            (
                ((cq_revenue / quota_qtd) * quota) / (
                    (cq_revenue_qtd_prior / quota_qtd_prior) * quota
                )
            ) - 1 wow_perc_seasonal_forecast,
            run_rate_forecast,
            run_rate_forecast / quota perc_run_rate_forecast,
            run_rate_forecast_prior / quota perc_run_rate_forecast_prior,
            (run_rate_forecast_prior / quota) - (run_rate_forecast / quota) wow_pp_run_rate_forecast,
            (run_rate_forecast / run_rate_forecast_prior) - 1 wow_perc_run_rate_forecast,
            daily_run_rate,
            daily_run_rate_prior,
            required_run_rate,
            (daily_run_rate / daily_run_rate_prior) - 1 wow_perc_daily_run_rate,
            straightline_forecast,
            straightline_forecast_prior,
            straightline_forecast / quota perc_straightline_forecast,
            straightline_forecast_prior / quota perc_straightline_forecast_prior,
            (straightline_forecast / lyq_revenue) - 1 yoy_perc_straightline_forecast,
            (straightline_forecast / lyq_revenue) - 1 qoq_perc_straightline_forecast,
            (straightline_forecast / straightline_forecast_prior) - 1 wow_perc_straightline_forecast,
            asofdate ds,
            asofdate
        FROM comments_stg_1_adv
    ),
comments_stg_3_adv AS (
    SELECT
        level,
        level_type,
        quota,
        cq_revenue,
        pq_revenue,
        lyq_revenue,
        attainment_qtd,
        yoy_perc_revenue,
        qoq_perc_revenue,
        wow_perc_quota,
        sales_forecast,
        sales_forecast_prior,
        perc_sales_forecast,
        perc_sales_forecast_prior,
        yoy_perc_sales_forecast,
        qoq_perc_sales_forecast,
        wow_perc_sales_forecast,
        seasonal_forecast,
        seasonal_forecast_prior,
        perc_seasonal_forecast,
        perc_seasonal_forecast_prior,
        qoq_perc_seasonal_forecast,
        yoy_perc_seasonal_forecast,
        wow_perc_seasonal_forecast,
        run_rate_forecast,
        perc_run_rate_forecast,
        perc_run_rate_forecast_prior,
        wow_pp_run_rate_forecast,
        wow_perc_run_rate_forecast,
        daily_run_rate,
        daily_run_rate_prior,
        required_run_rate,
        wow_perc_daily_run_rate,
        straightline_forecast,
        straightline_forecast_prior,
        perc_straightline_forecast,
        perc_straightline_forecast_prior,
        yoy_perc_straightline_forecast,
        qoq_perc_straightline_forecast,
        wow_perc_straightline_forecast,
        (perc_seasonal_forecast - perc_seasonal_forecast_prior) pp_wow_seasonal_forecast,
        (perc_sales_forecast - perc_sales_forecast_prior) pp_wow_sales_forecast,
        (perc_straightline_forecast - perc_straightline_forecast_prior) pp_wow_straightline_forecast,
        (perc_run_rate_forecast - perc_run_rate_forecast_prior) pp_wow_run_rate_forecast,
        asofdate ds,
        asofdate
    FROM comments_stg_2_adv
),
    comments_stg_4_adv AS (
        SELECT
            level,
            level_type,
            quota,
            cq_revenue,
            pq_revenue,
            lyq_revenue,
            attainment_qtd,
            yoy_perc_revenue,
            qoq_perc_revenue,
            wow_perc_quota,
            sales_forecast,
            sales_forecast_prior,
            perc_sales_forecast,
            perc_sales_forecast_prior,
            yoy_perc_sales_forecast,
            qoq_perc_sales_forecast,
            wow_perc_sales_forecast,
            seasonal_forecast,
            seasonal_forecast_prior,
            perc_seasonal_forecast,
            perc_seasonal_forecast_prior,
            qoq_perc_seasonal_forecast,
            yoy_perc_seasonal_forecast,
            wow_perc_seasonal_forecast,
            run_rate_forecast,
            perc_run_rate_forecast,
            perc_run_rate_forecast_prior,
            wow_pp_run_rate_forecast,
            wow_perc_run_rate_forecast,
            daily_run_rate,
            daily_run_rate_prior,
            required_run_rate,
            wow_perc_daily_run_rate,
            straightline_forecast,
            straightline_forecast_prior,
            perc_straightline_forecast,
            perc_straightline_forecast_prior,
            yoy_perc_straightline_forecast,
            qoq_perc_straightline_forecast,
            wow_perc_straightline_forecast,
        CASE
            WHEN pp_wow_sales_forecast > 0.05
                 THEN 'a strong increase of '
            WHEN pp_wow_sales_forecast BETWEEN 0.02
                AND 0.05 THEN 'good growth of '
            WHEN pp_wow_sales_forecast BETWEEN 0.001
                AND 0.02 THEN 'slight growth of '
            WHEN pp_wow_sales_forecast BETWEEN 0
                 AND 0.001 THEN 'no change of '
            WHEN pp_wow_sales_forecast < -0.05
                THEN 'a strong decrease of '
            WHEN pp_wow_sales_forecast BETWEEN -0.05
            AND -0.02    THEN 'a slight decline of '
            WHEN pp_wow_sales_forecast BETWEEN -0.02
                AND -0.001 THEN 'a small loss of '
            WHEN pp_wow_sales_forecast BETWEEN -0.001
                AND 0 THEN 'no change of '
            ELSE ''
        END wow_perc_sales_forecast_comments,
        CASE
            WHEN pp_wow_seasonal_forecast > 0.05
                THEN 'a significant increase of '
            WHEN pp_wow_seasonal_forecast BETWEEN 0.02
                AND 0.05
                THEN 'a great upward move of '
            WHEN pp_wow_seasonal_forecast BETWEEN 0.001
                AND 0.02
                THEN 'slight increase of '
            WHEN pp_wow_seasonal_forecast BETWEEN 0
                 AND 0.001
                THEN 'a stagnant '
            WHEN pp_wow_seasonal_forecast < -0.05
                THEN 'a decrease of '
            WHEN pp_wow_seasonal_forecast BETWEEN -0.05
            AND -0.02 THEN 'a drop of '
            WHEN pp_wow_seasonal_forecast BETWEEN -0.02
                AND -0.001 THEN 'a small drop of '
            WHEN pp_wow_seasonal_forecast BETWEEN -0.001
                AND 0 THEN 'a stagnant '
            ELSE ''
        END wow_perc_seasonal_forecast_comments,
        CASE
            WHEN pp_wow_straightline_forecast > 0.05
                THEN 'a significant increase of '
            WHEN pp_wow_straightline_forecast BETWEEN 0.02
                AND 0.05
                THEN 'a great upward move of '
            WHEN pp_wow_straightline_forecast BETWEEN 0.001
                AND 0.02
                THEN 'slight increase of '
            WHEN pp_wow_straightline_forecast BETWEEN 0
                 AND 0.001
                THEN 'a stagnant '
            WHEN pp_wow_straightline_forecast < -0.05
                THEN 'a decrease of '
            WHEN pp_wow_straightline_forecast BETWEEN -0.05
            AND -0.02
                THEN 'a drop of '
            WHEN pp_wow_straightline_forecast BETWEEN -0.02
                AND -0.001
                THEN 'a small drop of '
            WHEN pp_wow_straightline_forecast BETWEEN -0.001
                AND 0 THEN 'a stagnant '
            ELSE ''
        END wow_perc_straightline_forecast_comments,
        CASE
            WHEN pp_wow_run_rate_forecast > 0.05
                THEN 'a strong increase of '
            WHEN pp_wow_run_rate_forecast BETWEEN 0.02
                AND 0.05
                THEN 'good growth of '
            WHEN pp_wow_run_rate_forecast BETWEEN 0.001
                AND 0.02
                THEN 'slight growth of '
            WHEN pp_wow_run_rate_forecast BETWEEN 0
                 AND 0.001
                THEN 'no change of '
            WHEN pp_wow_run_rate_forecast < -0.05
                THEN 'a strong decrease of '
            WHEN pp_wow_run_rate_forecast BETWEEN -0.05
            AND -0.02
                THEN 'a slight decline of '
            WHEN pp_wow_run_rate_forecast BETWEEN -0.02
                AND -0.001
                THEN 'a small loss of '
            WHEN pp_wow_run_rate_forecast BETWEEN -0.001
                AND 0 THEN 'no change of '
            ELSE ''
        END wow_perc_run_rate_forecast_comments,
            pp_wow_seasonal_forecast,
            pp_wow_sales_forecast,
            pp_wow_straightline_forecast,
            pp_wow_run_rate_forecast,
            asofdate ds,
            asofdate
        FROM comments_stg_3_adv
    )
SELECT
        level_type,
        level,
        'Sales Fcst for ' || level || ' is currently at '
            || CAST(ROUND(100 * perc_sales_forecast, 1) AS VARCHAR) || '%. This represents ' || wow_perc_sales_forecast_comments
            || CAST(ROUND(100 * pp_wow_sales_forecast, 1) AS VARCHAR)
            || ' pp for Sales Fcst WoW. '
            AS level_comments_sales_forecast,
        'Seasonal Fcst for ' || level || ' is currently at '
            || CAST(ROUND(100 * perc_seasonal_forecast, 1) AS VARCHAR)
            || '% respectively. This represents '
            || WOW_PERC_SEASONAL_FORECAST_comments
            || CAST(ROUND(100 * pp_wow_seasonal_forecast, 1) AS VARCHAR)
            || ' pp Seasonal Fcst WoW. ' as level_comments_seasonal_forecast,

     'Run Rate Fcst for ' || level || ' is currently at '
            || CAST(ROUND(100 * perc_run_rate_forecast, 1) AS VARCHAR) || '%. This represents '
            || wow_perc_run_rate_forecast_comments
            || CAST(ROUND(100 * pp_wow_run_rate_forecast, 1) AS VARCHAR)
            || ' pp for Run Rate Fcst WoW. ' as level_comments_run_rate_forecast,
            'Straightline Fcst for ' || level || ' is currently at '
            || CAST(ROUND(100 * perc_run_rate_forecast, 1) AS VARCHAR) || '%. This represents '
            || wow_perc_straightline_forecast_comments
            || CAST(ROUND(100 * pp_wow_straightline_forecast, 1) AS VARCHAR)
            || ' pp Straightline Fcst WoW. ' as level_comments_straightline_forecast,
            'The Daily Run Rate is currently at '
            || CAST(ROUND((DAILY_RUN_RATE / 1000000), 2) AS VARCHAR) || 'M ' || (
            CASE
                WHEN wow_perc_daily_run_rate > 0.001 THEN (
                    '(up by ' || CAST(
                        ROUND(100 * wow_perc_daily_run_rate, 1) AS VARCHAR
                    ) || '% WoW).'
                )
                WHEN wow_perc_daily_run_rate < 0.001 THEN (
                    '(down by ' || CAST(
                        ROUND(100 * wow_perc_daily_run_rate, 1) AS VARCHAR
                    ) || '% WoW).'
                )
                ELSE '(No real change from last Week.)'
            END ) || Case when required_run_rate > daily_run_rate then ' Running ' || CAST(ROUND((-(daily_run_rate - required_run_rate) / 1000000), 2) AS VARCHAR) ||'M over RR.' else  ' Required Run Rate is '
|| CAST(ROUND((required_run_rate / 1000000), 2) AS VARCHAR) || 'M.' end as required_run_rate,
        ds
    FROM comments_stg_4_adv
    """,
)

gms_dynamic_comments_stg_2_advertiser = PrestoInsertOperatorWithSchema(
    dep_list=[wait_for_bpo_quota_and_forecast_fast],
    table="<TABLE:gms_dynamic_comments_stg_2>",
    partition="ds=<DATEID>/route_to_market=<Advertiser>",
    documentation={
        "description": """Creates Dynamic Commentary for Overall Stats of
              L2-L12 Territories for Sales Forecast, Seasonal Forecast, Run Rate
              Forecast & Straightline Forecast."""
    },
    create=Table(
        cols=[
            Column("level_type", "VARCHAR", ""),
            Column("level", "VARCHAR", ""),
            Column("level_comments_seasonal", "VARCHAR", ""),
            Column("level_comments_sales", "VARCHAR", ""),
            Column("level_comments_straightline", "VARCHAR", ""),
            Column("level_comments_run_rate", "VARCHAR", ""),
        ],
        partitions=[
            Column("ds", "VARCHAR", ""),
            Column("route_to_market", "VARCHAR", ""),
        ],
        comment="",
        initial_retention=1,
    ),
    select=r"""
    WITH comments_stg_1_adv AS (
        SELECT
            c2.level_type,
            c2.level,
            CASE
                WHEN revenue_segment = 'GBG In-Market' THEN ultimate_parent_name
                WHEN revenue_segment = 'GBG Scaled' then advertiser_name
                ELSE market
            END AS name,
            asofdate,
            SUM(lyq_revenue) lyq_revenue,
            SUM(lyq_revenue_qtd) lyq_revenue_qtd,
            SUM(pq_revenue) pq_revenue,
            SUM(pq_revenue_qtd) pq_revenue_qtd,
            SUM(advertiser_quota) quota,
            SUM(l7d_avg_revenue) daily_run_rate,
            SUM(l7d_avg_revenue_prior) daily_run_rate_prior,
            SUM(
                CASE
                    WHEN revenue_segment = 'GBG In-Market' THEN sales_forecast
                    ELSE (cq_revenue) / (days_closed_in_quarter)
                        * (days_total_in_quarter)
                END
            ) sales_forecast,
            SUM(
                CASE
                    WHEN revenue_segment = 'GBG In-Market' THEN sales_forecast_prior
                    ELSE ((cq_revenue_qtd_prior) / (days_closed_in_quarter - 7))
                        * (days_total_in_quarter)
                END
            ) sales_forecast_prior,
            SUM(run_rate_forecast) run_rate_forecast,
            SUM(run_rate_forecast_prior) run_rate_forecast_prior,
            SUM(cq_revenue) cq_revenue,
            SUM(cq_revenue_qtd_prior) cq_revenue_qtd_prior,
            SUM(lyq_revenue_qtd) / SUM(lyq_revenue) Ly_delivery,
            SUM(lyq_revenue_qtd_prior) / SUM(lyq_revenue) Ly_delivery_prior,
            SUM(advertiser_quota) * SUM(lyq_revenue_qtd) / SUM(lyq_revenue) quota_qtd,
            SUM(advertiser_quota) * SUM(lyq_revenue_qtd_prior) / SUM(lyq_revenue) quota_qtd_prior,
            (SUM(cq_revenue) / MIN(days_closed_in_quarter)) * MIN(
                days_total_in_quarter
            ) straightline_forecast,
            (SUM(cq_revenue_qtd_prior) / MIN(days_closed_in_quarter - 7))
                * MIN(days_total_in_quarter) straightline_forecast_prior,
            (SUM(advertiser_quota) - SUM(cq_revenue)) / DATE_DIFF(
                'day',
                CAST(asofdate AS date),
                CAST(<QUARTERID> AS date) - INTERVAL '1' DAY
            ) AS required_run_rate
        FROM bpo_gms_quota_and_forecast_fast
        CROSS JOIN UNNEST(
                ARRAY[
                    'Level 2 Advertiser',
                    'Level 4 Advertiser',
                    'Level 6 Advertiser',
                    'Level 8 Advertiser',
                    'Level 10 Advertiser',
                    'Level 12 Advertiser'
                ],
                ARRAY[
                    l2_reporting_territory,
                    l4_reporting_territory,
                    l6_reporting_territory,
                    l8_reporting_territory,
                    l10_reporting_territory,
                    l12_reporting_territory
                ]
            ) c2 (level_type, level)
        WHERE
            ds = '<DATEID>'
            AND l2_reporting_territory NOT IN ('Reseller', 'GBG Agency')
            and revenue_segment not in ('GPA Managed','GPA Unmanaged')
        GROUP BY
            1, 2, 3, 4
    ),
    comments_stg_2_adv AS (
        SELECT
            level,
            level_type,
            name,
            quota,
            quota_qtd,
            quota_qtd_prior,
            cq_revenue,
            pq_revenue,
            lyq_revenue,
            (cq_revenue / quota) attainment_qtd,
            (cq_revenue / lyq_revenue_qtd) - 1 yoy_perc_revenue,
            (cq_revenue / pq_revenue_qtd) - 1 qoq_perc_revenue,
            (cq_revenue / quota) - (cq_revenue_qtd_prior / quota) wow_perc_quota,
            sales_forecast,
            sales_forecast_prior,
            sales_forecast / quota perc_sales_forecast,
            sales_forecast_prior / quota perc_sales_forecast_prior,
            (sales_forecast / lyq_revenue) - 1 yoy_perc_sales_forecast,
            (sales_forecast / lyq_revenue) - 1 qoq_perc_sales_forecast,
            (sales_forecast / sales_forecast_prior) - 1 wow_perc_sales_forecast,
            (cq_revenue / quota_qtd) * quota seasonal_forecast,
            (cq_revenue_qtd_prior / quota_qtd_prior) * quota seasonal_forecast_prior,
            ((cq_revenue / quota_qtd) * quota) / quota perc_seasonal_forecast,
            ((cq_revenue_qtd_prior / quota_qtd_prior) * quota) / quota perc_seasonal_forecast_prior,
            (((cq_revenue / quota_qtd) * quota) / lyq_revenue) - 1 qoq_perc_seasonal_forecast,
            (((cq_revenue / quota_qtd) * quota) / lyq_revenue) - 1 yoy_perc_seasonal_forecast,
            (((cq_revenue / quota_qtd) * quota) / (
            (cq_revenue_qtd_prior / quota_qtd_prior) * quota)) - 1 wow_perc_seasonal_forecast,
            run_rate_forecast,
            run_rate_forecast_prior,
            run_rate_forecast / quota perc_run_rate_forecast,
            run_rate_forecast_prior / quota perc_run_rate_forecast_prior,
            (run_rate_forecast_prior / quota) - (run_rate_forecast / quota) wow_pp_run_rate_forecast,
            (run_rate_forecast / run_rate_forecast_prior) - 1 wow_perc_run_rate_forecast,
            daily_run_rate,
            daily_run_rate_prior,
            required_run_rate,
            (daily_run_rate / daily_run_rate_prior) - 1 wow_perc_daily_run_rate,
            straightline_forecast,
            straightline_forecast_prior,
            straightline_forecast / quota perc_straightline_forecast,
            straightline_forecast_prior / quota perc_straightline_forecast_prior,
            (straightline_forecast / lyq_revenue) - 1 yoy_perc_straightline_forecast,
            (straightline_forecast / lyq_revenue) - 1 qoq_perc_straightline_forecast,
            (straightline_forecast / straightline_forecast_prior) - 1 wow_perc_straightline_forecast,
            asofdate ds,
            asofdate
        FROM comments_stg_1_adv
    ),
    comments_stg_3_adv AS (
        SELECT
            level,
            level_type,
            name,
            quota,
            quota_qtd,
            quota_qtd_prior,
            cq_revenue,
            pq_revenue,
            lyq_revenue,
            attainment_qtd,
            yoy_perc_revenue,
            qoq_perc_revenue,
            wow_perc_quota,
            sales_forecast,
            sales_forecast_prior,
            perc_sales_forecast,
            perc_sales_forecast_prior,
            yoy_perc_sales_forecast,
            qoq_perc_sales_forecast,
            wow_perc_sales_forecast,
            seasonal_forecast,
            seasonal_forecast_prior,
            perc_seasonal_forecast,
            perc_seasonal_forecast_prior,
            qoq_perc_seasonal_forecast,
            yoy_perc_seasonal_forecast,
            wow_perc_seasonal_forecast,
            run_rate_forecast,
            perc_run_rate_forecast,
            perc_run_rate_forecast_prior,
            wow_pp_run_rate_forecast,
            wow_perc_run_rate_forecast,
            daily_run_rate,
            daily_run_rate_prior,
            required_run_rate,
            wow_perc_daily_run_rate,
            straightline_forecast,
            straightline_forecast_prior,
            perc_straightline_forecast,
            perc_straightline_forecast_prior,
            yoy_perc_straightline_forecast,
            qoq_perc_straightline_forecast,
            wow_perc_straightline_forecast,
            CASE
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) > 0.05
                     THEN 'a strong increase of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN 0.02
                    AND 0.05 THEN 'good growth of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN 0.001
                    AND 0.02 THEN 'slight growth of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN 0
                     AND 0.001 THEN 'no change of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) < -0.05
                    THEN 'a strong decrease of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN -0.02
                AND -0.02    THEN 'a slight decline of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN -0.001
                    AND -0.02 THEN 'a small loss of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN 0
                    AND -0.001 THEN 'no change of '
                ELSE ''
            END wow_perc_sales_forecast_comments,
            CASE
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) > 0.05
                    THEN 'a significant increase of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN 0.02
                    AND 0.05
                    THEN 'a great upward move of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN 0.001
                    AND 0.02
                    THEN 'slight increase of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN 0
                     AND 0.001
                    THEN 'a stagnant '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) < -0.05
                    THEN 'a decrease of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN -0.02
                AND -0.02
                    THEN 'a drop of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN -0.001
                    AND -0.02
                    THEN 'a small drop of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN 0
                    AND -0.001
                    THEN 'a stagnant '
                ELSE ''
            END wow_perc_seasonal_forecast_comments,
            CASE
                WHEN (perc_straightline_forecast - perc_seasonal_forecast_prior) > 0.05
                    THEN 'a significant increase of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN 0.02
                    AND 0.05
                    THEN 'a great upward move of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN 0.001
                    AND 0.02
                    THEN 'slight increase of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN 0
                     AND 0.001
                    THEN 'a stagnant '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) < -0.05
                    THEN 'a decrease of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN -0.02
                AND -0.02
                    THEN 'a drop of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN -0.001
                    AND -0.02
                    THEN 'a small drop of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN 0
                    AND -0.001
                    THEN 'a stagnant '
                ELSE ''
            END wow_perc_straightline_forecast_comments,
            CASE
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) > 0.05
                    THEN 'a strong increase of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN 0.02
                    AND 0.05
                    THEN 'good growth of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN 0.001
                    AND 0.02
                    THEN 'slight growth of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN 0
                     AND 0.001
                    THEN 'no change of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) < -0.05
                    THEN 'a strong decrease of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN -0.02
                AND -0.02
                    THEN 'a slight decline of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN -0.001
                    AND -0.02
                    THEN 'a small loss of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN 0
                    AND -0.001
                    THEN 'no change of '
                ELSE ''
            END wow_perc_run_rate_forecast_comments,
            (perc_seasonal_forecast - perc_seasonal_forecast_prior) pp_wow_seasonal_forecast,
            (perc_sales_forecast - perc_sales_forecast_prior) pp_wow_sales_forecast,
            (perc_straightline_forecast - perc_straightline_forecast_prior) pp_wow_straightline_forecast,
            (perc_run_rate_forecast - perc_run_rate_forecast_prior) pp_wow_run_rate_forecast,
            CASE
                WHEN is_nan(seasonal_forecast - seasonal_forecast_prior)
                    OR is_infinite(seasonal_forecast - seasonal_forecast_prior)
                    THEN NULL
                ELSE (seasonal_forecast - seasonal_forecast_prior)
            END seasonal_wow_delta,
            CASE
                WHEN is_nan(sales_forecast - sales_forecast_prior)
                    OR is_infinite(sales_forecast - sales_forecast_prior) THEN NULL
                ELSE (sales_forecast - sales_forecast_prior)
            END sales_wow_delta,
            CASE
                WHEN is_nan(straightline_forecast - straightline_forecast_prior)
                    OR is_infinite(
                    straightline_forecast - straightline_forecast_prior
                ) THEN NULL
                ELSE (straightline_forecast - straightline_forecast_prior)
            END straightline_wow_delta,
            CASE
                WHEN is_nan(run_rate_forecast - run_rate_forecast_prior)
                    OR is_infinite(run_rate_forecast - run_rate_forecast_prior)
                    THEN NULL
                ELSE (run_rate_forecast - run_rate_forecast_prior)
            END run_rate_wow_delta,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(seasonal_forecast - seasonal_forecast_prior)
                            OR is_infinite(
                            seasonal_forecast - seasonal_forecast_prior
                        ) THEN NULL
                        ELSE (seasonal_forecast - seasonal_forecast_prior)
                    END DESC
            ) highest_rank_seasonal,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(seasonal_forecast - seasonal_forecast_prior)
                            OR is_infinite(
                            seasonal_forecast - seasonal_forecast_prior
                        ) THEN NULL
                        ELSE (seasonal_forecast - seasonal_forecast_prior)
                    END ASC
            ) lowest_rank_seasonal,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(sales_forecast - sales_forecast_prior)
                            OR is_infinite(sales_forecast - sales_forecast_prior)
                            THEN NULL
                        ELSE (sales_forecast - sales_forecast_prior)
                    END DESC
            ) highest_rank_sales,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(sales_forecast - sales_forecast_prior)
                            OR is_infinite(sales_forecast - sales_forecast_prior)
                            THEN NULL
                        ELSE (sales_forecast - sales_forecast_prior)
                    END ASC
            ) lowest_rank_sales,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(
                            straightline_forecast - straightline_forecast_prior
                        ) OR is_infinite(
                            straightline_forecast - straightline_forecast_prior
                        ) THEN NULL
                        ELSE (straightline_forecast - straightline_forecast_prior)
                    END DESC
            ) highest_rank_straightline,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(
                            straightline_forecast - straightline_forecast_prior
                        ) OR is_infinite(
                            straightline_forecast - straightline_forecast_prior
                        ) THEN NULL
                        ELSE (straightline_forecast - straightline_forecast_prior)
                    END ASC
            ) lowest_rank_straightline,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(run_rate_forecast - run_rate_forecast_prior)
                            OR is_infinite(
                            run_rate_forecast - run_rate_forecast_prior
                        ) THEN NULL
                        ELSE (run_rate_forecast - run_rate_forecast_prior)
                    END DESC
            ) highest_rank_run_rate,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(run_rate_forecast - run_rate_forecast_prior)
                            OR is_infinite(
                            run_rate_forecast - run_rate_forecast_prior
                        ) THEN NULL
                        ELSE (run_rate_forecast - run_rate_forecast_prior)
                    END ASC
            ) lowest_rank_run_rate,
            asofdate ds,
            asofdate
        FROM comments_stg_2_adv
    ),
    comments_stg_4_adv AS (
        SELECT
            level_type,
            level,
            MAX(ds) AS ds,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 1 AND seasonal_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr1_sf_name_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 1 AND seasonal_wow_delta >= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 1 AND seasonal_wow_delta >= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 2 AND seasonal_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr2_sf_name_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 2 AND seasonal_wow_delta >= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 2 AND seasonal_wow_delta >= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 3 AND seasonal_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr3_sf_name_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 3 AND seasonal_wow_delta >= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 3 AND seasonal_wow_delta >= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 1 AND seasonal_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_name_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 1 AND seasonal_wow_delta <= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 1 AND seasonal_wow_delta <= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 2 AND seasonal_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_name_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 2 AND seasonal_wow_delta <= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 2 AND seasonal_wow_delta <= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 3 AND seasonal_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_name_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 3 AND seasonal_wow_delta <= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 3 AND seasonal_wow_delta <= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_sales = 1 AND sales_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr1_sf_name_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 1 AND sales_wow_delta >= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 1 AND sales_wow_delta >= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 2 AND sales_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr2_sf_name_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 2 AND sales_wow_delta >= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 2 AND sales_wow_delta >= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 3 AND sales_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr3_sf_name_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 3 AND sales_wow_delta >= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 3 AND sales_wow_delta >= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 1 AND sales_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_name_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 1 AND sales_wow_delta <= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 1 AND sales_wow_delta <= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 2 AND sales_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_name_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 2 AND sales_wow_delta <= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 2 AND sales_wow_delta <= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 3 AND sales_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_name_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 3 AND sales_wow_delta <= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 3 AND sales_wow_delta <= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 1 AND straightline_wow_delta
                        >= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr1_sf_name_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 1 AND straightline_wow_delta
                        >= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 1 AND straightline_wow_delta
                        >= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 2 AND straightline_wow_delta
                        >= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr2_sf_name_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 2 AND straightline_wow_delta
                        >= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 2 AND straightline_wow_delta
                        >= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 3 AND straightline_wow_delta
                        >= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr3_sf_name_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 3 AND straightline_wow_delta
                        >= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 3 AND straightline_wow_delta
                        >= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 1 AND straightline_wow_delta
                        <= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_name_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 1 AND straightline_wow_delta
                        <= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 1 AND straightline_wow_delta
                        <= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 2 AND straightline_wow_delta
                        <= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_name_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 2 AND straightline_wow_delta
                        <= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 2 AND straightline_wow_delta
                        <= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 3 AND straightline_wow_delta
                        <= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_name_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 3 AND straightline_wow_delta
                        <= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 3 AND straightline_wow_delta
                        <= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 1 AND run_rate_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr1_sf_name_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 1 AND run_rate_wow_delta >= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 1 AND run_rate_wow_delta >= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 2 AND run_rate_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr2_sf_name_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 2 AND run_rate_wow_delta >= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 2 AND run_rate_wow_delta >= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 3 AND run_rate_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr3_sf_name_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 3 AND run_rate_wow_delta >= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 3 AND run_rate_wow_delta >= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 1 AND run_rate_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_name_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 1 AND run_rate_wow_delta <= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 1 AND run_rate_wow_delta <= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 2 AND run_rate_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_name_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 2 AND run_rate_wow_delta <= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 2 AND run_rate_wow_delta <= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 3 AND run_rate_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_name_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 3 AND run_rate_wow_delta <= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 3 AND run_rate_wow_delta <= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_wow_sf_perc_run_rate
        FROM comments_stg_3_adv
        GROUP BY
            1, 2
    )
    SELECT
        CASE
            WHEN highest_nr1_sf_name_seasonal IS NULL
                THEN 'No notable Gainers this Week.'
            ELSE 'Gains this week using Seasonal came from '
        END || COALESCE(highest_nr1_sf_name_seasonal, '') || CASE
            WHEN highest_nr1_sf_name_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_seasonal IS NULL THEN ''
            WHEN highest_nr2_sf_perc_seasonal IS NULL THEN 'pp WoW).'
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(highest_nr2_sf_name_seasonal, '') || CASE
            WHEN highest_nr2_sf_perc_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_seasonal IS NULL THEN ''
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(highest_nr3_sf_name_seasonal, '') || CASE
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE 'pp WoW). '
        END || CASE
            WHEN lowest_nr1_sf_name_seasonal IS NULL
                THEN 'No notable Changes for Losses this Week. '
            ELSE 'Looking at Seasonal the Decline came from '
        END || COALESCE(lowest_nr1_sf_name_seasonal, '') || CASE
            WHEN lowest_nr1_sf_name_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_name_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_perc_seasonal IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN 'pp WoW).'
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(lowest_nr2_sf_name_seasonal, '') || CASE
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN ''
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(lowest_nr3_sf_name_seasonal, '') || CASE
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN ''
            WHEN lowest_nr1_sf_perc_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE 'pp WoW). '
        END AS level_comments_seasonal,
        CASE
            WHEN highest_nr1_sf_name_sales IS NULL THEN 'No notable Gainers this Week.'
            ELSE 'We have seen gains using Sales Fcst from '
        END || COALESCE(highest_nr1_sf_name_sales, '') || CASE
            WHEN highest_nr1_sf_name_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_sales IS NULL THEN ''
            WHEN highest_nr2_sf_perc_sales IS NULL THEN 'pp WoW).'
            WHEN highest_nr3_sf_perc_sales IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(highest_nr2_sf_name_sales, '') || CASE
            WHEN highest_nr2_sf_perc_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_sales IS NULL THEN ''
            WHEN highest_nr3_sf_perc_sales IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(highest_nr3_sf_name_sales, '') || CASE
            WHEN highest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE 'pp WoW). '
        END || CASE
            WHEN lowest_nr1_sf_name_sales IS NULL
                THEN 'No notable Changes for Losses this Week. '
            ELSE 'Decline using Sales this week came from '
        END || COALESCE(lowest_nr1_sf_name_sales, '') || CASE
            WHEN lowest_nr1_sf_name_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_name_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_perc_sales IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN 'pp WoW).'
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(lowest_nr2_sf_name_sales, '') || CASE
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN ''
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(lowest_nr3_sf_name_sales, '') || CASE
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN ''
            WHEN lowest_nr1_sf_perc_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE 'pp WoW). '
        END AS level_comments_sales,
        CASE
            WHEN highest_nr1_sf_name_straightline IS NULL
                THEN 'No notable Gainers this Week.'
            ELSE 'We have seen gains using straightline from '
        END || COALESCE(highest_nr1_sf_name_straightline, '') || CASE
            WHEN highest_nr1_sf_name_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_straightline IS NULL THEN ''
            WHEN highest_nr2_sf_perc_straightline IS NULL THEN 'pp WoW).'
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(highest_nr2_sf_name_straightline, '') || CASE
            WHEN highest_nr2_sf_perc_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_straightline IS NULL THEN ''
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(highest_nr3_sf_name_straightline, '') || CASE
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE 'pp WoW). '
        END || CASE
            WHEN lowest_nr1_sf_name_straightline IS NULL
                THEN 'No notable Changes for Losses this Week. '
            ELSE 'Decline using straightline this week came from '
        END || COALESCE(lowest_nr1_sf_name_straightline, '') || CASE
            WHEN lowest_nr1_sf_name_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_name_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_perc_straightline IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN 'pp WoW).'
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(lowest_nr2_sf_name_straightline, '') || CASE
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN ''
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(lowest_nr3_sf_name_straightline, '') || CASE
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN ''
            WHEN lowest_nr1_sf_perc_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE 'pp WoW). '
        END AS level_comments_straightline,
        CASE
            WHEN highest_nr1_sf_name_run_rate IS NULL
                THEN 'No notable Gainers this Week when looking at Run Rate.'
            ELSE 'Looking at Run Rate we have seen an increase from '
        END || COALESCE(highest_nr1_sf_name_run_rate, '') || CASE
            WHEN highest_nr1_sf_name_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_run_rate IS NULL THEN ''
            WHEN highest_nr2_sf_perc_run_rate IS NULL THEN 'pp WoW).'
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(highest_nr2_sf_name_run_rate, '') || CASE
            WHEN highest_nr2_sf_perc_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_run_rate IS NULL THEN ''
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(highest_nr3_sf_name_run_rate, '') || CASE
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE 'pp WoW). '
        END || CASE
            WHEN lowest_nr1_sf_name_run_rate IS NULL
                THEN 'No notable Changes for Losses this Week. '
            ELSE 'Run Rate drop this week came from '
        END || COALESCE(lowest_nr1_sf_name_run_rate, '') || CASE
            WHEN lowest_nr1_sf_name_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_name_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_perc_run_rate IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN 'pp WoW).'
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(lowest_nr2_sf_name_run_rate, '') || CASE
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN ''
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(lowest_nr3_sf_name_run_rate, '') || CASE
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN ''
            WHEN lowest_nr1_sf_perc_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE 'pp WoW). '
        END AS level_comments_run_rate,
        level_type,
        level,
        DS
    FROM comments_stg_4_adv
    """,
)

gms_dynamic_comments_stg_2_agency = PrestoInsertOperatorWithSchema(
    dep_list=[wait_for_bpo_quota_and_forecast_fast],
    table="<TABLE:gms_dynamic_comments_stg_2>",
    partition="ds=<DATEID>/route_to_market=<Agency>",
    documentation={
        "description": """Creates Dynamic Commentary for Overall Stats of
              L2-L12 Territories for Sales Forecast, Seasonal Forecast, Run Rate
              Forecast & Straightline Forecast."""
    },
    create=Table(
        cols=[
            Column("level_type", "VARCHAR", ""),
            Column("level", "VARCHAR", ""),
            Column("level_comments_seasonal", "VARCHAR", ""),
            Column("level_comments_sales", "VARCHAR", ""),
            Column("level_comments_straightline", "VARCHAR", ""),
            Column("level_comments_run_rate", "VARCHAR", ""),
        ],
        partitions=[
            Column("ds", "VARCHAR", ""),
            Column("route_to_market", "VARCHAR", ""),
        ],
        comment="",
        initial_retention=1,
    ),
    select=r"""
    WITH comments_stg_1_adv AS (
        SELECT
            c2.level_type,
            c2.level,
            CASE
                WHEN level like 'AP-%' THEN planning_agency_ult_name
                WHEN level like 'PM-%' then planning_agency_name
                ELSE market_agc
            END AS name,
            asofdate,
            SUM(lyq_revenue) lyq_revenue,
            SUM(lyq_revenue_qtd) lyq_revenue_qtd,
            SUM(pq_revenue) pq_revenue,
            SUM(pq_revenue_qtd) pq_revenue_qtd,
            SUM(advertiser_quota) quota,
            SUM(l7d_avg_revenue) daily_run_rate,
            SUM(l7d_avg_revenue_prior) daily_run_rate_prior,
            SUM((cq_revenue) / (days_closed_in_quarter)
                * (days_total_in_quarter)
            ) sales_forecast,
            SUM(((cq_revenue_qtd_prior) / (days_closed_in_quarter - 7))
                * (days_total_in_quarter)
            ) sales_forecast_prior,
            SUM(run_rate_forecast) run_rate_forecast,
            SUM(run_rate_forecast_prior) run_rate_forecast_prior,
            SUM(cq_revenue) cq_revenue,
            SUM(cq_revenue_qtd_prior) cq_revenue_qtd_prior,
            SUM(lyq_revenue_qtd) / SUM(lyq_revenue) Ly_delivery,
            SUM(lyq_revenue_qtd_prior) / SUM(lyq_revenue) Ly_delivery_prior,
            SUM(advertiser_quota) * SUM(lyq_revenue_qtd) / SUM(lyq_revenue) quota_qtd,
            SUM(advertiser_quota) * SUM(lyq_revenue_qtd_prior) / SUM(lyq_revenue) quota_qtd_prior,
            (SUM(cq_revenue) / MIN(days_closed_in_quarter)) * MIN(
                days_total_in_quarter
            ) straightline_forecast,
            (SUM(cq_revenue_qtd_prior) / MIN(days_closed_in_quarter - 7))
                * MIN(days_total_in_quarter) straightline_forecast_prior,
            (SUM(advertiser_quota) - SUM(cq_revenue)) / DATE_DIFF(
                'day',
                CAST(asofdate AS date),
                CAST(<QUARTERID> AS date) - INTERVAL '1' DAY
            ) AS required_run_rate
        FROM bpo_gms_quota_and_forecast_fast
        CROSS JOIN UNNEST(
                ARRAY[
                    'Level 2 Agency',
                    'Level 4 Agency',
                    'Level 6 Agency',
                    'Level 8 Agency',
                    'Level 10 Agency',
                    'Level 12 Agency'
                ],
                ARRAY[
                    l2_agency_territory,
                    l4_agency_territory,
                    l6_agency_territory,
                    l8_agency_territory,
                    l10_agency_territory,
                    l12_agency_territory
                ]
            ) c2 (level_type, level)
        WHERE
            ds = '<DATEID>'
            AND l2_agency_territory = 'GBG Agency'
            and revenue_segment not in ('GPA Managed','GPA Unmanaged')
        GROUP BY
            1, 2, 3, 4
    ),
    comments_stg_2_adv AS (
        SELECT
            level,
            level_type,
            name,
            quota,
            quota_qtd,
            quota_qtd_prior,
            cq_revenue,
            pq_revenue,
            lyq_revenue,
            (cq_revenue / quota) attainment_qtd,
            (cq_revenue / lyq_revenue_qtd) - 1 yoy_perc_revenue,
            (cq_revenue / pq_revenue_qtd) - 1 qoq_perc_revenue,
            (cq_revenue / quota) - (cq_revenue_qtd_prior / quota) wow_perc_quota,
            (COALESCE(sales_forecast,straightline_forecast)) sales_forecast,
            (COALESCE(sales_forecast_prior,straightline_forecast_prior)) sales_forecast_prior,
            (COALESCE(sales_forecast,straightline_forecast)) / quota perc_sales_forecast,
            (COALESCE(sales_forecast_prior,straightline_forecast_prior)) / quota perc_sales_forecast_prior,
            ((COALESCE(sales_forecast,straightline_forecast)) / lyq_revenue) - 1 yoy_perc_sales_forecast,
            ((COALESCE(sales_forecast,straightline_forecast)) / lyq_revenue) - 1 qoq_perc_sales_forecast,
            ((COALESCE(sales_forecast,straightline_forecast)) / (COALESCE(sales_forecast_prior,straightline_forecast_prior))) - 1 wow_perc_sales_forecast,
            (cq_revenue / quota_qtd) * quota seasonal_forecast,
            (cq_revenue_qtd_prior / quota_qtd_prior) * quota seasonal_forecast_prior,
            ((cq_revenue / quota_qtd) * quota) / quota perc_seasonal_forecast,
            ((cq_revenue_qtd_prior / quota_qtd_prior) * quota) / quota perc_seasonal_forecast_prior,
            (((cq_revenue / quota_qtd) * quota) / lyq_revenue) - 1 qoq_perc_seasonal_forecast,
            (((cq_revenue / quota_qtd) * quota) / lyq_revenue) - 1 yoy_perc_seasonal_forecast,
            (((cq_revenue / quota_qtd) * quota) / (
            (cq_revenue_qtd_prior / quota_qtd_prior) * quota)) - 1 wow_perc_seasonal_forecast,
            run_rate_forecast,
            run_rate_forecast_prior,
            run_rate_forecast / quota perc_run_rate_forecast,
            run_rate_forecast_prior / quota perc_run_rate_forecast_prior,
            (run_rate_forecast_prior / quota) - (run_rate_forecast / quota) wow_pp_run_rate_forecast,
            (run_rate_forecast / run_rate_forecast_prior) - 1 wow_perc_run_rate_forecast,
            daily_run_rate,
            daily_run_rate_prior,
            required_run_rate,
            (daily_run_rate / daily_run_rate_prior) - 1 wow_perc_daily_run_rate,
            straightline_forecast,
            straightline_forecast_prior,
            straightline_forecast / quota perc_straightline_forecast,
            straightline_forecast_prior / quota perc_straightline_forecast_prior,
            (straightline_forecast / lyq_revenue) - 1 yoy_perc_straightline_forecast,
            (straightline_forecast / lyq_revenue) - 1 qoq_perc_straightline_forecast,
            (straightline_forecast / straightline_forecast_prior) - 1 wow_perc_straightline_forecast,
            asofdate ds,
            asofdate
        FROM comments_stg_1_adv
    ),
    comments_stg_3_adv AS (
        SELECT
            level,
            level_type,
            name,
            quota,
            quota_qtd,
            quota_qtd_prior,
            cq_revenue,
            pq_revenue,
            lyq_revenue,
            attainment_qtd,
            yoy_perc_revenue,
            qoq_perc_revenue,
            wow_perc_quota,
            sales_forecast,
            sales_forecast_prior,
            perc_sales_forecast,
            perc_sales_forecast_prior,
            yoy_perc_sales_forecast,
            qoq_perc_sales_forecast,
            wow_perc_sales_forecast,
            seasonal_forecast,
            seasonal_forecast_prior,
            perc_seasonal_forecast,
            perc_seasonal_forecast_prior,
            qoq_perc_seasonal_forecast,
            yoy_perc_seasonal_forecast,
            wow_perc_seasonal_forecast,
            run_rate_forecast,
            perc_run_rate_forecast,
            perc_run_rate_forecast_prior,
            wow_pp_run_rate_forecast,
            wow_perc_run_rate_forecast,
            daily_run_rate,
            daily_run_rate_prior,
            required_run_rate,
            wow_perc_daily_run_rate,
            straightline_forecast,
            straightline_forecast_prior,
            perc_straightline_forecast,
            perc_straightline_forecast_prior,
            yoy_perc_straightline_forecast,
            qoq_perc_straightline_forecast,
            wow_perc_straightline_forecast,
            CASE
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) > 0.05
                     THEN 'a strong increase of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN 0.02
                    AND 0.05 THEN 'good growth of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN 0.001
                    AND 0.02 THEN 'slight growth of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN 0
                     AND 0.001 THEN 'no change of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) < -0.05
                    THEN 'a strong decrease of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN -0.02
                AND -0.02    THEN 'a slight decline of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN -0.001
                    AND -0.02 THEN 'a small loss of '
                WHEN (perc_sales_forecast - perc_sales_forecast_prior) BETWEEN 0
                    AND -0.001 THEN 'no change of '
                ELSE ''
            END wow_perc_sales_forecast_comments,
            CASE
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) > 0.05
                    THEN 'a significant increase of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN 0.02
                    AND 0.05
                    THEN 'a great upward move of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN 0.001
                    AND 0.02
                    THEN 'slight increase of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN 0
                     AND 0.001
                    THEN 'a stagnant '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) < -0.05
                    THEN 'a decrease of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN -0.02
                AND -0.02
                    THEN 'a drop of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN -0.001
                    AND -0.02
                    THEN 'a small drop of '
                WHEN (perc_seasonal_forecast - perc_seasonal_forecast_prior) BETWEEN 0
                    AND -0.001
                    THEN 'a stagnant '
                ELSE ''
            END wow_perc_seasonal_forecast_comments,
            CASE
                WHEN (perc_straightline_forecast - perc_seasonal_forecast_prior) > 0.05
                    THEN 'a significant increase of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN 0.02
                    AND 0.05
                    THEN 'a great upward move of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN 0.001
                    AND 0.02
                    THEN 'slight increase of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN 0
                     AND 0.001
                    THEN 'a stagnant '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) < -0.05
                    THEN 'a decrease of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN -0.02
                AND -0.02
                    THEN 'a drop of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN -0.001
                    AND -0.02
                    THEN 'a small drop of '
                WHEN (perc_straightline_forecast - perc_straightline_forecast_prior) BETWEEN 0
                    AND -0.001
                    THEN 'a stagnant '
                ELSE ''
            END wow_perc_straightline_forecast_comments,
            CASE
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) > 0.05
                    THEN 'a strong increase of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN 0.02
                    AND 0.05
                    THEN 'good growth of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN 0.001
                    AND 0.02
                    THEN 'slight growth of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN 0
                     AND 0.001
                    THEN 'no change of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) < -0.05
                    THEN 'a strong decrease of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN -0.02
                AND -0.02
                    THEN 'a slight decline of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN -0.001
                    AND -0.02
                    THEN 'a small loss of '
                WHEN (perc_run_rate_forecast - perc_run_rate_forecast_prior) BETWEEN 0
                    AND -0.001
                    THEN 'no change of '
                ELSE ''
            END wow_perc_run_rate_forecast_comments,
            (perc_seasonal_forecast - perc_seasonal_forecast_prior) pp_wow_seasonal_forecast,
            (perc_sales_forecast - perc_sales_forecast_prior) pp_wow_sales_forecast,
            (perc_straightline_forecast - perc_straightline_forecast_prior) pp_wow_straightline_forecast,
            (perc_run_rate_forecast - perc_run_rate_forecast_prior) pp_wow_run_rate_forecast,
            CASE
                WHEN is_nan(seasonal_forecast - seasonal_forecast_prior)
                    OR is_infinite(seasonal_forecast - seasonal_forecast_prior)
                    THEN NULL
                ELSE (seasonal_forecast - seasonal_forecast_prior)
            END seasonal_wow_delta,
            CASE
                WHEN is_nan(sales_forecast - sales_forecast_prior)
                    OR is_infinite(sales_forecast - sales_forecast_prior) THEN NULL
                ELSE (sales_forecast - sales_forecast_prior)
            END sales_wow_delta,
            CASE
                WHEN is_nan(straightline_forecast - straightline_forecast_prior)
                    OR is_infinite(
                    straightline_forecast - straightline_forecast_prior
                ) THEN NULL
                ELSE (straightline_forecast - straightline_forecast_prior)
            END straightline_wow_delta,
            CASE
                WHEN is_nan(run_rate_forecast - run_rate_forecast_prior)
                    OR is_infinite(run_rate_forecast - run_rate_forecast_prior)
                    THEN NULL
                ELSE (run_rate_forecast - run_rate_forecast_prior)
            END run_rate_wow_delta,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(seasonal_forecast - seasonal_forecast_prior)
                            OR is_infinite(
                            seasonal_forecast - seasonal_forecast_prior
                        ) THEN NULL
                        ELSE (seasonal_forecast - seasonal_forecast_prior)
                    END DESC
            ) highest_rank_seasonal,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(seasonal_forecast - seasonal_forecast_prior)
                            OR is_infinite(
                            seasonal_forecast - seasonal_forecast_prior
                        ) THEN NULL
                        ELSE (seasonal_forecast - seasonal_forecast_prior)
                    END ASC
            ) lowest_rank_seasonal,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(sales_forecast - sales_forecast_prior)
                            OR is_infinite(sales_forecast - sales_forecast_prior)
                            THEN NULL
                        ELSE (sales_forecast - sales_forecast_prior)
                    END DESC
            ) highest_rank_sales,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(sales_forecast - sales_forecast_prior)
                            OR is_infinite(sales_forecast - sales_forecast_prior)
                            THEN NULL
                        ELSE (sales_forecast - sales_forecast_prior)
                    END ASC
            ) lowest_rank_sales,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(
                            straightline_forecast - straightline_forecast_prior
                        ) OR is_infinite(
                            straightline_forecast - straightline_forecast_prior
                        ) THEN NULL
                        ELSE (straightline_forecast - straightline_forecast_prior)
                    END DESC
            ) highest_rank_straightline,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(
                            straightline_forecast - straightline_forecast_prior
                        ) OR is_infinite(
                            straightline_forecast - straightline_forecast_prior
                        ) THEN NULL
                        ELSE (straightline_forecast - straightline_forecast_prior)
                    END ASC
            ) lowest_rank_straightline,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(run_rate_forecast - run_rate_forecast_prior)
                            OR is_infinite(
                            run_rate_forecast - run_rate_forecast_prior
                        ) THEN NULL
                        ELSE (run_rate_forecast - run_rate_forecast_prior)
                    END DESC
            ) highest_rank_run_rate,
            ROW_NUMBER() OVER(
                PARTITION BY
                    level_type,
                    level
                ORDER BY
                    CASE
                        WHEN is_nan(run_rate_forecast - run_rate_forecast_prior)
                            OR is_infinite(
                            run_rate_forecast - run_rate_forecast_prior
                        ) THEN NULL
                        ELSE (run_rate_forecast - run_rate_forecast_prior)
                    END ASC
            ) lowest_rank_run_rate,
            asofdate ds,
            asofdate
        FROM comments_stg_2_adv
    ),
    comments_stg_4_adv AS (
        SELECT
            level_type,
            level,
            MAX(ds) AS ds,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 1 AND seasonal_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr1_sf_name_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 1 AND seasonal_wow_delta >= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 1 AND seasonal_wow_delta >= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 2 AND seasonal_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr2_sf_name_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 2 AND seasonal_wow_delta >= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 2 AND seasonal_wow_delta >= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 3 AND seasonal_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr3_sf_name_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 3 AND seasonal_wow_delta >= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_seasonal = 3 AND seasonal_wow_delta >= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 1 AND seasonal_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_name_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 1 AND seasonal_wow_delta <= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 1 AND seasonal_wow_delta <= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 2 AND seasonal_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_name_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 2 AND seasonal_wow_delta <= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 2 AND seasonal_wow_delta <= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 3 AND seasonal_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_name_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 3 AND seasonal_wow_delta <= 0.001
                        THEN perc_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN lowest_rank_seasonal = 3 AND seasonal_wow_delta <= 0.001
                        THEN pp_wow_seasonal_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_wow_sf_perc_seasonal,
            MAX(
                CASE
                    WHEN highest_rank_sales = 1 AND sales_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr1_sf_name_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 1 AND sales_wow_delta >= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 1 AND sales_wow_delta >= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 2 AND sales_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr2_sf_name_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 2 AND sales_wow_delta >= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 2 AND sales_wow_delta >= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 3 AND sales_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr3_sf_name_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 3 AND sales_wow_delta >= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_sales = 3 AND sales_wow_delta >= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 1 AND sales_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_name_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 1 AND sales_wow_delta <= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 1 AND sales_wow_delta <= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 2 AND sales_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_name_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 2 AND sales_wow_delta <= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 2 AND sales_wow_delta <= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 3 AND sales_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_name_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 3 AND sales_wow_delta <= 0.001
                        THEN perc_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_perc_sales,
            MAX(
                CASE
                    WHEN lowest_rank_sales = 3 AND sales_wow_delta <= 0.001
                        THEN pp_wow_sales_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_wow_sf_perc_sales,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 1 AND straightline_wow_delta
                        >= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr1_sf_name_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 1 AND straightline_wow_delta
                        >= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 1 AND straightline_wow_delta
                        >= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 2 AND straightline_wow_delta
                        >= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr2_sf_name_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 2 AND straightline_wow_delta
                        >= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 2 AND straightline_wow_delta
                        >= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 3 AND straightline_wow_delta
                        >= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr3_sf_name_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 3 AND straightline_wow_delta
                        >= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_straightline = 3 AND straightline_wow_delta
                        >= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 1 AND straightline_wow_delta
                        <= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_name_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 1 AND straightline_wow_delta
                        <= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 1 AND straightline_wow_delta
                        <= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 2 AND straightline_wow_delta
                        <= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_name_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 2 AND straightline_wow_delta
                        <= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 2 AND straightline_wow_delta
                        <= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 3 AND straightline_wow_delta
                        <= 0.001 THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_name_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 3 AND straightline_wow_delta
                        <= 0.001 THEN perc_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_perc_straightline,
            MAX(
                CASE
                    WHEN lowest_rank_straightline = 3 AND straightline_wow_delta
                        <= 0.001 THEN pp_wow_straightline_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_wow_sf_perc_straightline,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 1 AND run_rate_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr1_sf_name_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 1 AND run_rate_wow_delta >= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 1 AND run_rate_wow_delta >= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr1_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 2 AND run_rate_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr2_sf_name_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 2 AND run_rate_wow_delta >= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 2 AND run_rate_wow_delta >= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr2_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 3 AND run_rate_wow_delta >= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS highest_nr3_sf_name_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 3 AND run_rate_wow_delta >= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN highest_rank_run_rate = 3 AND run_rate_wow_delta >= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS highest_nr3_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 1 AND run_rate_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_name_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 1 AND run_rate_wow_delta <= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 1 AND run_rate_wow_delta <= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr1_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 2 AND run_rate_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_name_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 2 AND run_rate_wow_delta <= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 2 AND run_rate_wow_delta <= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr2_wow_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 3 AND run_rate_wow_delta <= 0.001
                        THEN SUBSTR(UPPER(name), 1, 1) || SUBSTR(
                        LOWER(name),
                        2,
                        15
                    )
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_name_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 3 AND run_rate_wow_delta <= 0.001
                        THEN perc_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_sf_perc_run_rate,
            MAX(
                CASE
                    WHEN lowest_rank_run_rate = 3 AND run_rate_wow_delta <= 0.001
                        THEN pp_wow_run_rate_forecast
                    ELSE NULL
                END
            ) AS lowest_nr3_wow_sf_perc_run_rate
        FROM comments_stg_3_adv
        GROUP BY
            1, 2
    )
    SELECT
        CASE
            WHEN highest_nr1_sf_name_seasonal IS NULL
                THEN 'No notable Gainers this Week.'
            ELSE 'Gains this week using Seasonal came from '
        END || COALESCE(highest_nr1_sf_name_seasonal, '') || CASE
            WHEN highest_nr1_sf_name_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_seasonal IS NULL THEN ''
            WHEN highest_nr2_sf_perc_seasonal IS NULL THEN 'pp WoW).'
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(highest_nr2_sf_name_seasonal, '') || CASE
            WHEN highest_nr2_sf_perc_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_seasonal IS NULL THEN ''
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(highest_nr3_sf_name_seasonal, '') || CASE
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE 'pp WoW). '
        END || CASE
            WHEN lowest_nr1_sf_name_seasonal IS NULL
                THEN 'No notable Changes for Losses this Week. '
            ELSE 'Looking at Seasonal the Decline came from '
        END || COALESCE(lowest_nr1_sf_name_seasonal, '') || CASE
            WHEN lowest_nr1_sf_name_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_name_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_perc_seasonal IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN 'pp WoW).'
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(lowest_nr2_sf_name_seasonal, '') || CASE
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN ''
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(lowest_nr3_sf_name_seasonal, '') || CASE
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_seasonal IS NULL THEN ''
            WHEN lowest_nr1_sf_perc_seasonal IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_wow_sf_perc_seasonal, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_seasonal IS NULL THEN ''
            ELSE 'pp WoW). '
        END AS level_comments_seasonal,
        CASE
            WHEN highest_nr1_sf_name_sales IS NULL THEN 'No notable Gainers this Week.'
            ELSE 'We have seen gains using Sales Fcst from '
        END || COALESCE(highest_nr1_sf_name_sales, '') || CASE
            WHEN highest_nr1_sf_name_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_sales IS NULL THEN ''
            WHEN highest_nr2_sf_perc_sales IS NULL THEN 'pp WoW).'
            WHEN highest_nr3_sf_perc_sales IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(highest_nr2_sf_name_sales, '') || CASE
            WHEN highest_nr2_sf_perc_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_sales IS NULL THEN ''
            WHEN highest_nr3_sf_perc_sales IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(highest_nr3_sf_name_sales, '') || CASE
            WHEN highest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE 'pp WoW). '
        END || CASE
            WHEN lowest_nr1_sf_name_sales IS NULL
                THEN 'No notable Changes for Losses this Week. '
            ELSE 'Decline using Sales this week came from '
        END || COALESCE(lowest_nr1_sf_name_sales, '') || CASE
            WHEN lowest_nr1_sf_name_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_name_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_perc_sales IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN 'pp WoW).'
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(lowest_nr2_sf_name_sales, '') || CASE
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN ''
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(lowest_nr3_sf_name_sales, '') || CASE
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_sales IS NULL THEN ''
            WHEN lowest_nr1_sf_perc_sales IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_wow_sf_perc_sales, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_sales IS NULL THEN ''
            ELSE 'pp WoW). '
        END AS level_comments_sales,
        CASE
            WHEN highest_nr1_sf_name_straightline IS NULL
                THEN 'No notable Gainers this Week.'
            ELSE 'We have seen gains using straightline from '
        END || COALESCE(highest_nr1_sf_name_straightline, '') || CASE
            WHEN highest_nr1_sf_name_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_straightline IS NULL THEN ''
            WHEN highest_nr2_sf_perc_straightline IS NULL THEN 'pp WoW).'
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(highest_nr2_sf_name_straightline, '') || CASE
            WHEN highest_nr2_sf_perc_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_straightline IS NULL THEN ''
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(highest_nr3_sf_name_straightline, '') || CASE
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE 'pp WoW). '
        END || CASE
            WHEN lowest_nr1_sf_name_straightline IS NULL
                THEN 'No notable Changes for Losses this Week. '
            ELSE 'Decline using straightline this week came from '
        END || COALESCE(lowest_nr1_sf_name_straightline, '') || CASE
            WHEN lowest_nr1_sf_name_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_name_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_perc_straightline IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN 'pp WoW).'
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(lowest_nr2_sf_name_straightline, '') || CASE
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN ''
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(lowest_nr3_sf_name_straightline, '') || CASE
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_straightline IS NULL THEN ''
            WHEN lowest_nr1_sf_perc_straightline IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_wow_sf_perc_straightline, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_straightline IS NULL THEN ''
            ELSE 'pp WoW). '
        END AS level_comments_straightline,
        CASE
            WHEN highest_nr1_sf_name_run_rate IS NULL
                THEN 'No notable Gainers this Week when looking at Run Rate.'
            ELSE 'Looking at Run Rate we have seen an increase from '
        END || COALESCE(highest_nr1_sf_name_run_rate, '') || CASE
            WHEN highest_nr1_sf_name_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr1_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr1_sf_name_run_rate IS NULL THEN ''
            WHEN highest_nr2_sf_perc_run_rate IS NULL THEN 'pp WoW).'
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(highest_nr2_sf_name_run_rate, '') || CASE
            WHEN highest_nr2_sf_perc_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr2_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr2_sf_perc_run_rate IS NULL THEN ''
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(highest_nr3_sf_name_run_rate, '') || CASE
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * highest_nr3_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN highest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE 'pp WoW). '
        END || CASE
            WHEN lowest_nr1_sf_name_run_rate IS NULL
                THEN 'No notable Changes for Losses this Week. '
            ELSE 'Run Rate drop this week came from '
        END || COALESCE(lowest_nr1_sf_name_run_rate, '') || CASE
            WHEN lowest_nr1_sf_name_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_name_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr1_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr1_sf_perc_run_rate IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN 'pp WoW).'
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN 'pp WoW) & '
            ELSE 'pp WoW), '
        END || COALESCE(lowest_nr2_sf_name_run_rate, '') || CASE
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr2_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN ''
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN 'pp WoW).'
            ELSE 'pp WoW) & '
        END || COALESCE(lowest_nr3_sf_name_run_rate, '') || CASE
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN ''
            WHEN lowest_nr2_sf_perc_run_rate IS NULL THEN ''
            WHEN lowest_nr1_sf_perc_run_rate IS NULL THEN ''
            ELSE ' ('
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE '%, '
        END || COALESCE(
            CAST(ROUND(100 * lowest_nr3_wow_sf_perc_run_rate, 1) AS VARCHAR),
            ''
        ) || CASE
            WHEN lowest_nr3_sf_perc_run_rate IS NULL THEN ''
            ELSE 'pp WoW). '
        END AS level_comments_run_rate,
        level_type,
        level,
        DS
    FROM comments_stg_4_adv
    """,
)

gms_dynamic_comments_snapshot = PrestoInsertOperatorWithSchema(
    dep_list=[
        gms_dynamic_comments_stg_1_advertiser,
        gms_dynamic_comments_stg_1_agency,
        gms_dynamic_comments_stg_2_advertiser,
        gms_dynamic_comments_stg_2_agency,
    ],
    table="<TABLE:gms_dynamic_comments_snapshot>",
    documentation={"description": """Combing Overall Stats with Top Clients/Markets"""},
    create=Table(
        cols=[
            Column("level_type", "VARCHAR", "", policy=HiveAnon.NOT_UII),
            Column("level", "VARCHAR", "", policy=HiveAnon.NOT_UII),
            Column(
                "level_comments_sales_forecast", "VARCHAR", "", policy=HiveAnon.NOT_UII
            ),
            Column(
                "level_comments_seasonal_forecast",
                "VARCHAR",
                "",
                policy=HiveAnon.NOT_UII,
            ),
            Column(
                "level_comments_run_rate_forecast",
                "VARCHAR",
                "",
                policy=HiveAnon.NOT_UII,
            ),
            Column(
                "level_comments_straightline_forecast",
                "VARCHAR",
                "",
                policy=HiveAnon.NOT_UII,
            ),
            Column("required_run_rate", "VARCHAR", "", policy=HiveAnon.NOT_UII),
        ],
        partitions=[
            Column("ds", "VARCHAR", ""),
        ],
        comment="",
        initial_retention=90,
    ),
    select=r"""
            SELECT
stg1.level_type,
stg1.level,
COALESCE(stg1.level_comments_sales_forecast,'') || COALESCE(stg2.level_comments_sales,'') level_comments_sales_forecast,
COALESCE(stg1.level_comments_seasonal_forecast,'') || COALESCE(stg2.level_comments_seasonal,'') level_comments_seasonal_forecast,
COALESCE(stg1.level_comments_run_rate_forecast,'') || COALESCE(stg2.level_comments_run_rate,'') level_comments_run_rate_forecast,
COALESCE(stg1.level_comments_straightline_forecast,'') || COALESCE(stg2.level_comments_straightline,'') level_comments_straightline_forecast,
COALESCE(stg1.required_run_rate,'') required_run_rate,
stg1.ds
from <TABLE:gms_dynamic_comments_stg_1> stg1
left join <TABLE:gms_dynamic_comments_stg_2> stg2
on stg1.level = stg2.level and stg1.level_type = stg2.level_type
and stg1.ds = stg2.ds
where stg1.ds = '<DATEID>'
    """,
)


if is_test():
    pass
else:

    tableau_refresh_bpo_gms_quota_and_forecast_rtm_fast = TableauPublishOperator(
        dep_list=[
            gms_dynamic_comments_snapshot,
        ],
        refresh_cfg_id=28714,
        num_retries=2,
    )
