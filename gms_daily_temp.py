#!/usr/bin/env python3
# @lint-ignore-every DSFMT [do not copy-paste]. Format the file to remove this comment. https://fburl.com/wiki/mmzh332m

# See https://our.intern.facebook.com/intern/dex/dataswarm-operators/
# for a list of all dataswarm operators and usage information.
from dataswarm.operators import (
    GlobalDefaults,
    PrestoOperator,
    TableauPublishOperator,
    WaitForHiveOperator,
    PrestoInsertOperator,
    WaitForHivePartitionsOperator,
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

wait_for_bpo_gms_quota_and_forecast_snapshot = WaitForHiveOperator(
    dep_list=[],
    table="bpo_gms_quota_and_forecast_snapshot",
    use_future=False,
    fail_on_future=False,
)

wait_for_inc_dim_organization_to_brand_team_crm = WaitForHiveOperator(
    dep_list=[],
    table="inc_dim_organization_to_brand_team_crm",
    use_future=False,
    fail_on_future=False,
)

wait_for_acdp_dim_l45_commission_split_v2 = WaitForHiveOperator(
    dep_list=[],
    table="acdp_dim_l45_commission_split_v2:ad_reporting",
    use_future=False,
    fail_on_future=False,
)

wait_for_acdp_dim_l45_organization = WaitForHiveOperator(
    dep_list=[],
    table="acdp_dim_l45_organization:ad_reporting",
    use_future=False,
    fail_on_future=False,
)

wait_for_l45_fct_account_team_target = WaitForHiveOperator(
    dep_list=[],
    table="l45_fct_account_team_target:ad_reporting",
    use_future=False,
    fail_on_future=False,
)

wait_for_l45_fct_account_team_target = WaitForHivePartitionsOperator(
    table="l45_fct_account_team_target:ad_reporting",
    namespace="edw_bir01",
    partitions_list=[
        "ds=<DATEID>/program_fbid=624028128057659",
        "ds=<DATEID>/program_fbid=548525925668144",
    ],
)

insert_bpo_gms_quota_and_forecast_temp_adv = PrestoInsertOperator(
    dep_list=[
        wait_for_bpo_gms_quota_and_forecast_snapshot,
        wait_for_inc_dim_organization_to_brand_team_crm,
        wait_for_acdp_dim_l45_commission_split_v2,
        wait_for_acdp_dim_l45_organization,
        wait_for_l45_fct_account_team_target,
    ],
    table="<TABLE:bpo_gms_quota_and_forecast_temp>",
    partition="ds=<DATEID>/route_to_market=Advertiser",
    documentation={
        "description": """GMS Dashboard Logic without Row Level Security and only Advertiser View"""
    },
    create=r"""
CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_quota_and_forecast_temp> (
             ultimate_client_fbid varchar
            ,ultimate_client_sfid varchar
            ,ultimate_client_name varchar
            ,client_name varchar
            ,client_fbid varchar
            ,client_sfid varchar
            ,revenue_segment varchar
            ,asofdate varchar
            ,quarter_id varchar
            ,next_quarter_id varchar
            ,days_left_in_quarter bigint
            ,days_left_in_quarter_prior bigint
            ,days_total_in_quarter bigint
            ,days_closed_in_quarter bigint
            ,l12_reporting_territory varchar
            ,l10_reporting_territory varchar
            ,l8_reporting_territory varchar
            ,l6_reporting_territory varchar
            ,l4_reporting_territory varchar
            ,l2_reporting_territory varchar
                , run_rate_forecast double
                , run_rate_forecast_prior double
                , quota double
                , sales_forecast double
                , sales_forecast_prior double
                , cq_revenue double
                , pq_revenue double
                , cq_revenue_qtd_prior double
                , lyq_revenue double
                , l2yq_revenue double
                , lyq_revenue_qtd double
                , l2yq_revenue_qtd double
                , lyq_revenue_qtd_prior double
                , l2yq_revenue_qtd_prior double
                , pq_revenue_qtd double
                , L7d_revenue double
                , L7d_revenue_prior double
                , L7d_avg_revenue double
                , L7d_avg_revenue_prior double
                , L14d_revenue double
                , L14d_revenue_prior double
                , L14d_avg_revenue double
                , L14d_avg_revenue_prior double
                , sales_forecast_prior_2w double
                , ds varchar
                , route_to_market varchar
)
WITH (
   format = 'DWRF',
   oncall = 'gms_central_analytics',
   partitioned_by = ARRAY['ds','route_to_market'],
   retention_days = 7,
   uii = false
)
    """,
    select=r"""
WITH quarter_date as (Select
cast('<quarter_id>' as date) as quarter_id,
(cast('<quarter_id>' as date) + INTERVAL '3' MONTH) next_quarter_id,
max(cast(date_id as date)) asofdate
from
bpo_gms_quota_and_forecast_snapshot
where cq_revenue is not null and ds = '<DATEID>'
group by 1,2)

,quarter_dates as (
Select
quarter_id,
next_quarter_id,
asofdate,
DATE_DIFF('day',(asofdate),next_quarter_id- INTERVAL '1' DAY) days_left_in_quarter,
DATE_DIFF('day',(asofdate - INTERVAL '7' DAY),next_quarter_id- INTERVAL '1' DAY) days_left_in_quarter_prior,
DATE_DIFF('day', quarter_id, next_quarter_id) days_total_in_quarter,
DATE_DIFF('day', quarter_id, asofdate + INTERVAL '1' DAY) days_closed_in_quarter
from
quarter_date),
am_goals as
(SELECT
    a.territory_l12_name,
    c.org_fbid client_fbid,
    c.ultimate_parent_org_fbid ultimate_client_fbid,
    a.ds,
    SUM(a.rec_rev_target) AS am_goal
FROM l45_fct_account_team_target:ad_reporting a
LEFT JOIN (
    SELECT DISTINCT
        a.employee_id,
        a.brand_team_id,
        a.commission_split_role_name,
        a.territory_id,
        b.id1 AS org_fbid,
        a.ds
    FROM acdp_dim_l45_commission_split_v2:ad_reporting a
    LEFT JOIN inc_dim_organization_to_brand_team_crm b
        ON a.brand_team_id = b.id2
        AND b.ds = '<DATEID>'
    WHERE
        a.ds = '<DATEID>'
        AND a.commission_split_status = 'active'
        AND '<DATEID>' BETWEEN a.commission_split_start_date AND COALESCE(
            a.commission_split_end_date,
            '2099-12-31'
        )
) b
    ON a.employee_fbid = b.employee_id
    AND a.account_team_fbid = b.brand_team_id
    AND a.territory_l12_id = b.territory_id
LEFT JOIN acdp_dim_l45_organization:ad_reporting c
    ON b.org_fbid = c.org_fbid
    AND c.ds = '<DATEID>'
WHERE
    a.ds = '<DATEID>'
    AND a.quarter_id = CAST(DATE_TRUNC('QUARTER', CAST('<DATEID>' AS DATE)) AS VARCHAR)
    AND a.commission_type = 'SMB Account Strategist AM'
GROUP BY
    1, 2, 3,4 )
,quota_and_fcast as (

Select
 ultimate_parent_fbid ultimate_client_fbid
,ultimate_parent_sfid ultimate_client_sfid
,ultimate_parent_name ultimate_client_name
,advertiser_name client_name
,advertiser_fbid client_fbid
,advertiser_sfid client_sfid
,cast(asofdate as varchar) asofdate
,cast(quarter_id as varchar) quarter_id
,cast(next_quarter_id as varchar) next_quarter_id
,days_left_in_quarter
,days_left_in_quarter_prior
,days_total_in_quarter
,days_closed_in_quarter
,l12_reporting_territory
,l10_reporting_territory
,l8_reporting_territory
,l6_reporting_territory
,l4_reporting_territory
,l2_reporting_territory
,revenue_segment
,SUM(cq_revenue) + (SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_revenue else 0 end)/7 * days_left_in_quarter) run_rate_forecast

,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_revenue else 0 end) + (SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY)
and cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_revenue else 0 end)/7 * days_left_in_quarter_prior) run_rate_forecast_prior

,(SUM(case when date_id = '<quarter_id>' then advertiser_quota else 0 end) - SUM(cq_revenue)) / days_left_in_quarter as required_run_rate


,SUM(case when date_id = '<quarter_id>' then advertiser_quota else 0 end) advertiser_quota
,SUM(case when date_id = '<quarter_id>' then sales_forecast else 0 end)  sales_forecast
,SUM(case when date_id = '<quarter_id>' then sales_forecast_prior else 0 end)  sales_forecast_prior
,SUM(CASE WHEN date_id = '<quarter_id>' THEN reseller_quota ELSE 0 END) reseller_quota
,SUM(CASE WHEN date_id = '<quarter_id>' THEN agency_quota ELSE 0 END) agency_quota
--Revenue
,SUM(cq_revenue) cq_revenue
,SUM(pq_revenue) pq_revenue
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_revenue else 0 end) cq_revenue_qtd_prior
,Sum(ly_revenue) lyq_revenue
,Sum(l2y_revenue) l2yq_revenue
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_revenue else 0 end) lyq_revenue_qtd
,Sum(case when cast(date_id as date) <= (asofdate)
then l2y_revenue else 0 end) l2yq_revenue_qtd
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_revenue else 0 end) lyq_revenue_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then l2y_revenue else 0 end) l2yq_revenue_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate)
then pq_revenue else 0 end) pq_revenue_qtd
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_revenue else null end) L7d_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_revenue else null end) L7d_revenue_prior

,

SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_revenue else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_revenue else null end) / days_closed_in_quarter) end) L7d_avg_revenue,

SUM(case when days_closed_in_quarter >= 14 then (case when date_id >= '<DATEID-13>'
and date_id <= '<DATEID-7>' then cq_revenue else null end) / 7 else
(case when date_id >= '<DATEID-13>' and date_id <= '<DATEID-7>'
then cq_revenue else null end) / (days_closed_in_quarter-7) end) L7d_avg_revenue_prior ,


SUM(case when days_closed_in_quarter >= 14 then ((case when date_id >= '<DATEID-13>'
then cq_revenue else null end) / 14) else
((case when date_id >= '<DATEID>' then
cq_revenue else null end) / days_closed_in_quarter) end) L14d_avg_revenue,

SUM(case when days_closed_in_quarter >= 28 then (case when date_id >= '<DATEID-27>'
and date_id < '<DATEID-14>' then cq_revenue else null end) / 14 else
(case when date_id >= '<DATEID-20>' and date_id < '<DATEID-14>'
then cq_revenue else null end) / (days_closed_in_quarter-14) end) L14d_avg_revenue_prior ,

SUM(case when days_closed_in_quarter >= 28 then ((case when date_id >= '<DATEID-27>'
then cq_revenue else null end) / 28) else
((case when date_id >= '<DATEID>' then
cq_revenue else null end) / days_closed_in_quarter) end) L28d_avg_revenue,

SUM(case when days_closed_in_quarter >= 56 then (case when date_id >= '<DATEID-55>'
and date_id < '<DATEID-28>' then cq_revenue else null end) / 28 else
(case when date_id >= '<DATEID-55>' and date_id < '<DATEID-28>'
then cq_revenue else null end) / (days_closed_in_quarter-28) end) L28d_avg_revenue_prior

,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_revenue else null end)/7 L7d_avg_ly_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_revenue else null end)/7 L7d_avg_ly_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_revenue else null end) L7d_ly_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_revenue else null end) L7d_ly_revenue_prior
,SUM(cq_optimal) cq_optimal
,SUM(pq_optimal) pq_optimal
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_optimal else 0 end) cq_optimal_qtd_prior
,Sum(ly_optimal) lyq_optimal
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_optimal else 0 end) lyq_optimal_qtd
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_optimal else 0 end) lyq_optimal_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate)
then pq_optimal else 0 end) pq_optimal_qtd
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_revenue else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_optimal else null end) / days_closed_in_quarter) end) L7d_optimal
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_optimal else null end) L7d_optimal_prior
    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_optimal else null end)/7 L7d_avg_optimal
    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_optimal else null end)/7 L7d_avg_optimal_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_optimal else null end)/7 L7d_avg_ly_optimal
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_optimal else null end)/7 L7d_avg_ly_optimal_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_optimal else null end) L7d_ly_optimal
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_optimal else null end) L7d_ly_optimal_prior
,SUM(CASE WHEN date_id = '<quarter_id>' THEN optimal_quota ELSE 0 END) optimal_quota
,SUM(CASE WHEN date_id = '<quarter_id>' THEN agc_optimal_quota ELSE 0 END) agc_optimal_quota
,'<DATEID>' as ds
,SUM(dr_resilient_cq) dr_resilient_cq
,SUM(dr_resilient_pq) dr_resilient_pq
,SUM(dr_resilient_ly) dr_resilient_ly
,SUM(dr_resilient_lyq) dr_resilient_lyq
,SUM(CASE WHEN date_id = '<quarter_id>' THEN dr_resilience_goal ELSE 0 END) dr_resilience_goal
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_cq else 0 end) cq_dr_resilient_qtd_prior
,Sum(dr_resilient_ly) lyq_dr_resilient
,Sum(case when cast(date_id as date) <= (asofdate)
then dr_resilient_ly else 0 end) lyq_dr_resilient_qtd
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_ly else 0 end) lyq_dr_resilient_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate)
then dr_resilient_pq else 0 end) pq_dr_resilient_qtd
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then dr_resilient_cq else null end) / 7) else
((case when date_id >= '<DATEID>' then
dr_resilient_cq else null end) / days_closed_in_quarter) end) L7d_dr_resilient
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_cq else null end) L7d_dr_resilient_prior
    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then dr_resilient_cq else null end)/7 L7d_avg_dr_resilient
    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_cq else null end)/7 L7d_avg_dr_resilient_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then dr_resilient_ly else null end)/7 L7d_avg_ly_dr_resilient
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_ly else null end)/7 L7d_avg_ly_dr_resilient_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then dr_resilient_ly else null end) L7d_ly_dr_resilient
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_ly else null end) L7d_ly_dr_resilient_prior
,SUM(CASE WHEN date_id = '<quarter_id>' THEN dr_resilience_goal ELSE 0 END) dr_resilient_quota
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_product_resilient_rec_rev else 0 end) cq_product_resilient_rec_rev_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_ebr_usd_rec_rev else 0 end) cq_ebr_usd_rec_rev_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_capi_ebr_revenue else 0 end) cq_capi_ebr_revenue_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_product_resilient_rec_rev else 0 end) lyq_product_resilient_rec_rev_qtd
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_ebr_usd_rec_rev else 0 end) lyq_ebr_usd_rec_rev_qtd
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_capi_ebr_revenue else 0 end) lyq_capi_ebr_revenue_qtd
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_product_resilient_rec_rev else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_product_resilient_rec_rev else null end) / days_closed_in_quarter) end) L7d_product_resilient_rec_rev
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_ebr_usd_rec_rev else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_ebr_usd_rec_rev else null end) / days_closed_in_quarter) end) L7d_ebr_usd_rec_rev
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_capi_ebr_revenue else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_capi_ebr_revenue else null end) / days_closed_in_quarter) end) L7d_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_product_resilient_rec_rev else null end) L7d_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_ebr_usd_rec_rev else null end) L7d_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_capi_ebr_revenue else null end) L7d_capi_ebr_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_product_resilient_rec_rev else null end)/7 L7d_avg_product_resilient_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_ebr_usd_rec_rev else null end)/7 L7d_avg_ebr_usd_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_capi_ebr_revenue else null end)/7 L7d_avg_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_product_resilient_rec_rev else null end)/7 l7d_avg_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_ebr_usd_rec_rev else null end)/7 l7d_avg_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_capi_ebr_revenue else null end)/7 l7d_avg_capi_ebr_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_product_resilient_rec_rev else null end)/7 l7d_avg_ly_product_resilient_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_ebr_usd_rec_rev else null end)/7 l7d_avg_ly_ebr_usd_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_capi_ebr_revenue else null end)/7 l7d_avg_ly_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_product_resilient_rec_rev else null end)/7 L7d_avg_ly_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_ebr_usd_rec_rev else null end)/7 L7d_avg_ly_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_capi_ebr_revenue else null end)/7 L7d_avg_ly_capi_ebr_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_product_resilient_rec_rev else null end) L7d_ly_product_resilient_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_ebr_usd_rec_rev  else null end) L7d_ly_ebr_usd_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_capi_ebr_revenue else null end) L7d_ly_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_product_resilient_rec_rev else null end) L7d_ly_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_ebr_usd_rec_rev else null end) L7d_ly_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_capi_ebr_revenue else null end) L7d_ly_capi_ebr_revenue_prior
,SUM(cq_product_resilient_rec_rev) cq_product_resilient_rec_rev
,SUM(cq_ebr_usd_rec_rev) cq_ebr_usd_rec_rev
,SUM(cq_capi_ebr_revenue) cq_capi_ebr_revenue
,SUM(pq_product_resilient_rec_rev) pq_product_resilient_rec_rev
,SUM(pq_ebr_usd_rec_rev) pq_ebr_usd_rec_rev
,SUM(pq_capi_ebr_revenue) pq_capi_ebr_revenue
,SUM(ly_product_resilient_rec_rev) ly_product_resilient_rec_rev
,SUM(ly_ebr_usd_rec_rev) ly_ebr_usd_rec_rev
,SUM(ly_capi_ebr_revenue) ly_capi_ebr_revenue
,SUM(lyq_product_resilient_rec_rev) lyq_product_resilient_rec_rev
,SUM(lyq_ebr_usd_rec_rev) lyq_ebr_usd_rec_rev
,SUM(lyq_capi_ebr_revenue) lyq_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY)
then cq_revenue else null end) L14d_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_revenue else null end) L14d_revenue_prior
,SUM(sales_forecast_prior_2w) sales_forecast_prior_2w

         from bpo_gms_quota_and_forecast_snapshot

              cross join quarter_dates

            where ds = '<DATEID>'

            group by
            ultimate_parent_fbid
            ,ultimate_parent_sfid
            ,ultimate_parent_name
            ,advertiser_name
            ,advertiser_fbid
            ,advertiser_sfid
            ,revenue_segment
            ,cast(asofdate as varchar)
            ,cast(quarter_id as varchar)
            ,cast(next_quarter_id as varchar)
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
,l12_reporting_territory
,l10_reporting_territory
,l8_reporting_territory
,l6_reporting_territory
,l4_reporting_territory
,l2_reporting_territory
)
, am_goals_no_rev as (

Select

            cast(c.ultimate_parent_org_sfid as varchar) ultimate_client_sfid
            ,cast(c.ultimate_parent_org_fbid as varchar) ultimate_client_fbid
            ,ultimate_parent_org_name ultimate_client_name
            ,c.parent_org_name client_name
            ,cast(c.org_fbid as varchar) client_fbid
            ,cast(c.org_fbid as varchar) client_sfid
            ,'GBG Scaled' revenue_segment
            ,cast(asofdate as varchar) asofdate
            ,cast(quarter_dates.quarter_id as varchar) quarter_id
            ,cast(next_quarter_id as varchar) next_quarter_id
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
            ,a.territory_l12_name l12_reporting_territory
            ,a.territory_l10_name l10_reporting_territory
            ,a.territory_l8_name l8_reporting_territory
            ,a.territory_l6_name l6_reporting_territory
            ,a.territory_l4_name l4_reporting_territory
            ,a.territory_l2_name l2_reporting_territory
                ,SUM(0) run_rate_forecast
                ,SUM(0) run_rate_forecast_prior
                ,SUM(a.rec_rev_target) quota
                ,SUM(0) sales_forecast
                ,SUM(0) sales_forecast_prior
                ,SUM(0) reseller_quota
                ,SUM(0) agency_quota
                --Revenue
                ,SUM(0) cq_revenue
                ,SUM(0) pq_revenue
                ,SUM(0) cq_revenue_qtd_prior
                ,SUM(0) lyq_revenue
                ,SUM(0) l2yq_revenue
                ,SUM(0) lyq_revenue_qtd
                ,SUM(0) l2yq_revenue_qtd
                ,SUM(0) lyq_revenue_qtd_prior
                ,SUM(0) l2yq_revenue_qtd_prior
                ,SUM(0) pq_revenue_qtd
                ,SUM(0) L7d_revenue
                ,SUM(0) L7d_revenue_prior
                ,SUM(0) L7d_avg_revenue
                ,SUM(0) L7d_avg_revenue_prior
                -- Optimal
                ,SUM(0) cq_optimal
                ,SUM(0) pq_optimal
                ,SUM(0) cq_optimal_qtd_prior
                ,SUM(0) lyq_optimal
                ,SUM(0) lyq_optimal_qtd
                ,SUM(0) lyq_optimal_qtd_prior
                ,SUM(0) pq_optimal_qtd
                ,SUM(0) l7d_optimal
                ,SUM(0) l7d_optimal_prior
                ,SUM(0) l7d_avg_optimal
                ,SUM(0) l7d_avg_optimal_prior
                ,SUM(0) l7d_avg_ly_optimal
                ,SUM(0) l7d_avg_ly_optimal_prior
                ,SUM(0) l7d_ly_optimal
                ,SUM(0) l7d_ly_optimal_prior
                ,SUM(0) optimal_quota
                ,SUM(0) agc_optimal_quota
                ,SUM(0) dr_resilience_goal
                ,SUM(0) dr_resilient_cq
                ,SUM(0) dr_resilient_pq
                ,SUM(0) dr_resilient_ly
                ,SUM(0) dr_resilient_lyq
                ,SUM(0) cq_dr_resilient_qtd_prior
                ,SUM(0) lyq_dr_resilient
                ,SUM(0) lyq_dr_resilient_qtd
                ,SUM(0) lyq_dr_resilient_qtd_prior
                ,SUM(0) pq_dr_resilient_qtd
                ,SUM(0) l7d_dr_resilient
                ,SUM(0) l7d_dr_resilient_prior
                ,SUM(0) l7d_avg_dr_resilient
                ,SUM(0) l7d_avg_dr_resilient_prior
                ,SUM(0) l7d_avg_ly_dr_resilient
                ,SUM(0) l7d_avg_ly_dr_resilient_prior
                ,SUM(0) l7d_ly_dr_resilient
                ,SUM(0) l7d_ly_dr_resilient_prior
                ,SUM(0) dr_resilient_quota
                ,SUM(0) cq_product_resilient_rec_rev
                ,SUM(0) cq_ebr_usd_rec_rev
                ,SUM(0) cq_capi_ebr_revenue
                ,SUM(0) pq_product_resilient_rec_rev
                ,SUM(0) pq_ebr_usd_rec_rev
                ,SUM(0) pq_capi_ebr_revenue
                ,SUM(0) ly_product_resilient_rec_rev
                ,SUM(0) ly_ebr_usd_rec_rev
                ,SUM(0) ly_capi_ebr_revenue
                ,SUM(0) lyq_product_resilient_rec_rev
                ,SUM(0) lyq_ebr_usd_rec_rev
                ,SUM(0) lyq_capi_ebr_revenue
                ,SUM(0) cq_product_resilient_rec_rev_qtd_prior
                ,SUM(0) cq_ebr_usd_rec_rev_qtd_prior
                ,SUM(0) cq_capi_ebr_revenue_qtd_prior
                ,SUM(0) lyq_product_resilient_rec_rev_qtd
                ,SUM(0) lyq_ebr_usd_rec_rev_qtd
                ,SUM(0) lyq_capi_ebr_revenue_qtd
                ,SUM(0) l7d_product_resilient_rec_rev
                ,SUM(0) l7d_ebr_usd_rec_rev
                ,SUM(0) l7d_capi_ebr_revenue
                ,SUM(0) l7d_product_resilient_rec_rev_prior
                ,SUM(0) l7d_ebr_usd_rec_rev_prior
                ,SUM(0) l7d_capi_ebr_revenue_prior
                ,SUM(0) l7d_avg_product_resilient_rec_rev
                ,SUM(0) l7d_avg_ebr_usd_rec_rev
                ,SUM(0) l7d_avg_capi_ebr_revenue
                ,SUM(0) l7d_avg_product_resilient_rec_rev_prior
                ,SUM(0) l7d_avg_ebr_usd_rec_rev_prior
                ,SUM(0) l7d_avg_capi_ebr_revenue_prior
                ,SUM(0) l7d_avg_ly_product_resilient_rec_rev
                ,SUM(0) l7d_avg_ly_ebr_usd_rec_rev
                ,SUM(0) l7d_avg_ly_capi_ebr_revenue
                ,SUM(0) l7d_avg_ly_product_resilient_rec_rev_prior
                ,SUM(0) l7d_avg_ly_ebr_usd_rec_rev_prior
                ,SUM(0) l7d_avg_ly_capi_ebr_revenue_prior
                ,SUM(0) l7d_ly_product_resilient_rec_rev
                ,SUM(0) l7d_ly_ebr_usd_rec_rev
                ,SUM(0)l7d_ly_capi_ebr_revenue
                ,SUM(0) l7d_ly_product_resilient_rec_rev_prior
                ,SUM(0) l7d_ly_ebr_usd_rec_rev_prior
                ,SUM(0) l7d_ly_capi_ebr_revenue_prior
                ,SUM(0) L14d_revenue
                ,SUM(0) L14d_revenue_prior
                ,SUM(0) L14d_avg_revenue
                ,SUM(0) L14d_avg_revenue_prior
                ,SUM(0) sales_forecast_prior_2w
                            ,a.ds ds

FROM l45_fct_account_team_target:ad_reporting a
LEFT JOIN (
    SELECT DISTINCT
        a.employee_id,
        a.brand_team_id,
        a.commission_split_role_name,
        a.territory_id,
        b.id1 AS org_fbid,
        a.ds
    FROM acdp_dim_l45_commission_split_v2:ad_reporting a
    LEFT JOIN inc_dim_organization_to_brand_team_crm b
        ON a.brand_team_id = b.id2
        AND b.ds = '<DATEID>'
    WHERE
        a.ds = '<DATEID>'
        AND a.commission_split_status = 'active'
        AND '<DATEID>' BETWEEN a.commission_split_start_date AND COALESCE(
            a.commission_split_end_date,
            '2099-12-31'
        )
) b
    ON a.employee_fbid = b.employee_id
    AND a.account_team_fbid = b.brand_team_id
    AND a.territory_l12_id = b.territory_id
LEFT JOIN acdp_dim_l45_organization:ad_reporting c
CROSS JOIN quarter_dates
    ON b.org_fbid = c.org_fbid
    AND c.ds = '<DATEID>'
WHERE
    a.ds = '<DATEID>'
    AND a.quarter_id = CAST(DATE_TRUNC('QUARTER', CAST('<DATEID>' AS DATE)) AS VARCHAR)
    AND a.commission_type = 'SMB Account Strategist AM'

group by

            cast(c.ultimate_parent_org_sfid as varchar)
            ,cast(c.ultimate_parent_org_fbid as varchar)
            ,ultimate_parent_org_name
            ,c.parent_org_name
            ,cast(c.org_fbid as varchar)
            ,cast(c.org_fbid as varchar)
            ,cast(asofdate as varchar)
            ,cast(quarter_dates.quarter_id as varchar)
            ,cast(next_quarter_id as varchar)
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
            ,a.territory_l12_name
            ,a.territory_l10_name
            ,a.territory_l8_name
            ,a.territory_l6_name
            ,a.territory_l4_name
            ,a.territory_l2_name
            ,a.ds

)

            SELECT
            quota_and_fcast.ultimate_client_fbid
            ,quota_and_fcast.ultimate_client_sfid
            ,quota_and_fcast.ultimate_client_name
            ,quota_and_fcast.client_name
            ,quota_and_fcast.client_fbid
            ,quota_and_fcast.client_sfid
            ,revenue_segment
            ,'<DATEID>' asofdate
            ,cast(quarter_id as varchar) quarter_id
            ,cast(next_quarter_id as varchar) next_quarter_id
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
            ,l12_reporting_territory
            ,l10_reporting_territory
            ,l8_reporting_territory
            ,l6_reporting_territory
            ,l4_reporting_territory
            ,l2_reporting_territory
                ,SUM(run_rate_forecast) run_rate_forecast
                ,SUM(run_rate_forecast_prior) run_rate_forecast_prior
                ,SUM(case when revenue_segment = 'GBG Scaled' then am_goals.am_goal else advertiser_quota end) quota
                ,SUM(sales_forecast) sales_forecast
                ,SUM(sales_forecast_prior) sales_forecast_prior
                ,SUM(reseller_quota) reseller_quota
                ,SUM(agency_quota) agency_quota
                --Revenue
                ,SUM(cq_revenue) cq_revenue
                ,SUM(pq_revenue) pq_revenue
                ,SUM(cq_revenue_qtd_prior) cq_revenue_qtd_prior
                ,SUM(lyq_revenue) lyq_revenue
                ,SUM(l2yq_revenue) l2yq_revenue
                ,SUM(lyq_revenue_qtd) lyq_revenue_qtd
                ,SUM(l2yq_revenue_qtd) l2yq_revenue_qtd
                ,SUM(lyq_revenue_qtd_prior) lyq_revenue_qtd_prior
                ,SUM(l2yq_revenue_qtd_prior) l2yq_revenue_qtd_prior
                ,SUM(pq_revenue_qtd) pq_revenue_qtd
                ,SUM(L7d_revenue) L7d_revenue
                ,SUM(L7d_revenue_prior) L7d_revenue_prior
                ,SUM(L7d_avg_revenue) L7d_avg_revenue
                ,SUM(L7d_avg_revenue_prior) L7d_avg_revenue_prior
                -- Optimal
                ,SUM(cq_optimal) cq_optimal
                ,SUM(pq_optimal) pq_optimal
                ,SUM(cq_optimal_qtd_prior) cq_optimal_qtd_prior
                ,SUM(lyq_optimal) lyq_optimal
                ,SUM(lyq_optimal_qtd) lyq_optimal_qtd
                ,SUM(lyq_optimal_qtd_prior) lyq_optimal_qtd_prior
                ,SUM(pq_optimal_qtd) pq_optimal_qtd
                ,SUM(l7d_optimal) l7d_optimal
                ,SUM(l7d_optimal_prior) l7d_optimal_prior
                ,SUM(l7d_avg_optimal) l7d_avg_optimal
                ,SUM(l7d_avg_optimal_prior) l7d_avg_optimal_prior
                ,SUM(l7d_avg_ly_optimal) l7d_avg_ly_optimal
                ,SUM(l7d_avg_ly_optimal_prior) l7d_avg_ly_optimal_prior
                ,SUM(l7d_ly_optimal) l7d_ly_optimal
                ,SUM(l7d_ly_optimal_prior) l7d_ly_optimal_prior
                ,SUM(optimal_quota) optimal_quota
                ,SUM(agc_optimal_quota) agc_optimal_quota
                ,SUM(dr_resilience_goal) dr_resilience_goal
                ,SUM(dr_resilient_cq) dr_resilient_cq
                ,SUM(dr_resilient_pq) dr_resilient_pq
                ,SUM(dr_resilient_ly) dr_resilient_ly
                ,SUM(dr_resilient_lyq) dr_resilient_lyq
                ,SUM(cq_dr_resilient_qtd_prior) cq_dr_resilient_qtd_prior
                ,SUM(lyq_dr_resilient) lyq_dr_resilient
                ,SUM(lyq_dr_resilient_qtd) lyq_dr_resilient_qtd
                ,SUM(lyq_dr_resilient_qtd_prior) lyq_dr_resilient_qtd_prior
                ,SUM(pq_dr_resilient_qtd) pq_dr_resilient_qtd
                ,SUM(l7d_dr_resilient) l7d_dr_resilient
                ,SUM(l7d_dr_resilient_prior) l7d_dr_resilient_prior
                ,SUM(l7d_avg_dr_resilient) l7d_avg_dr_resilient
                ,SUM(l7d_avg_dr_resilient_prior) l7d_avg_dr_resilient_prior
                ,SUM(l7d_avg_ly_dr_resilient) l7d_avg_ly_dr_resilient
                ,SUM(l7d_avg_ly_dr_resilient_prior) l7d_avg_ly_dr_resilient_prior
                ,SUM(l7d_ly_dr_resilient) l7d_ly_dr_resilient
                ,SUM(l7d_ly_dr_resilient_prior) l7d_ly_dr_resilient_prior
                ,SUM(dr_resilient_quota) dr_resilient_quota
                ,SUM(cq_product_resilient_rec_rev) cq_product_resilient_rec_rev
                ,SUM(cq_ebr_usd_rec_rev) cq_ebr_usd_rec_rev
                ,SUM(cq_capi_ebr_revenue) cq_capi_ebr_revenue
                ,SUM(pq_product_resilient_rec_rev) pq_product_resilient_rec_rev
                ,SUM(pq_ebr_usd_rec_rev) pq_ebr_usd_rec_rev
                ,SUM(pq_capi_ebr_revenue) pq_capi_ebr_revenue
                ,SUM(ly_product_resilient_rec_rev) ly_product_resilient_rec_rev
                ,SUM(ly_ebr_usd_rec_rev) ly_ebr_usd_rec_rev
                ,SUM(ly_capi_ebr_revenue) ly_capi_ebr_revenue
                ,SUM(lyq_product_resilient_rec_rev) lyq_product_resilient_rec_rev
                ,SUM(lyq_ebr_usd_rec_rev) lyq_ebr_usd_rec_rev
                ,SUM(lyq_capi_ebr_revenue) lyq_capi_ebr_revenue
                ,SUM(cq_product_resilient_rec_rev_qtd_prior) cq_product_resilient_rec_rev_qtd_prior
                ,SUM(cq_ebr_usd_rec_rev_qtd_prior) cq_ebr_usd_rec_rev_qtd_prior
                ,SUM(cq_capi_ebr_revenue_qtd_prior) cq_capi_ebr_revenue_qtd_prior
                ,SUM(lyq_product_resilient_rec_rev_qtd) lyq_product_resilient_rec_rev_qtd
                ,SUM(lyq_ebr_usd_rec_rev_qtd) lyq_ebr_usd_rec_rev_qtd
                ,SUM(lyq_capi_ebr_revenue_qtd) lyq_capi_ebr_revenue_qtd
                ,SUM(l7d_product_resilient_rec_rev) l7d_product_resilient_rec_rev
                ,SUM(l7d_ebr_usd_rec_rev) l7d_ebr_usd_rec_rev
                ,SUM(l7d_capi_ebr_revenue) l7d_capi_ebr_revenue
                ,SUM(l7d_product_resilient_rec_rev_prior) l7d_product_resilient_rec_rev_prior
                ,SUM(l7d_ebr_usd_rec_rev_prior) l7d_ebr_usd_rec_rev_prior
                ,SUM(l7d_capi_ebr_revenue_prior) l7d_capi_ebr_revenue_prior
                ,SUM(l7d_avg_product_resilient_rec_rev) l7d_avg_product_resilient_rec_rev
                ,SUM(l7d_avg_ebr_usd_rec_rev) l7d_avg_ebr_usd_rec_rev
                ,SUM(l7d_avg_capi_ebr_revenue) l7d_avg_capi_ebr_revenue
                ,SUM(l7d_avg_product_resilient_rec_rev_prior) l7d_avg_product_resilient_rec_rev_prior
                ,SUM(l7d_avg_ebr_usd_rec_rev_prior) l7d_avg_ebr_usd_rec_rev_prior
                ,SUM(l7d_avg_capi_ebr_revenue_prior) l7d_avg_capi_ebr_revenue_prior
                ,SUM(l7d_avg_ly_product_resilient_rec_rev) l7d_avg_ly_product_resilient_rec_rev
                ,SUM(l7d_avg_ly_ebr_usd_rec_rev) l7d_avg_ly_ebr_usd_rec_rev
                ,SUM(l7d_avg_ly_capi_ebr_revenue) l7d_avg_ly_capi_ebr_revenue
                ,SUM(l7d_avg_ly_product_resilient_rec_rev_prior) l7d_avg_ly_product_resilient_rec_rev_prior
                ,SUM(l7d_avg_ly_ebr_usd_rec_rev_prior) l7d_avg_ly_ebr_usd_rec_rev_prior
                ,SUM(l7d_avg_ly_capi_ebr_revenue_prior) l7d_avg_ly_capi_ebr_revenue_prior
                ,SUM(l7d_ly_product_resilient_rec_rev) l7d_ly_product_resilient_rec_rev
                ,SUM(l7d_ly_ebr_usd_rec_rev) l7d_ly_ebr_usd_rec_rev
                ,SUM(l7d_ly_capi_ebr_revenue)l7d_ly_capi_ebr_revenue
                ,SUM(l7d_ly_product_resilient_rec_rev_prior) l7d_ly_product_resilient_rec_rev_prior
                ,SUM(l7d_ly_ebr_usd_rec_rev_prior) l7d_ly_ebr_usd_rec_rev_prior
                ,SUM(l7d_ly_capi_ebr_revenue_prior) l7d_ly_capi_ebr_revenue_prior
                ,SUM(L14d_revenue) L14d_revenue
                ,SUM(L14d_revenue_prior) L14d_revenue_prior
                ,SUM(L14d_avg_revenue) L14d_avg_revenue
                ,SUM(L14d_avg_revenue_prior) L14d_avg_revenue_prior
                ,SUM(sales_forecast_prior_2w) sales_forecast_prior_2w
                ,quota_and_fcast.ds
                ,'Advertiser' route_to_market

            from quota_and_fcast

            left join am_goals on
    am_goals.territory_l12_name = quota_and_fcast.l12_reporting_territory and
    am_goals.client_fbid = cast(quota_and_fcast.client_fbid as bigint) and
    am_goals.ultimate_client_fbid = cast(quota_and_fcast.ultimate_client_fbid as bigint)


            group by
            quota_and_fcast.ultimate_client_fbid
            ,quota_and_fcast.ultimate_client_sfid
            ,quota_and_fcast.ultimate_client_name
            ,quota_and_fcast.client_name
            ,quota_and_fcast.client_fbid
            ,quota_and_fcast.client_sfid
            ,revenue_segment
            ,cast(quarter_id as varchar)
            ,cast(next_quarter_id as varchar)
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
            ,l12_reporting_territory
            ,l10_reporting_territory
            ,l8_reporting_territory
            ,l6_reporting_territory
            ,l4_reporting_territory
            ,l2_reporting_territory
            ,quota_and_fcast.ds

            UNION ALL

Select

             am_goals_no_rev.ultimate_client_fbid
            ,am_goals_no_rev.ultimate_client_sfid
            ,am_goals_no_rev.ultimate_client_name
            ,am_goals_no_rev.client_name
            ,am_goals_no_rev.client_fbid
            ,am_goals_no_rev.client_sfid
            ,am_goals_no_rev.revenue_segment
            ,am_goals_no_rev.asofdate
            ,am_goals_no_rev.quarter_id
            ,am_goals_no_rev.next_quarter_id
            ,am_goals_no_rev.days_left_in_quarter
            ,am_goals_no_rev.days_left_in_quarter_prior
            ,am_goals_no_rev.days_total_in_quarter
            ,am_goals_no_rev.days_closed_in_quarter
            ,am_goals_no_rev.l12_reporting_territory
            ,am_goals_no_rev.l10_reporting_territory
            ,am_goals_no_rev.l8_reporting_territory
            ,am_goals_no_rev.l6_reporting_territory
            ,am_goals_no_rev.l4_reporting_territory
            ,am_goals_no_rev.l2_reporting_territory
                ,run_rate_forecast
                ,run_rate_forecast_prior
                ,am_goals_no_rev.quota quota
                ,sales_forecast
                ,sales_forecast_prior
                ,reseller_quota
                ,agency_quota
                --Revenue
                ,cq_revenue
                ,pq_revenue
                ,cq_revenue_qtd_prior
                ,lyq_revenue
                ,l2yq_revenue
                ,lyq_revenue_qtd
                ,l2yq_revenue_qtd
                ,lyq_revenue_qtd_prior
                ,l2yq_revenue_qtd_prior
                ,pq_revenue_qtd
                ,L7d_revenue
                ,L7d_revenue_prior
                ,L7d_avg_revenue
                ,L7d_avg_revenue_prior
                -- Optimal
                ,cq_optimal
                ,pq_optimal
                ,cq_optimal_qtd_prior
                ,lyq_optimal
                ,lyq_optimal_qtd
                ,lyq_optimal_qtd_prior
                ,pq_optimal_qtd
                ,l7d_optimal
                ,l7d_optimal_prior
                ,l7d_avg_optimal
                ,l7d_avg_optimal_prior
                ,l7d_avg_ly_optimal
                ,l7d_avg_ly_optimal_prior
                ,l7d_ly_optimal
                ,l7d_ly_optimal_prior
                ,optimal_quota
                ,agc_optimal_quota
                ,dr_resilience_goal
                ,dr_resilient_cq
                ,dr_resilient_pq
                ,dr_resilient_ly
                ,dr_resilient_lyq
                ,cq_dr_resilient_qtd_prior
                ,lyq_dr_resilient
                ,lyq_dr_resilient_qtd
                ,lyq_dr_resilient_qtd_prior
                ,pq_dr_resilient_qtd
                ,l7d_dr_resilient
                ,l7d_dr_resilient_prior
                ,l7d_avg_dr_resilient
                ,l7d_avg_dr_resilient_prior
                ,l7d_avg_ly_dr_resilient
                ,l7d_avg_ly_dr_resilient_prior
                ,l7d_ly_dr_resilient
                ,l7d_ly_dr_resilient_prior
                ,dr_resilient_quota
                ,cq_product_resilient_rec_rev
                ,cq_ebr_usd_rec_rev
                ,cq_capi_ebr_revenue
                ,pq_product_resilient_rec_rev
                ,pq_ebr_usd_rec_rev
                ,pq_capi_ebr_revenue
                ,ly_product_resilient_rec_rev
                ,ly_ebr_usd_rec_rev
                ,ly_capi_ebr_revenue
                ,lyq_product_resilient_rec_rev
                ,lyq_ebr_usd_rec_rev
                ,lyq_capi_ebr_revenue
                ,cq_product_resilient_rec_rev_qtd_prior
                ,cq_ebr_usd_rec_rev_qtd_prior
                ,cq_capi_ebr_revenue_qtd_prior
                ,lyq_product_resilient_rec_rev_qtd
                ,lyq_ebr_usd_rec_rev_qtd
                ,lyq_capi_ebr_revenue_qtd
                ,l7d_product_resilient_rec_rev
                ,l7d_ebr_usd_rec_rev
                ,l7d_capi_ebr_revenue
                ,l7d_product_resilient_rec_rev_prior
                ,l7d_ebr_usd_rec_rev_prior
                ,l7d_capi_ebr_revenue_prior
                ,l7d_avg_product_resilient_rec_rev
                ,l7d_avg_ebr_usd_rec_rev
                ,l7d_avg_capi_ebr_revenue
                ,l7d_avg_product_resilient_rec_rev_prior
                ,l7d_avg_ebr_usd_rec_rev_prior
                ,l7d_avg_capi_ebr_revenue_prior
                ,l7d_avg_ly_product_resilient_rec_rev
                ,l7d_avg_ly_ebr_usd_rec_rev
                ,l7d_avg_ly_capi_ebr_revenue
                ,l7d_avg_ly_product_resilient_rec_rev_prior
                ,l7d_avg_ly_ebr_usd_rec_rev_prior
                ,l7d_avg_ly_capi_ebr_revenue_prior
                ,l7d_ly_product_resilient_rec_rev
                ,l7d_ly_ebr_usd_rec_rev
                ,l7d_ly_capi_ebr_revenue
                ,l7d_ly_product_resilient_rec_rev_prior
                ,l7d_ly_ebr_usd_rec_rev_prior
                ,l7d_ly_capi_ebr_revenue_prior
                ,L14d_revenue
                ,L14d_revenue_prior
                ,L14d_avg_revenue
                ,L14d_avg_revenue_prior
                ,sales_forecast_prior_2w
                ,ds
                ,'Advertiser' route_to_market


from am_goals_no_rev

left join (
    Select
    l12_reporting_territory,
    client_fbid,
    ultimate_client_fbid,
    SUM(advertiser_quota) quota
    from quota_and_fcast
    group by     l12_reporting_territory,
    client_fbid,
    ultimate_client_fbid
    ) quota_and_fcast on

    am_goals_no_rev.l12_reporting_territory = quota_and_fcast.l12_reporting_territory and
    am_goals_no_rev.client_fbid = quota_and_fcast.client_fbid and
    am_goals_no_rev.ultimate_client_fbid = quota_and_fcast.ultimate_client_fbid

where quota_and_fcast.quota is null


    """,
)

insert_bpo_gms_quota_and_forecast_temp_agc = PrestoInsertOperator(
    dep_list=[
        wait_for_bpo_gms_quota_and_forecast_snapshot,
        wait_for_inc_dim_organization_to_brand_team_crm,
        wait_for_acdp_dim_l45_commission_split_v2,
        wait_for_acdp_dim_l45_organization,
        wait_for_l45_fct_account_team_target,
    ],
    table="<TABLE:bpo_gms_quota_and_forecast_temp>",
    partition="ds=<DATEID>/route_to_market=Agency",
    documentation={
        "description": """GMS Dashboard Logic without Row Level Security and only Advertiser View"""
    },
    create=r"""
CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_quota_and_forecast_temp> (
             ultimate_client_fbid varchar
            ,ultimate_client_sfid varchar
            ,ultimate_client_name varchar
            ,client_name varchar
            ,client_fbid varchar
            ,client_sfid varchar
            ,revenue_segment varchar
            ,asofdate varchar
            ,quarter_id varchar
            ,next_quarter_id varchar
            ,days_left_in_quarter bigint
            ,days_left_in_quarter_prior bigint
            ,days_total_in_quarter bigint
            ,days_closed_in_quarter bigint
            ,l12_reporting_territory varchar
            ,l10_reporting_territory varchar
            ,l8_reporting_territory varchar
            ,l6_reporting_territory varchar
            ,l4_reporting_territory varchar
            ,l2_reporting_territory varchar
                , run_rate_forecast double
                , run_rate_forecast_prior double
                , quota double
                , sales_forecast double
                , sales_forecast_prior double
                , cq_revenue double
                , pq_revenue double
                , cq_revenue_qtd_prior double
                , lyq_revenue double
                , l2yq_revenue double
                , lyq_revenue_qtd double
                , l2yq_revenue_qtd double
                , lyq_revenue_qtd_prior double
                , l2yq_revenue_qtd_prior double
                , pq_revenue_qtd double
                , L7d_revenue double
                , L7d_revenue_prior double
                , L7d_avg_revenue double
                , L7d_avg_revenue_prior double
                , L14d_revenue double
                , L14d_revenue_prior double
                , L14d_avg_revenue double
                , L14d_avg_revenue_prior double
                , sales_forecast_prior_2w double
                , ds varchar
                , route_to_market varchar
)
WITH (
   format = 'DWRF',
   oncall = 'gms_central_analytics',
   partitioned_by = ARRAY['ds','route_to_market'],
   retention_days = 7,
   uii = false
)
    """,
    select=r"""
WITH quarter_date as (Select
cast('<quarter_id>' as date) as quarter_id,
(cast('<quarter_id>' as date) + INTERVAL '3' MONTH) next_quarter_id,
max(cast(date_id as date)) asofdate
from
bpo_gms_quota_and_forecast_snapshot
where cq_revenue is not null and ds = '<DATEID>'
group by 1,2)

,quarter_dates as (
Select
quarter_id,
next_quarter_id,
asofdate,
DATE_DIFF('day',(asofdate),next_quarter_id- INTERVAL '1' DAY) days_left_in_quarter,
DATE_DIFF('day',(asofdate - INTERVAL '7' DAY),next_quarter_id- INTERVAL '1' DAY) days_left_in_quarter_prior,
DATE_DIFF('day', quarter_id, next_quarter_id) days_total_in_quarter,
DATE_DIFF('day', quarter_id, asofdate + INTERVAL '1' DAY) days_closed_in_quarter
from
quarter_date),
am_goals as
(SELECT
    a.territory_l12_name,
    c.org_fbid client_fbid,
    c.ultimate_parent_org_fbid ultimate_client_fbid,
    a.ds,
    SUM(a.rec_rev_target) AS am_goal
FROM l45_fct_account_team_target:ad_reporting a
LEFT JOIN (
    SELECT DISTINCT
        a.employee_id,
        a.brand_team_id,
        a.commission_split_role_name,
        a.territory_id,
        b.id1 AS org_fbid,
        a.ds
    FROM acdp_dim_l45_commission_split_v2:ad_reporting a
    LEFT JOIN inc_dim_organization_to_brand_team_crm b
        ON a.brand_team_id = b.id2
        AND b.ds = '<DATEID>'
    WHERE
        a.ds = '<DATEID>'
        AND a.commission_split_status = 'active'
        AND '<DATEID>' BETWEEN a.commission_split_start_date AND COALESCE(
            a.commission_split_end_date,
            '2099-12-31'
        )
) b
    ON a.employee_fbid = b.employee_id
    AND a.account_team_fbid = b.brand_team_id
    AND a.territory_l12_id = b.territory_id
LEFT JOIN acdp_dim_l45_organization:ad_reporting c
    ON b.org_fbid = c.org_fbid
    AND c.ds = '<DATEID>'
WHERE
    a.ds = '<DATEID>'
    AND a.quarter_id = CAST(DATE_TRUNC('QUARTER', CAST('<DATEID>' AS DATE)) AS VARCHAR)
    AND a.commission_type = 'SMB Account Strategist PM'
    and is_agency = 1
GROUP BY
    1, 2, 3,4 )
,quota_and_fcast as (

Select
 planning_agency_ult_fbid ultimate_client_fbid
,planning_agency_ult_sfid ultimate_client_sfid
,planning_agency_ult_name ultimate_client_name
,planning_agency_name client_name
,planning_agency_fbid client_fbid
,planning_agency_sfid client_sfid
,cast(asofdate as varchar) asofdate
,cast(quarter_id as varchar) quarter_id
,cast(next_quarter_id as varchar) next_quarter_id
,days_left_in_quarter
,days_left_in_quarter_prior
,days_total_in_quarter
,days_closed_in_quarter
,l12_agency_territory
,l10_agency_territory
,l8_agency_territory
,l6_agency_territory
,l4_agency_territory
,l2_agency_territory
,program_agency revenue_segment
,SUM(cq_revenue) + (SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_revenue else 0 end)/7 * days_left_in_quarter) run_rate_forecast

,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_revenue else 0 end) + (SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY)
and cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_revenue else 0 end)/7 * days_left_in_quarter_prior) run_rate_forecast_prior

,(SUM(case when date_id = '<quarter_id>' then advertiser_quota else 0 end) - SUM(cq_revenue)) / days_left_in_quarter as required_run_rate


,SUM(case when date_id = '<quarter_id>' then advertiser_quota else 0 end) advertiser_quota
,SUM(case when date_id = '<quarter_id>' then sales_forecast else 0 end)  sales_forecast
,SUM(case when date_id = '<quarter_id>' then sales_forecast_prior else 0 end)  sales_forecast_prior
,SUM(CASE WHEN date_id = '<quarter_id>' THEN reseller_quota ELSE 0 END) reseller_quota
,SUM(CASE WHEN date_id = '<quarter_id>' THEN agency_quota ELSE 0 END) agency_quota
--Revenue
,SUM(cq_revenue) cq_revenue
,SUM(pq_revenue) pq_revenue
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_revenue else 0 end) cq_revenue_qtd_prior
,Sum(ly_revenue) lyq_revenue
,Sum(l2y_revenue) l2yq_revenue
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_revenue else 0 end) lyq_revenue_qtd
,Sum(case when cast(date_id as date) <= (asofdate)
then l2y_revenue else 0 end) l2yq_revenue_qtd
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_revenue else 0 end) lyq_revenue_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then l2y_revenue else 0 end) l2yq_revenue_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate)
then pq_revenue else 0 end) pq_revenue_qtd
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_revenue else null end) L7d_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_revenue else null end) L7d_revenue_prior

,

SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_revenue else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_revenue else null end) / days_closed_in_quarter) end) L7d_avg_revenue,

SUM(case when days_closed_in_quarter >= 14 then (case when date_id >= '<DATEID-13>'
and date_id <= '<DATEID-7>' then cq_revenue else null end) / 7 else
(case when date_id >= '<DATEID-13>' and date_id <= '<DATEID-7>'
then cq_revenue else null end) / (days_closed_in_quarter-7) end) L7d_avg_revenue_prior ,


SUM(case when days_closed_in_quarter >= 14 then ((case when date_id >= '<DATEID-13>'
then cq_revenue else null end) / 14) else
((case when date_id >= '<DATEID>' then
cq_revenue else null end) / days_closed_in_quarter) end) L14d_avg_revenue,

SUM(case when days_closed_in_quarter >= 28 then (case when date_id >= '<DATEID-27>'
and date_id < '<DATEID-14>' then cq_revenue else null end) / 14 else
(case when date_id >= '<DATEID-20>' and date_id < '<DATEID-14>'
then cq_revenue else null end) / (days_closed_in_quarter-14) end) L14d_avg_revenue_prior ,

SUM(case when days_closed_in_quarter >= 28 then ((case when date_id >= '<DATEID-27>'
then cq_revenue else null end) / 28) else
((case when date_id >= '<DATEID>' then
cq_revenue else null end) / days_closed_in_quarter) end) L28d_avg_revenue,

SUM(case when days_closed_in_quarter >= 56 then (case when date_id >= '<DATEID-55>'
and date_id < '<DATEID-28>' then cq_revenue else null end) / 28 else
(case when date_id >= '<DATEID-55>' and date_id < '<DATEID-28>'
then cq_revenue else null end) / (days_closed_in_quarter-28) end) L28d_avg_revenue_prior

,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_revenue else null end)/7 L7d_avg_ly_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_revenue else null end)/7 L7d_avg_ly_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_revenue else null end) L7d_ly_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_revenue else null end) L7d_ly_revenue_prior
,SUM(cq_optimal) cq_optimal
,SUM(pq_optimal) pq_optimal
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_optimal else 0 end) cq_optimal_qtd_prior
,Sum(ly_optimal) lyq_optimal
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_optimal else 0 end) lyq_optimal_qtd
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_optimal else 0 end) lyq_optimal_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate)
then pq_optimal else 0 end) pq_optimal_qtd
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_revenue else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_optimal else null end) / days_closed_in_quarter) end) L7d_optimal
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_optimal else null end) L7d_optimal_prior
    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_optimal else null end)/7 L7d_avg_optimal
    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_optimal else null end)/7 L7d_avg_optimal_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_optimal else null end)/7 L7d_avg_ly_optimal
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_optimal else null end)/7 L7d_avg_ly_optimal_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_optimal else null end) L7d_ly_optimal
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_optimal else null end) L7d_ly_optimal_prior
,SUM(CASE WHEN date_id = '<quarter_id>' THEN optimal_quota ELSE 0 END) optimal_quota
,SUM(CASE WHEN date_id = '<quarter_id>' THEN agc_optimal_quota ELSE 0 END) agc_optimal_quota
,'<DATEID>' as ds
,SUM(dr_resilient_cq) dr_resilient_cq
,SUM(dr_resilient_pq) dr_resilient_pq
,SUM(dr_resilient_ly) dr_resilient_ly
,SUM(dr_resilient_lyq) dr_resilient_lyq
,SUM(CASE WHEN date_id = '<quarter_id>' THEN dr_resilience_goal ELSE 0 END) dr_resilience_goal
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_cq else 0 end) cq_dr_resilient_qtd_prior
,Sum(dr_resilient_ly) lyq_dr_resilient
,Sum(case when cast(date_id as date) <= (asofdate)
then dr_resilient_ly else 0 end) lyq_dr_resilient_qtd
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_ly else 0 end) lyq_dr_resilient_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate)
then dr_resilient_pq else 0 end) pq_dr_resilient_qtd
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then dr_resilient_cq else null end) / 7) else
((case when date_id >= '<DATEID>' then
dr_resilient_cq else null end) / days_closed_in_quarter) end) L7d_dr_resilient
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_cq else null end) L7d_dr_resilient_prior
    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then dr_resilient_cq else null end)/7 L7d_avg_dr_resilient
    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_cq else null end)/7 L7d_avg_dr_resilient_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then dr_resilient_ly else null end)/7 L7d_avg_ly_dr_resilient
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_ly else null end)/7 L7d_avg_ly_dr_resilient_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then dr_resilient_ly else null end) L7d_ly_dr_resilient
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then dr_resilient_ly else null end) L7d_ly_dr_resilient_prior
,SUM(CASE WHEN date_id = '<quarter_id>' THEN dr_resilience_goal ELSE 0 END) dr_resilient_quota
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_product_resilient_rec_rev else 0 end) cq_product_resilient_rec_rev_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_ebr_usd_rec_rev else 0 end) cq_ebr_usd_rec_rev_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_capi_ebr_revenue else 0 end) cq_capi_ebr_revenue_qtd_prior
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_product_resilient_rec_rev else 0 end) lyq_product_resilient_rec_rev_qtd
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_ebr_usd_rec_rev else 0 end) lyq_ebr_usd_rec_rev_qtd
,Sum(case when cast(date_id as date) <= (asofdate)
then ly_capi_ebr_revenue else 0 end) lyq_capi_ebr_revenue_qtd
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_product_resilient_rec_rev else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_product_resilient_rec_rev else null end) / days_closed_in_quarter) end) L7d_product_resilient_rec_rev
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_ebr_usd_rec_rev else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_ebr_usd_rec_rev else null end) / days_closed_in_quarter) end) L7d_ebr_usd_rec_rev
,SUM(case when days_closed_in_quarter >= 7 then ((case when date_id >= '<DATEID-6>'
then cq_capi_ebr_revenue else null end) / 7) else
((case when date_id >= '<DATEID>' then
cq_capi_ebr_revenue else null end) / days_closed_in_quarter) end) L7d_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_product_resilient_rec_rev else null end) L7d_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_ebr_usd_rec_rev else null end) L7d_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_capi_ebr_revenue else null end) L7d_capi_ebr_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_product_resilient_rec_rev else null end)/7 L7d_avg_product_resilient_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_ebr_usd_rec_rev else null end)/7 L7d_avg_ebr_usd_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
then cq_capi_ebr_revenue else null end)/7 L7d_avg_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_product_resilient_rec_rev else null end)/7 l7d_avg_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_ebr_usd_rec_rev else null end)/7 l7d_avg_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then cq_capi_ebr_revenue else null end)/7 l7d_avg_capi_ebr_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_product_resilient_rec_rev else null end)/7 l7d_avg_ly_product_resilient_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_ebr_usd_rec_rev else null end)/7 l7d_avg_ly_ebr_usd_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_capi_ebr_revenue else null end)/7 l7d_avg_ly_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_product_resilient_rec_rev else null end)/7 L7d_avg_ly_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_ebr_usd_rec_rev else null end)/7 L7d_avg_ly_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_capi_ebr_revenue else null end)/7 L7d_avg_ly_capi_ebr_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_product_resilient_rec_rev else null end) L7d_ly_product_resilient_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_ebr_usd_rec_rev  else null end) L7d_ly_ebr_usd_rec_rev
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY) and
    cast(date_id as date) <= (asofdate)
then ly_capi_ebr_revenue else null end) L7d_ly_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_product_resilient_rec_rev else null end) L7d_ly_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_ebr_usd_rec_rev else null end) L7d_ly_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
then ly_capi_ebr_revenue else null end) L7d_ly_capi_ebr_revenue_prior
,SUM(cq_product_resilient_rec_rev) cq_product_resilient_rec_rev
,SUM(cq_ebr_usd_rec_rev) cq_ebr_usd_rec_rev
,SUM(cq_capi_ebr_revenue) cq_capi_ebr_revenue
,SUM(pq_product_resilient_rec_rev) pq_product_resilient_rec_rev
,SUM(pq_ebr_usd_rec_rev) pq_ebr_usd_rec_rev
,SUM(pq_capi_ebr_revenue) pq_capi_ebr_revenue
,SUM(ly_product_resilient_rec_rev) ly_product_resilient_rec_rev
,SUM(ly_ebr_usd_rec_rev) ly_ebr_usd_rec_rev
,SUM(ly_capi_ebr_revenue) ly_capi_ebr_revenue
,SUM(lyq_product_resilient_rec_rev) lyq_product_resilient_rec_rev
,SUM(lyq_ebr_usd_rec_rev) lyq_ebr_usd_rec_rev
,SUM(lyq_capi_ebr_revenue) lyq_capi_ebr_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '14' DAY)
then cq_revenue else null end) L14d_revenue
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_revenue else null end) L14d_revenue_prior
,SUM(sales_forecast_prior_2w) sales_forecast_prior_2w

         from bpo_gms_quota_and_forecast_snapshot

              cross join quarter_dates


            where ds = '<DATEID>'

            and (ap_username is not null or pm_username is not null)

            group by
            planning_agency_ult_fbid
            ,planning_agency_ult_sfid
            ,planning_agency_ult_name
            ,planning_agency_name
            ,planning_agency_fbid
            ,planning_agency_sfid
            ,program_agency
            ,cast(asofdate as varchar)
            ,cast(quarter_id as varchar)
            ,cast(next_quarter_id as varchar)
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
            ,l12_agency_territory
            ,l10_agency_territory
            ,l8_agency_territory
            ,l6_agency_territory
            ,l4_agency_territory
            ,l2_agency_territory
)
, am_goals_no_rev as (

Select

            cast(c.ultimate_parent_org_sfid as varchar) ultimate_client_sfid
            ,cast(c.ultimate_parent_org_fbid as varchar) ultimate_client_fbid
            ,ultimate_parent_org_name ultimate_client_name
            ,c.parent_org_name client_name
            ,cast(c.org_fbid as varchar) client_fbid
            ,cast(c.org_fbid as varchar) client_sfid
            ,'GBG Scaled Agency' revenue_segment
            ,cast(asofdate as varchar) asofdate
            ,cast(quarter_dates.quarter_id as varchar) quarter_id
            ,cast(next_quarter_id as varchar) next_quarter_id
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
            ,a.territory_l12_name l12_reporting_territory
            ,a.territory_l10_name l10_reporting_territory
            ,a.territory_l8_name l8_reporting_territory
            ,a.territory_l6_name l6_reporting_territory
            ,a.territory_l4_name l4_reporting_territory
            ,a.territory_l2_name l2_reporting_territory
                ,SUM(0) run_rate_forecast
                ,SUM(0) run_rate_forecast_prior
                ,SUM(a.rec_rev_target) quota
                ,SUM(0) sales_forecast
                ,SUM(0) sales_forecast_prior
                ,SUM(0) reseller_quota
                ,SUM(0) agency_quota
                --Revenue
                ,SUM(0) cq_revenue
                ,SUM(0) pq_revenue
                ,SUM(0) cq_revenue_qtd_prior
                ,SUM(0) lyq_revenue
                ,SUM(0) l2yq_revenue
                ,SUM(0) lyq_revenue_qtd
                ,SUM(0) l2yq_revenue_qtd
                ,SUM(0) lyq_revenue_qtd_prior
                ,SUM(0) l2yq_revenue_qtd_prior
                ,SUM(0) pq_revenue_qtd
                ,SUM(0) L7d_revenue
                ,SUM(0) L7d_revenue_prior
                ,SUM(0) L7d_avg_revenue
                ,SUM(0) L7d_avg_revenue_prior
                -- Optimal
                ,SUM(0) cq_optimal
                ,SUM(0) pq_optimal
                ,SUM(0) cq_optimal_qtd_prior
                ,SUM(0) lyq_optimal
                ,SUM(0) lyq_optimal_qtd
                ,SUM(0) lyq_optimal_qtd_prior
                ,SUM(0) pq_optimal_qtd
                ,SUM(0) l7d_optimal
                ,SUM(0) l7d_optimal_prior
                ,SUM(0) l7d_avg_optimal
                ,SUM(0) l7d_avg_optimal_prior
                ,SUM(0) l7d_avg_ly_optimal
                ,SUM(0) l7d_avg_ly_optimal_prior
                ,SUM(0) l7d_ly_optimal
                ,SUM(0) l7d_ly_optimal_prior
                ,SUM(0) optimal_quota
                ,SUM(0) agc_optimal_quota
                ,SUM(0) dr_resilience_goal
                ,SUM(0) dr_resilient_cq
                ,SUM(0) dr_resilient_pq
                ,SUM(0) dr_resilient_ly
                ,SUM(0) dr_resilient_lyq
                ,SUM(0) cq_dr_resilient_qtd_prior
                ,SUM(0) lyq_dr_resilient
                ,SUM(0) lyq_dr_resilient_qtd
                ,SUM(0) lyq_dr_resilient_qtd_prior
                ,SUM(0) pq_dr_resilient_qtd
                ,SUM(0) l7d_dr_resilient
                ,SUM(0) l7d_dr_resilient_prior
                ,SUM(0) l7d_avg_dr_resilient
                ,SUM(0) l7d_avg_dr_resilient_prior
                ,SUM(0) l7d_avg_ly_dr_resilient
                ,SUM(0) l7d_avg_ly_dr_resilient_prior
                ,SUM(0) l7d_ly_dr_resilient
                ,SUM(0) l7d_ly_dr_resilient_prior
                ,SUM(0) dr_resilient_quota
                ,SUM(0) cq_product_resilient_rec_rev
                ,SUM(0) cq_ebr_usd_rec_rev
                ,SUM(0) cq_capi_ebr_revenue
                ,SUM(0) pq_product_resilient_rec_rev
                ,SUM(0) pq_ebr_usd_rec_rev
                ,SUM(0) pq_capi_ebr_revenue
                ,SUM(0) ly_product_resilient_rec_rev
                ,SUM(0) ly_ebr_usd_rec_rev
                ,SUM(0) ly_capi_ebr_revenue
                ,SUM(0) lyq_product_resilient_rec_rev
                ,SUM(0) lyq_ebr_usd_rec_rev
                ,SUM(0) lyq_capi_ebr_revenue
                ,SUM(0) cq_product_resilient_rec_rev_qtd_prior
                ,SUM(0) cq_ebr_usd_rec_rev_qtd_prior
                ,SUM(0) cq_capi_ebr_revenue_qtd_prior
                ,SUM(0) lyq_product_resilient_rec_rev_qtd
                ,SUM(0) lyq_ebr_usd_rec_rev_qtd
                ,SUM(0) lyq_capi_ebr_revenue_qtd
                ,SUM(0) l7d_product_resilient_rec_rev
                ,SUM(0) l7d_ebr_usd_rec_rev
                ,SUM(0) l7d_capi_ebr_revenue
                ,SUM(0) l7d_product_resilient_rec_rev_prior
                ,SUM(0) l7d_ebr_usd_rec_rev_prior
                ,SUM(0) l7d_capi_ebr_revenue_prior
                ,SUM(0) l7d_avg_product_resilient_rec_rev
                ,SUM(0) l7d_avg_ebr_usd_rec_rev
                ,SUM(0) l7d_avg_capi_ebr_revenue
                ,SUM(0) l7d_avg_product_resilient_rec_rev_prior
                ,SUM(0) l7d_avg_ebr_usd_rec_rev_prior
                ,SUM(0) l7d_avg_capi_ebr_revenue_prior
                ,SUM(0) l7d_avg_ly_product_resilient_rec_rev
                ,SUM(0) l7d_avg_ly_ebr_usd_rec_rev
                ,SUM(0) l7d_avg_ly_capi_ebr_revenue
                ,SUM(0) l7d_avg_ly_product_resilient_rec_rev_prior
                ,SUM(0) l7d_avg_ly_ebr_usd_rec_rev_prior
                ,SUM(0) l7d_avg_ly_capi_ebr_revenue_prior
                ,SUM(0) l7d_ly_product_resilient_rec_rev
                ,SUM(0) l7d_ly_ebr_usd_rec_rev
                ,SUM(0)l7d_ly_capi_ebr_revenue
                ,SUM(0) l7d_ly_product_resilient_rec_rev_prior
                ,SUM(0) l7d_ly_ebr_usd_rec_rev_prior
                ,SUM(0) l7d_ly_capi_ebr_revenue_prior
                ,SUM(0) L14d_revenue
                ,SUM(0) L14d_revenue_prior
                ,SUM(0) L14d_avg_revenue
                ,SUM(0) L14d_avg_revenue_prior
                ,SUM(0) sales_forecast_prior_2w
                            ,a.ds ds

FROM l45_fct_account_team_target:ad_reporting a
LEFT JOIN (
    SELECT DISTINCT
        a.employee_id,
        a.brand_team_id,
        a.commission_split_role_name,
        a.territory_id,
        b.id1 AS org_fbid,
        a.ds
    FROM acdp_dim_l45_commission_split_v2:ad_reporting a
    LEFT JOIN inc_dim_organization_to_brand_team_crm b
        ON a.brand_team_id = b.id2
        AND b.ds = '<DATEID>'
    WHERE
        a.ds = '<DATEID>'
        AND a.commission_split_status = 'active'
        AND '<DATEID>' BETWEEN a.commission_split_start_date AND COALESCE(
            a.commission_split_end_date,
            '2099-12-31'
        )
) b
    ON a.employee_fbid = b.employee_id
    AND a.account_team_fbid = b.brand_team_id
    AND a.territory_l12_id = b.territory_id
LEFT JOIN acdp_dim_l45_organization:ad_reporting c
CROSS JOIN quarter_dates
    ON b.org_fbid = c.org_fbid
    AND c.ds = '<DATEID>'
WHERE
    a.ds = '<DATEID>'
    AND a.quarter_id = CAST(DATE_TRUNC('QUARTER', CAST('<DATEID>' AS DATE)) AS VARCHAR)
    AND a.commission_type = 'SMB Account Strategist PM'
    and is_agency = 1

group by

            cast(c.ultimate_parent_org_sfid as varchar)
            ,cast(c.ultimate_parent_org_fbid as varchar)
            ,ultimate_parent_org_name
            ,c.parent_org_name
            ,cast(c.org_fbid as varchar)
            ,cast(c.org_fbid as varchar)
            ,cast(asofdate as varchar)
            ,cast(quarter_dates.quarter_id as varchar)
            ,cast(next_quarter_id as varchar)
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
            ,a.territory_l12_name
            ,a.territory_l10_name
            ,a.territory_l8_name
            ,a.territory_l6_name
            ,a.territory_l4_name
            ,a.territory_l2_name
            ,a.ds

)

            SELECT
            quota_and_fcast.ultimate_client_fbid
            ,quota_and_fcast.ultimate_client_sfid
            ,quota_and_fcast.ultimate_client_name
            ,quota_and_fcast.client_name
            ,quota_and_fcast.client_fbid
            ,quota_and_fcast.client_sfid
            ,revenue_segment
            ,'<DATEID>' asofdate
            ,cast(quarter_id as varchar) quarter_id
            ,cast(next_quarter_id as varchar) next_quarter_id
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
            ,l12_agency_territory l12_reporting_territory
            ,l10_agency_territory l10_reporting_territory
            ,l8_agency_territory l8_reporting_territory
            ,l6_agency_territory l6_reporting_territory
            ,l4_agency_territory l4_reporting_territory
            ,l2_agency_territory l2_reporting_territory
                ,SUM(run_rate_forecast) run_rate_forecast
                ,SUM(run_rate_forecast_prior) run_rate_forecast_prior
                ,SUM(case when revenue_segment = 'GBG Scaled Agency' then am_goals.am_goal else agency_quota end) quota
                ,SUM(sales_forecast) sales_forecast
                ,SUM(sales_forecast_prior) sales_forecast_prior
                ,SUM(reseller_quota) reseller_quota
                ,SUM(agency_quota) agency_quota
                --Revenue
                ,SUM(cq_revenue) cq_revenue
                ,SUM(pq_revenue) pq_revenue
                ,SUM(cq_revenue_qtd_prior) cq_revenue_qtd_prior
                ,SUM(lyq_revenue) lyq_revenue
                ,SUM(l2yq_revenue) l2yq_revenue
                ,SUM(lyq_revenue_qtd) lyq_revenue_qtd
                ,SUM(l2yq_revenue_qtd) l2yq_revenue_qtd
                ,SUM(lyq_revenue_qtd_prior) lyq_revenue_qtd_prior
                ,SUM(l2yq_revenue_qtd_prior) l2yq_revenue_qtd_prior
                ,SUM(pq_revenue_qtd) pq_revenue_qtd
                ,SUM(L7d_revenue) L7d_revenue
                ,SUM(L7d_revenue_prior) L7d_revenue_prior
                ,SUM(L7d_avg_revenue) L7d_avg_revenue
                ,SUM(L7d_avg_revenue_prior) L7d_avg_revenue_prior
                -- Optimal
                ,SUM(cq_optimal) cq_optimal
                ,SUM(pq_optimal) pq_optimal
                ,SUM(cq_optimal_qtd_prior) cq_optimal_qtd_prior
                ,SUM(lyq_optimal) lyq_optimal
                ,SUM(lyq_optimal_qtd) lyq_optimal_qtd
                ,SUM(lyq_optimal_qtd_prior) lyq_optimal_qtd_prior
                ,SUM(pq_optimal_qtd) pq_optimal_qtd
                ,SUM(l7d_optimal) l7d_optimal
                ,SUM(l7d_optimal_prior) l7d_optimal_prior
                ,SUM(l7d_avg_optimal) l7d_avg_optimal
                ,SUM(l7d_avg_optimal_prior) l7d_avg_optimal_prior
                ,SUM(l7d_avg_ly_optimal) l7d_avg_ly_optimal
                ,SUM(l7d_avg_ly_optimal_prior) l7d_avg_ly_optimal_prior
                ,SUM(l7d_ly_optimal) l7d_ly_optimal
                ,SUM(l7d_ly_optimal_prior) l7d_ly_optimal_prior
                ,SUM(optimal_quota) optimal_quota
                ,SUM(agc_optimal_quota) agc_optimal_quota
                ,SUM(dr_resilience_goal) dr_resilience_goal
                ,SUM(dr_resilient_cq) dr_resilient_cq
                ,SUM(dr_resilient_pq) dr_resilient_pq
                ,SUM(dr_resilient_ly) dr_resilient_ly
                ,SUM(dr_resilient_lyq) dr_resilient_lyq
                ,SUM(cq_dr_resilient_qtd_prior) cq_dr_resilient_qtd_prior
                ,SUM(lyq_dr_resilient) lyq_dr_resilient
                ,SUM(lyq_dr_resilient_qtd) lyq_dr_resilient_qtd
                ,SUM(lyq_dr_resilient_qtd_prior) lyq_dr_resilient_qtd_prior
                ,SUM(pq_dr_resilient_qtd) pq_dr_resilient_qtd
                ,SUM(l7d_dr_resilient) l7d_dr_resilient
                ,SUM(l7d_dr_resilient_prior) l7d_dr_resilient_prior
                ,SUM(l7d_avg_dr_resilient) l7d_avg_dr_resilient
                ,SUM(l7d_avg_dr_resilient_prior) l7d_avg_dr_resilient_prior
                ,SUM(l7d_avg_ly_dr_resilient) l7d_avg_ly_dr_resilient
                ,SUM(l7d_avg_ly_dr_resilient_prior) l7d_avg_ly_dr_resilient_prior
                ,SUM(l7d_ly_dr_resilient) l7d_ly_dr_resilient
                ,SUM(l7d_ly_dr_resilient_prior) l7d_ly_dr_resilient_prior
                ,SUM(dr_resilient_quota) dr_resilient_quota
                ,SUM(cq_product_resilient_rec_rev) cq_product_resilient_rec_rev
                ,SUM(cq_ebr_usd_rec_rev) cq_ebr_usd_rec_rev
                ,SUM(cq_capi_ebr_revenue) cq_capi_ebr_revenue
                ,SUM(pq_product_resilient_rec_rev) pq_product_resilient_rec_rev
                ,SUM(pq_ebr_usd_rec_rev) pq_ebr_usd_rec_rev
                ,SUM(pq_capi_ebr_revenue) pq_capi_ebr_revenue
                ,SUM(ly_product_resilient_rec_rev) ly_product_resilient_rec_rev
                ,SUM(ly_ebr_usd_rec_rev) ly_ebr_usd_rec_rev
                ,SUM(ly_capi_ebr_revenue) ly_capi_ebr_revenue
                ,SUM(lyq_product_resilient_rec_rev) lyq_product_resilient_rec_rev
                ,SUM(lyq_ebr_usd_rec_rev) lyq_ebr_usd_rec_rev
                ,SUM(lyq_capi_ebr_revenue) lyq_capi_ebr_revenue
                ,SUM(cq_product_resilient_rec_rev_qtd_prior) cq_product_resilient_rec_rev_qtd_prior
                ,SUM(cq_ebr_usd_rec_rev_qtd_prior) cq_ebr_usd_rec_rev_qtd_prior
                ,SUM(cq_capi_ebr_revenue_qtd_prior) cq_capi_ebr_revenue_qtd_prior
                ,SUM(lyq_product_resilient_rec_rev_qtd) lyq_product_resilient_rec_rev_qtd
                ,SUM(lyq_ebr_usd_rec_rev_qtd) lyq_ebr_usd_rec_rev_qtd
                ,SUM(lyq_capi_ebr_revenue_qtd) lyq_capi_ebr_revenue_qtd
                ,SUM(l7d_product_resilient_rec_rev) l7d_product_resilient_rec_rev
                ,SUM(l7d_ebr_usd_rec_rev) l7d_ebr_usd_rec_rev
                ,SUM(l7d_capi_ebr_revenue) l7d_capi_ebr_revenue
                ,SUM(l7d_product_resilient_rec_rev_prior) l7d_product_resilient_rec_rev_prior
                ,SUM(l7d_ebr_usd_rec_rev_prior) l7d_ebr_usd_rec_rev_prior
                ,SUM(l7d_capi_ebr_revenue_prior) l7d_capi_ebr_revenue_prior
                ,SUM(l7d_avg_product_resilient_rec_rev) l7d_avg_product_resilient_rec_rev
                ,SUM(l7d_avg_ebr_usd_rec_rev) l7d_avg_ebr_usd_rec_rev
                ,SUM(l7d_avg_capi_ebr_revenue) l7d_avg_capi_ebr_revenue
                ,SUM(l7d_avg_product_resilient_rec_rev_prior) l7d_avg_product_resilient_rec_rev_prior
                ,SUM(l7d_avg_ebr_usd_rec_rev_prior) l7d_avg_ebr_usd_rec_rev_prior
                ,SUM(l7d_avg_capi_ebr_revenue_prior) l7d_avg_capi_ebr_revenue_prior
                ,SUM(l7d_avg_ly_product_resilient_rec_rev) l7d_avg_ly_product_resilient_rec_rev
                ,SUM(l7d_avg_ly_ebr_usd_rec_rev) l7d_avg_ly_ebr_usd_rec_rev
                ,SUM(l7d_avg_ly_capi_ebr_revenue) l7d_avg_ly_capi_ebr_revenue
                ,SUM(l7d_avg_ly_product_resilient_rec_rev_prior) l7d_avg_ly_product_resilient_rec_rev_prior
                ,SUM(l7d_avg_ly_ebr_usd_rec_rev_prior) l7d_avg_ly_ebr_usd_rec_rev_prior
                ,SUM(l7d_avg_ly_capi_ebr_revenue_prior) l7d_avg_ly_capi_ebr_revenue_prior
                ,SUM(l7d_ly_product_resilient_rec_rev) l7d_ly_product_resilient_rec_rev
                ,SUM(l7d_ly_ebr_usd_rec_rev) l7d_ly_ebr_usd_rec_rev
                ,SUM(l7d_ly_capi_ebr_revenue)l7d_ly_capi_ebr_revenue
                ,SUM(l7d_ly_product_resilient_rec_rev_prior) l7d_ly_product_resilient_rec_rev_prior
                ,SUM(l7d_ly_ebr_usd_rec_rev_prior) l7d_ly_ebr_usd_rec_rev_prior
                ,SUM(l7d_ly_capi_ebr_revenue_prior) l7d_ly_capi_ebr_revenue_prior
                ,SUM(L14d_revenue) L14d_revenue
                ,SUM(L14d_revenue_prior) L14d_revenue_prior
                ,SUM(L14d_avg_revenue) L14d_avg_revenue
                ,SUM(L14d_avg_revenue_prior) L14d_avg_revenue_prior
                ,SUM(sales_forecast_prior_2w) sales_forecast_prior_2w
                ,quota_and_fcast.ds
                ,'Agency' route_to_market

            from quota_and_fcast

            left join am_goals on
    am_goals.territory_l12_name =
    quota_and_fcast.l12_agency_territory and
    am_goals.client_fbid = cast(quota_and_fcast.client_fbid as bigint) and
    am_goals.ultimate_client_fbid = cast(quota_and_fcast.ultimate_client_fbid as bigint)


            group by
            quota_and_fcast.ultimate_client_fbid
            ,quota_and_fcast.ultimate_client_sfid
            ,quota_and_fcast.ultimate_client_name
            ,quota_and_fcast.client_name
            ,quota_and_fcast.client_fbid
            ,quota_and_fcast.client_sfid
            ,revenue_segment
            ,cast(quarter_id as varchar)
            ,cast(next_quarter_id as varchar)
            ,days_left_in_quarter
            ,days_left_in_quarter_prior
            ,days_total_in_quarter
            ,days_closed_in_quarter
            ,l12_agency_territory
            ,l10_agency_territory
            ,l8_agency_territory
            ,l6_agency_territory
            ,l4_agency_territory
            ,l2_agency_territory
            ,quota_and_fcast.ds

            UNION ALL

Select

             am_goals_no_rev.ultimate_client_fbid
            ,am_goals_no_rev.ultimate_client_sfid
            ,am_goals_no_rev.ultimate_client_name
            ,am_goals_no_rev.client_name
            ,am_goals_no_rev.client_fbid
            ,am_goals_no_rev.client_sfid
            ,am_goals_no_rev.revenue_segment
            ,am_goals_no_rev.asofdate
            ,am_goals_no_rev.quarter_id
            ,am_goals_no_rev.next_quarter_id
            ,am_goals_no_rev.days_left_in_quarter
            ,am_goals_no_rev.days_left_in_quarter_prior
            ,am_goals_no_rev.days_total_in_quarter
            ,am_goals_no_rev.days_closed_in_quarter
            ,am_goals_no_rev.l12_reporting_territory
            ,am_goals_no_rev.l10_reporting_territory
            ,am_goals_no_rev.l8_reporting_territory
            ,am_goals_no_rev.l6_reporting_territory
            ,am_goals_no_rev.l4_reporting_territory
            ,am_goals_no_rev.l2_reporting_territory
                ,run_rate_forecast
                ,run_rate_forecast_prior
                ,am_goals_no_rev.quota quota
                ,sales_forecast
                ,sales_forecast_prior
                ,reseller_quota
                ,agency_quota
                --Revenue
                ,cq_revenue
                ,pq_revenue
                ,cq_revenue_qtd_prior
                ,lyq_revenue
                ,l2yq_revenue
                ,lyq_revenue_qtd
                ,l2yq_revenue_qtd
                ,lyq_revenue_qtd_prior
                ,l2yq_revenue_qtd_prior
                ,pq_revenue_qtd
                ,L7d_revenue
                ,L7d_revenue_prior
                ,L7d_avg_revenue
                ,L7d_avg_revenue_prior
                -- Optimal
                ,cq_optimal
                ,pq_optimal
                ,cq_optimal_qtd_prior
                ,lyq_optimal
                ,lyq_optimal_qtd
                ,lyq_optimal_qtd_prior
                ,pq_optimal_qtd
                ,l7d_optimal
                ,l7d_optimal_prior
                ,l7d_avg_optimal
                ,l7d_avg_optimal_prior
                ,l7d_avg_ly_optimal
                ,l7d_avg_ly_optimal_prior
                ,l7d_ly_optimal
                ,l7d_ly_optimal_prior
                ,optimal_quota
                ,agc_optimal_quota
                ,dr_resilience_goal
                ,dr_resilient_cq
                ,dr_resilient_pq
                ,dr_resilient_ly
                ,dr_resilient_lyq
                ,cq_dr_resilient_qtd_prior
                ,lyq_dr_resilient
                ,lyq_dr_resilient_qtd
                ,lyq_dr_resilient_qtd_prior
                ,pq_dr_resilient_qtd
                ,l7d_dr_resilient
                ,l7d_dr_resilient_prior
                ,l7d_avg_dr_resilient
                ,l7d_avg_dr_resilient_prior
                ,l7d_avg_ly_dr_resilient
                ,l7d_avg_ly_dr_resilient_prior
                ,l7d_ly_dr_resilient
                ,l7d_ly_dr_resilient_prior
                ,dr_resilient_quota
                ,cq_product_resilient_rec_rev
                ,cq_ebr_usd_rec_rev
                ,cq_capi_ebr_revenue
                ,pq_product_resilient_rec_rev
                ,pq_ebr_usd_rec_rev
                ,pq_capi_ebr_revenue
                ,ly_product_resilient_rec_rev
                ,ly_ebr_usd_rec_rev
                ,ly_capi_ebr_revenue
                ,lyq_product_resilient_rec_rev
                ,lyq_ebr_usd_rec_rev
                ,lyq_capi_ebr_revenue
                ,cq_product_resilient_rec_rev_qtd_prior
                ,cq_ebr_usd_rec_rev_qtd_prior
                ,cq_capi_ebr_revenue_qtd_prior
                ,lyq_product_resilient_rec_rev_qtd
                ,lyq_ebr_usd_rec_rev_qtd
                ,lyq_capi_ebr_revenue_qtd
                ,l7d_product_resilient_rec_rev
                ,l7d_ebr_usd_rec_rev
                ,l7d_capi_ebr_revenue
                ,l7d_product_resilient_rec_rev_prior
                ,l7d_ebr_usd_rec_rev_prior
                ,l7d_capi_ebr_revenue_prior
                ,l7d_avg_product_resilient_rec_rev
                ,l7d_avg_ebr_usd_rec_rev
                ,l7d_avg_capi_ebr_revenue
                ,l7d_avg_product_resilient_rec_rev_prior
                ,l7d_avg_ebr_usd_rec_rev_prior
                ,l7d_avg_capi_ebr_revenue_prior
                ,l7d_avg_ly_product_resilient_rec_rev
                ,l7d_avg_ly_ebr_usd_rec_rev
                ,l7d_avg_ly_capi_ebr_revenue
                ,l7d_avg_ly_product_resilient_rec_rev_prior
                ,l7d_avg_ly_ebr_usd_rec_rev_prior
                ,l7d_avg_ly_capi_ebr_revenue_prior
                ,l7d_ly_product_resilient_rec_rev
                ,l7d_ly_ebr_usd_rec_rev
                ,l7d_ly_capi_ebr_revenue
                ,l7d_ly_product_resilient_rec_rev_prior
                ,l7d_ly_ebr_usd_rec_rev_prior
                ,l7d_ly_capi_ebr_revenue_prior
                ,L14d_revenue
                ,L14d_revenue_prior
                ,L14d_avg_revenue
                ,L14d_avg_revenue_prior
                ,sales_forecast_prior_2w
                ,ds
                ,'Agency' route_to_market


from am_goals_no_rev

left join (
    Select
    L12_agency_territory l12_reporting_territory,
    client_fbid,
    ultimate_client_fbid,
    SUM(agency_quota) quota
    from quota_and_fcast
    group by     L12_agency_territory,
    client_fbid,
    ultimate_client_fbid
    ) quota_and_fcast on

    am_goals_no_rev.l12_reporting_territory =
    quota_and_fcast.l12_reporting_territory and
    am_goals_no_rev.client_fbid = quota_and_fcast.client_fbid and
    am_goals_no_rev.ultimate_client_fbid = quota_and_fcast.ultimate_client_fbid

where quota_and_fcast.quota is null


    """,
)


create_view_bpo_gms_quota_and_forecast_temp = PrestoOperator(
    dep_list=[
        insert_bpo_gms_quota_and_forecast_temp_adv,
        insert_bpo_gms_quota_and_forecast_temp_agc,
    ],
    query=r"""
CREATE OR REPLACE VIEW <TABLE:bpo_gms_quota_and_forecast_temp_v> as

Select * from <TABLE:bpo_gms_quota_and_forecast_temp>

where ds = '<DATEID>'

""",
)


if is_test():
    pass
else:

    tableau_refresh_bpo_gms_quota_and_forecast_temp = TableauPublishOperator(
        dep_list=[
            create_view_bpo_gms_quota_and_forecast_temp,
        ],
        refresh_cfg_id=29667,
        num_retries=2,
    )
