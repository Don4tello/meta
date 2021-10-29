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

wait_for_bpo_coverage_asis_stg_3 = WaitForHiveOperator(
    dep_list=[],
    table="bpo_coverage_asis_stg_3",
    use_future=False,
    fail_on_future=False,
)

wait_for_d_employee_plus = WaitForHiveOperator(
    dep_list=[],
    table="d_employee_plus:hr",
    use_future=False,
    fail_on_future=False,
)

gbg_scaled_compensation = PrestoInsertOperator(
    dep_list=[
        wait_for_bpo_coverage_asis_stg_3,
        wait_for_d_employee_plus,
    ],
    table="<TABLE:gbg_scaled_compensation>",
    partition="ds=<DATEID>",
    documentation={
        "description": """Creating GBG Scaled Compensation Layout and Deduping Logic"""
    },
    create=r"""
CREATE TABLE IF NOT EXISTS <TABLE:gbg_scaled_compensation> (
   region varchar,
   overlap_region varchar,
   manager varchar,
   directs varchar,
   days_total_in_quarter bigint,
   days_closed_in_quarter bigint,
   cq_am_revenue double,
   cq_pm_revenue double,
   deduped_cq_revenue double,
   advertiser_quota double,
   agency_quota double,
   deduped_quota double,
   am_straightline_fcst double,
   pm_straightline_fcst double,
   deduped_straightline_fcst double,
   l2y_am_revenue double,
   l2y_pm_revenue double,
   deduped_l2y_revenue double,
   l2yq_am_revenue_qtd double,
   l2yq_pm_revenue_qtd double,
   l2yq_deduped_revenue_qtd double,
   l6_territory varchar,
   L6_overlap_territory varchar,
   overlap_manager varchar,
   L10_territory varchar,
   L10_overlap_territory varchar,
   regional_director varchar,
   regional_overlap_director varchar,
   overlap_directs varchar,
   L10_manager varchar,
   L8_manager varchar,
   overlap_L10_manager varchar,
   overlap_L8_manager varchar,
   l8_territory varchar,
   L8_overlap_territory varchar,
   ds varchar
)
WITH (
   format = 'DWRF',
   oncall = 'gms_central_analytics',
   partitioned_by = ARRAY['ds'],
   retention_days = 7,
   uii = false
)
    """,
    select=r"""

WITH quarter_date as
(Select cast('<quarter_id>' as date) as quarter_id,
(cast('<quarter_id>' as date) + INTERVAL '3' MONTH) next_quarter_id,
max(cast(date_id as date)) asofdate
from
bpo_coverage_asis_stg_3
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

emp AS (
        SELECT
            personal_fbid AS employee_fbid,
            employee_id,
            preferred_name,
            unix_username,
            CASE
                WHEN region_name IN ('US', 'Canada') THEN 'NA'
                ELSE region_name
            END AS employee_region,
            manager_employee_id
        FROM d_employee_plus:hr



    where ds = '<DATEID>'



    ),
    mgr AS (
        SELECT
            personal_fbid AS manager_fbid,
            employee_id AS manager_employee_id,
            manager_employee_id as manager_plus_employee_id

        FROM d_employee_plus:hr

    where ds = '<DATEID>'



    ),
    rep AS (
        SELECT
            emp.employee_fbid,
            emp.employee_id,
            emp.employee_region,
            mgr.manager_fbid,
            mgr.manager_employee_id,
            mgr.manager_plus_employee_id
        FROM emp
        LEFT JOIN mgr
            ON emp.manager_employee_id = mgr.manager_employee_id
    )
,
    mgr_plus as (

    SELECT
        rep.employee_fbid,
        rep.employee_id,
        rep.employee_region,
        rep.manager_fbid,
        rep.manager_employee_id,
        mgr.manager_employee_id AS manager_plus_fbid,
        mgr.manager_plus_employee_id AS manager_plus_employee_id


    from rep

        LEFT JOIN mgr
            ON rep.manager_plus_employee_id = mgr.manager_employee_id )

,
    mgr_plus_plus as (

    SELECT
        mgr_plus.employee_fbid,
        mgr_plus.employee_id,
        mgr_plus.employee_region,
        mgr_plus.manager_fbid,
        mgr_plus.manager_employee_id,
        mgr_plus.manager_plus_fbid,
        mgr_plus.manager_plus_employee_id,
        mgr.manager_employee_id AS manager_plus_plus_fbid,
        mgr.manager_plus_employee_id AS manager_plus_plus_employee_id


    from mgr_plus

        LEFT JOIN mgr
            ON mgr_plus.manager_plus_employee_id = mgr.manager_employee_id )

,mgr_plus_final as (

Select

        mgr_plus_plus.employee_region,
        a.unix_username employee_unix_name,
        b.unix_username manager_unix_name,
        c.unix_username manager_plus_unix_name,
        a.preferred_name directs,
        b.preferred_name manager,
        c.preferred_name manager_plus,
        d.preferred_name manager_plus_plus

from mgr_plus_plus

left join emp a on a.employee_id = mgr_plus_plus.employee_id
left join emp b on b.employee_id = mgr_plus_plus.manager_employee_id
left join emp c on c.employee_id = mgr_plus_plus.manager_plus_employee_id
left join emp d on d.employee_id = mgr_plus_plus.manager_plus_plus_employee_id


group by

        mgr_plus_plus.employee_region,
        a.unix_username ,
        b.unix_username ,
        c.unix_username ,
        a.preferred_name ,
        b.preferred_name ,
        c.preferred_name ,
        d.preferred_name

)
,
terr as (
                SELECT
                    user_name_mgr_6 unix_manager,
                    full_name_mgr_6 full_manager

                FROM sales_ops_forecast_territory_flat flat
                WHERE
                    flat.ds = '<DATEID>'
                    group by 1,2)
,
final_am as
(

Select
'am' rtm,
am_username username,
pm_username overlap_username,
days_total_in_quarter,
days_closed_in_quarter,
L4_reporting_territory as L4_territory,
L4_agency_territory as L4_overlap_territory,
L6_reporting_territory as L6_territory,
L6_agency_territory as L6_overlap_territory,
L8_reporting_territory as L8_territory,
L8_agency_territory as L8_overlap_territory,
L10_reporting_territory as L10_territory,
L10_agency_territory as L10_overlap_territory,
l6_reporting_terr_mgr as regional_director,
unix_manager as regional_overlap_director,
l8_manager_agency_territory as overlap_l8_manager,
l10_manager_agency_territory as overlap_l10_manager,
l8_reporting_terr_mgr as l8_manager,
l10_reporting_terr_mgr as l10_manager,
-- Revenue
Sum(case when am_username is not null then cq_revenue else null end) cq_am_revenue,
Sum(0) cq_pm_revenue,
Sum(cq_revenue) deduped_cq_revenue,

-- Quota
Sum(case when date_id = '<quarter_id>' and am_username is not null then advertiser_quota else null end) advertiser_quota,
Sum(0) agency_quota,
Sum(case when am_username is not null and pm_username is not null
and date_id = '<quarter_id>' then agency_quota else 0 end) deduped_quota,

-- Straightline Forecast
(Sum(case when am_username is not null then cq_revenue else null end) / max(days_closed_in_quarter)) * max(days_total_in_quarter) am_straightline_fcst,
(Sum(0)) pm_straightline_fcst,
(Sum(cq_revenue ) / max(days_closed_in_quarter)) * max(days_total_in_quarter) deduped_straightline_fcst,
-- Last 2 Year Revenue
Sum(case when am_username is not null then l2y_revenue else null end) l2y_am_revenue,
Sum(0) l2y_pm_revenue,
Sum(l2y_revenue) deduped_l2y_revenue,
Sum(case when cast(date_id as date) <= (asofdate) and am_username is not null
then l2y_revenue else 0 end) l2yq_am_revenue_qtd,
Sum(0) l2yq_pm_revenue_qtd,
Sum(case when cast(date_id as date) <= (asofdate)
then l2y_revenue else 0 end) l2yq_deduped_revenue_qtd

from bpo_coverage_asis_stg_3 coverage

    left join terr on l6_manager_agency_territory = full_manager

    cross join quarter_dates

    where ds = '<DATEID>' and (pm_username is not null or am_username is not null )

group by
am_username,
pm_username,
days_total_in_quarter,
days_closed_in_quarter,
L4_reporting_territory,
L4_agency_territory,
L6_reporting_territory,
L6_agency_territory,
L8_reporting_territory,
L8_agency_territory,
L10_reporting_territory,
L10_agency_territory,
l6_reporting_terr_mgr,
unix_manager,
l8_manager_agency_territory,
l10_manager_agency_territory,
l8_reporting_terr_mgr,
l10_reporting_terr_mgr,
ds

)
,

final_pm as
(

Select
'pm' rtm,
pm_username username,
am_username overlap_username,
days_total_in_quarter,
days_closed_in_quarter,
L4_agency_territory as L4_territory,
L4_reporting_territory as L4_overlap_territory,
L6_agency_territory as L6_territory,
L6_reporting_territory as L6_overlap_territory,
L8_agency_territory as L8_territory,
L8_reporting_territory as L8_overlap_territory,
L10_agency_territory as L10_territory,
L10_reporting_territory as L10_overlap_territory,
unix_manager as regional_director,
l6_reporting_terr_mgr as regional_overlap_director,
L8_reporting_terr_mgr as overlap_l8_manager,
L10_reporting_terr_mgr as overlap_l10_manager,
l8_manager_agency_territory as l8_manager,
l10_manager_agency_territory as l10_manager,
-- Revenue
Sum(0) cq_am_revenue,
Sum(case when pm_username is not null then cq_revenue else null end) cq_pm_revenue,
Sum(cq_revenue) deduped_cq_revenue,

-- Quota
Sum(0) advertiser_quota,
Sum(case when date_id = '<quarter_id>' and pm_username is not null then agency_quota else null end) agency_quota,
Sum(0) deduped_quota,

-- Straightline Forecast
(Sum(0)) am_straightline_fcst,
(Sum(case when pm_username is not null then cq_revenue else null end) / max(days_closed_in_quarter)) * max(days_total_in_quarter) pm_straightline_fcst,
(Sum(cq_revenue ) / max(days_closed_in_quarter)) * max(days_total_in_quarter) deduped_straightline_fcst,
-- Last 2 Year Revenue
Sum(0) l2y_am_revenue,
Sum(case when pm_username is not null then l2y_revenue else null end) l2y_pm_revenue,
Sum(l2y_revenue) deduped_l2y_revenue,
Sum(0) l2yq_am_revenue_qtd,
Sum(case when cast(date_id as date) <= (asofdate) and pm_username is not null
then l2y_revenue else 0 end) l2yq_pm_revenue_qtd,
Sum(case when cast(date_id as date) <= (asofdate)
then l2y_revenue else 0 end) l2yq_deduped_revenue_qtd

from bpo_coverage_asis_stg_3 coverage

    left join terr on l6_manager_agency_territory = full_manager


    cross join quarter_dates

    where ds = '<DATEID>' and (pm_username is not null or am_username is not null )

group by
pm_username,
am_username,
days_total_in_quarter,
days_closed_in_quarter,
L4_reporting_territory,
L4_agency_territory,
L6_agency_territory,
L6_reporting_territory,
L8_agency_territory,
L8_reporting_territory,
L10_agency_territory,
L10_reporting_territory,
unix_manager,
l6_reporting_terr_mgr,
l8_reporting_terr_mgr,
l10_reporting_terr_mgr,
l8_manager_agency_territory,
l10_manager_agency_territory,
ds

)

Select
rtm,
L10_territory,
L10_overlap_territory,
l6_territory,
L6_overlap_territory,
l8_territory,
L8_overlap_territory,
L4_territory region,
L4_overlap_territory overlap_region,
(am.manager) manager,
(pm.manager) overlap_manager,
(am.directs) directs,
(pm.directs) overlap_directs,
days_total_in_quarter,
days_closed_in_quarter,
regional_director,
regional_overlap_director,
overlap_l8_manager,
overlap_l10_manager,
l8_manager,
l10_manager,
-- Revenue
cq_am_revenue,
cq_pm_revenue,
deduped_cq_revenue,

-- Quota
advertiser_quota,
agency_quota,
deduped_quota,

-- Straightline Forecast
am_straightline_fcst,
pm_straightline_fcst,
deduped_straightline_fcst,
-- Last 2 Year Revenue
l2y_am_revenue,
l2y_pm_revenue,
deduped_l2y_revenue,
l2yq_am_revenue_qtd,
l2yq_pm_revenue_qtd,
l2yq_deduped_revenue_qtd


from final_am final

left join mgr_plus_final am on final.username = am.employee_unix_name
left join mgr_plus_final pm on final.overlap_username = pm.employee_unix_name


UNION ALL

Select
rtm,
L10_territory,
L10_overlap_territory,
l6_territory,
L6_overlap_territory,
l8_territory,
L8_overlap_territory,
(L4_territory) region,
L4_overlap_territory overlap_region,
(pm.manager) manager,
(am.manager) overlap_manager,
(pm.directs) directs,
(am.directs) overlap_directs,
days_total_in_quarter,
days_closed_in_quarter,
regional_director,
regional_overlap_director,
overlap_l8_manager,
overlap_l10_manager,
l8_manager,
l10_manager,
-- Revenue
cq_am_revenue,
cq_pm_revenue,
deduped_cq_revenue,

-- Quota
advertiser_quota,
agency_quota,
deduped_quota,

-- Straightline Forecast
am_straightline_fcst,
pm_straightline_fcst,
deduped_straightline_fcst,
-- Last 2 Year Revenue
l2y_am_revenue,
l2y_pm_revenue,
deduped_l2y_revenue,
l2yq_am_revenue_qtd,
l2yq_pm_revenue_qtd,
l2yq_deduped_revenue_qtd


from final_pm final

    left join mgr_plus_final pm on final.username = pm.employee_unix_name
    left join mgr_plus_final am on final.overlap_username = am.employee_unix_name


    """,
)

insert_rds_into_gbg_scaled_comp = PrestoInsertOperator(
    dep_list=[gbg_scaled_compensation],
    table="<TABLE:gbg_scaled_compensation_rd>",
    partition={"ds": "<DATEID>", "lead": None},
    create="""
CREATE TABLE IF NOT EXISTS <TABLE:gbg_scaled_compensation_rd> (
   region varchar,
   manager varchar,
   directs varchar,
   days_total_in_quarter bigint,
   days_closed_in_quarter bigint,
   cq_am_revenue double,
   cq_pm_revenue double,
   deduped_cq_revenue double,
   advertiser_quota double,
   agency_quota double,
   deduped_quota double,
   am_straightline_fcst double,
   pm_straightline_fcst double,
   deduped_straightline_fcst double,
   l2y_am_revenue double,
   l2y_pm_revenue double,
   deduped_l2y_revenue double,
   l2yq_am_revenue_qtd double,
   l2yq_pm_revenue_qtd double,
   l2yq_deduped_revenue_qtd double,
   l6_territory varchar,
   l8_territory varchar,
   L10_territory varchar,
   ds varchar,
   lead varchar
)
WITH (
    partitioned_by = ARRAY['ds', 'lead'],
    retention_days = <RETENTION:90>,
    uii = FALSE
)
""",
    select="""
SELECT
region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(cq_am_revenue) + SUM(cq_pm_revenue) -
SUM(case when regional_director = regional_overlap_director and lower(l6_territory) not like '%gaming%'
then cq_am_revenue else 0 end) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(advertiser_quota) + SUM(agency_quota) - SUM(case when regional_director = regional_overlap_director and lower(l6_territory) not like '%gaming%' then advertiser_quota else 0 end) deduped_quota,
SUM(am_straightline_fcst) + SUM(pm_straightline_fcst) - SUM(case when regional_director = regional_overlap_director and lower(l6_territory) not like '%gaming%'then deduped_straightline_fcst else 0 end) deduped_straightline_fcst,
SUM(l2y_am_revenue) + SUM(l2y_pm_revenue) - SUM(case when regional_director = regional_overlap_director and lower(l6_territory) not like '%gaming%'then deduped_l2y_revenue else 0 end) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_am_revenue_qtd) + SUM(l2yq_pm_revenue_qtd) - SUM(case when regional_director = regional_overlap_director and lower(l6_territory) not like '%gaming%' then l2yq_deduped_revenue_qtd else 0 end) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
ds,
regional_director lead

from <TABLE:gbg_scaled_compensation>

where ds = '<DATEID>' and L6_territory like 'AM-%' or L6_territory like 'PM-%'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
ds,
regional_director
""",
)

insert_l8_manager_into_gbg_scaled_comp = PrestoInsertOperator(
    dep_list=[gbg_scaled_compensation],
    table="<TABLE:gbg_scaled_compensation_lead>",
    partition={"ds": "<DATEID>", "lead": None},
    create="""
CREATE TABLE IF NOT EXISTS <TABLE:gbg_scaled_compensation_lead> (
   region varchar,
   manager varchar,
   directs varchar,
   days_total_in_quarter bigint,
   days_closed_in_quarter bigint,
   cq_am_revenue double,
   cq_pm_revenue double,
   deduped_cq_revenue double,
   advertiser_quota double,
   agency_quota double,
   deduped_quota double,
   am_straightline_fcst double,
   pm_straightline_fcst double,
   deduped_straightline_fcst double,
   l2y_am_revenue double,
   l2y_pm_revenue double,
   deduped_l2y_revenue double,
   l2yq_am_revenue_qtd double,
   l2yq_pm_revenue_qtd double,
   l2yq_deduped_revenue_qtd double,
   l6_territory varchar,
   l8_territory varchar,
   L10_territory varchar,
   ds varchar,
   lead varchar
)
WITH (
    partitioned_by = ARRAY['ds', 'lead'],
    retention_days = <RETENTION:90>,
    uii = FALSE
)
""",
    select="""
SELECT
region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(cq_am_revenue) + SUM(cq_pm_revenue) -
SUM(case when l8_manager = overlap_l8_manager and lower(l6_territory) not like '%gaming%'
then cq_am_revenue else 0 end) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(advertiser_quota) + SUM(agency_quota) - SUM(case when l8_manager = overlap_l8_manager and lower(l6_territory) not like '%gaming%' then advertiser_quota else 0 end) deduped_quota,
SUM(am_straightline_fcst) + SUM(pm_straightline_fcst) - SUM(case when l8_manager = overlap_l8_manager and lower(l6_territory) not like '%gaming%'then deduped_straightline_fcst else 0 end) deduped_straightline_fcst,
SUM(l2y_am_revenue) + SUM(l2y_pm_revenue) - SUM(case when l8_manager = overlap_l8_manager and lower(l6_territory) not like '%gaming%'then deduped_l2y_revenue else 0 end) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_am_revenue_qtd) + SUM(l2yq_pm_revenue_qtd) - SUM(case when l8_manager = overlap_l8_manager and lower(l6_territory) not like '%gaming%' then l2yq_deduped_revenue_qtd else 0 end) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
l8_manager lead,
ds

from <TABLE:gbg_scaled_compensation>

where ds = '<DATEID>' and L6_territory like 'AM-%' or L6_territory like 'PM-%'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
l8_manager,
ds
""",
)

insert_l10_manager_into_gbg_scaled_comp = PrestoInsertOperator(
    dep_list=[gbg_scaled_compensation],
    table="<TABLE:gbg_scaled_compensation_l10_lead>",
    partition={"ds": "<DATEID>", "l10_lead": None},
    create="""
CREATE TABLE IF NOT EXISTS <TABLE:gbg_scaled_compensation_l10_lead> (
   region varchar,
   manager varchar,
   directs varchar,
   days_total_in_quarter bigint,
   days_closed_in_quarter bigint,
   cq_am_revenue double,
   cq_pm_revenue double,
   deduped_cq_revenue double,
   advertiser_quota double,
   agency_quota double,
   deduped_quota double,
   am_straightline_fcst double,
   pm_straightline_fcst double,
   deduped_straightline_fcst double,
   l2y_am_revenue double,
   l2y_pm_revenue double,
   deduped_l2y_revenue double,
   l2yq_am_revenue_qtd double,
   l2yq_pm_revenue_qtd double,
   l2yq_deduped_revenue_qtd double,
   l6_territory varchar,
   l8_territory varchar,
   L10_territory varchar,
   ds varchar,
   l10_lead varchar
)
WITH (
    partitioned_by = ARRAY['ds', 'l10_lead'],
    retention_days = <RETENTION:90>,
    uii = FALSE
)
""",
    select="""
SELECT
region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(cq_am_revenue) + SUM(cq_pm_revenue) -
SUM(case when l10_manager = overlap_l10_manager and lower(l6_territory) not like '%gaming%'
then cq_am_revenue else 0 end) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(advertiser_quota) + SUM(agency_quota) - SUM(case when l10_manager = overlap_l10_manager and lower(l6_territory) not like '%gaming%' then advertiser_quota else 0 end) deduped_quota,
SUM(am_straightline_fcst) + SUM(pm_straightline_fcst) - SUM(case when l10_manager = overlap_l10_manager and lower(l6_territory) not like '%gaming%'then deduped_straightline_fcst else 0 end) deduped_straightline_fcst,
SUM(l2y_am_revenue) + SUM(l2y_pm_revenue) - SUM(case when l10_manager = overlap_l10_manager and lower(l6_territory) not like '%gaming%'then deduped_l2y_revenue else 0 end) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_am_revenue_qtd) + SUM(l2yq_pm_revenue_qtd) - SUM(case when l10_manager = overlap_l10_manager and lower(l6_territory) not like '%gaming%' then l2yq_deduped_revenue_qtd else 0 end) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
l10_manager l10_lead,
ds

from <TABLE:gbg_scaled_compensation>

where ds = '<DATEID>' and L6_territory like 'AM-%' or L6_territory like 'PM-%'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
l10_manager,
ds
""",
)

gbg_scaled_compensation_v = PrestoOperator(
    dep_list=[
        gbg_scaled_compensation,
        insert_rds_into_gbg_scaled_comp,
        insert_l8_manager_into_gbg_scaled_comp,
        insert_l10_manager_into_gbg_scaled_comp,
    ],
    query="""
    CREATE OR REPLACE VIEW <TABLE:gbg_scaled_compensation_v> AS

SELECT
'RVP' compensation_type,
'EMEA' region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(cq_am_revenue) + SUM(cq_pm_revenue) -
SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-EMEA%'
then cq_am_revenue else 0 end) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(advertiser_quota) + SUM(agency_quota) - SUM(case when manager is not null and  overlap_manager is not null
and overlap_region like 'GBG-EMEA%'
then advertiser_quota else 0 end) deduped_quota,
SUM(am_straightline_fcst) + SUM(pm_straightline_fcst) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-EMEA%' then deduped_straightline_fcst else 0 end) deduped_straightline_fcst,
SUM(l2y_am_revenue) + SUM(l2y_pm_revenue) - SUM(case when manager is not null and  overlap_manager is not null  and overlap_region like 'GBG-EMEA%' then deduped_l2y_revenue else 0 end) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_am_revenue_qtd) + SUM(l2yq_pm_revenue_qtd) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-EMEA%' then l2yq_deduped_revenue_qtd else 0 end) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
regional_director lead,
ds

from <TABLE:gbg_scaled_compensation>

where ds = '<DATEID>' and region like 'GBG-EMEA%'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
regional_director,
ds

UNION ALL

SELECT
'RVP' compensation_type,
'APAC' region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(cq_am_revenue) + SUM(cq_pm_revenue) -
SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-APAC%'
then cq_am_revenue else 0 end) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(advertiser_quota) + SUM(agency_quota) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-APAC%' then advertiser_quota else 0 end) deduped_quota,
SUM(am_straightline_fcst) + SUM(pm_straightline_fcst) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-APAC%'then deduped_straightline_fcst else 0 end) deduped_straightline_fcst,
SUM(l2y_am_revenue) + SUM(l2y_pm_revenue) - SUM(case when manager is not null and  overlap_manager is not null  and overlap_region like 'GBG-APAC%'then deduped_l2y_revenue else 0 end) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_am_revenue_qtd) + SUM(l2yq_pm_revenue_qtd) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-APAC%' then l2yq_deduped_revenue_qtd else 0 end) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
regional_director lead,
ds

from <TABLE:gbg_scaled_compensation>

where ds = '<DATEID>' and region like 'GBG-APAC%'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
regional_director,
ds

UNION ALL

SELECT
'RVP' compensation_type,
'NA' region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(cq_am_revenue) + SUM(cq_pm_revenue) -
SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-NA%'
then cq_am_revenue else 0 end) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(advertiser_quota) + SUM(agency_quota) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-NA%' then advertiser_quota else 0 end) deduped_quota,
SUM(am_straightline_fcst) + SUM(pm_straightline_fcst) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-NA%'then deduped_straightline_fcst else 0 end) deduped_straightline_fcst,
SUM(l2y_am_revenue) + SUM(l2y_pm_revenue) - SUM(case when manager is not null and  overlap_manager is not null  and overlap_region like 'GBG-NA%'then deduped_l2y_revenue else 0 end) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_am_revenue_qtd) + SUM(l2yq_pm_revenue_qtd) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-NA%' then l2yq_deduped_revenue_qtd else 0 end) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
regional_director lead,
ds

from <TABLE:gbg_scaled_compensation>

where ds = '<DATEID>' and region like 'GBG-NA%'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
regional_director,
ds

UNION ALL

SELECT
'RVP' compensation_type,
'LATAM' region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(cq_am_revenue) + SUM(cq_pm_revenue) -
SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-LATAM%'
then cq_am_revenue else 0 end) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(advertiser_quota) + SUM(agency_quota) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-LATAM%' then advertiser_quota else 0 end) deduped_quota,
SUM(am_straightline_fcst) + SUM(pm_straightline_fcst) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-LATAM%'then deduped_straightline_fcst else 0 end) deduped_straightline_fcst,
SUM(l2y_am_revenue) + SUM(l2y_pm_revenue) - SUM(case when manager is not null and  overlap_manager is not null  and overlap_region like 'GBG-LATAM%'then deduped_l2y_revenue else 0 end) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_am_revenue_qtd) + SUM(l2yq_pm_revenue_qtd) - SUM(case when manager is not null and  overlap_manager is not null and overlap_region like 'GBG-LATAM%' then l2yq_deduped_revenue_qtd else 0 end) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
regional_director lead,
ds

from <TABLE:gbg_scaled_compensation>

where ds = '<DATEID>' and region like 'GBG-LATAM%'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
regional_director,
ds

UNION ALL

SELECT
'RVP' compensation_type,
'Gaming' region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(cq_am_revenue) + SUM(cq_pm_revenue) -
SUM(case when manager is not null and  overlap_manager is not null and lower(l6_territory) not like '%gaming%'
then cq_am_revenue else 0 end) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(advertiser_quota) + SUM(agency_quota) - SUM(case when manager is not null and  overlap_manager is not null and lower(l6_territory) not like '%gaming%' then advertiser_quota else 0 end) deduped_quota,
SUM(am_straightline_fcst) + SUM(pm_straightline_fcst) - SUM(case when manager is not null and  overlap_manager is not null and lower(l6_territory) not like '%gaming%'then deduped_straightline_fcst else 0 end) deduped_straightline_fcst,
SUM(l2y_am_revenue) + SUM(l2y_pm_revenue) - SUM(case when manager is not null and  overlap_manager is not null  and lower(l6_territory) not like '%gaming%'then deduped_l2y_revenue else 0 end) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_am_revenue_qtd) + SUM(l2yq_pm_revenue_qtd) - SUM(case when manager is not null and  overlap_manager is not null and lower(l6_territory) not like '%gaming%' then l2yq_deduped_revenue_qtd else 0 end) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
regional_director lead,
ds

from <TABLE:gbg_scaled_compensation>

where ds = '<DATEID>' and region like 'GBG-Gaming%'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
regional_director,
ds

UNION ALL

SELECT
'Manager' compensation_type,
region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(cq_am_revenue) + SUM(cq_pm_revenue) -
SUM(case when manager = overlap_manager and lower(l6_territory) not like '%gaming%'
then cq_am_revenue else 0 end) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(advertiser_quota) + SUM(agency_quota) - SUM(case when manager = overlap_manager and lower(l6_territory) not like '%gaming%' then advertiser_quota else 0 end) deduped_quota,
SUM(am_straightline_fcst) + SUM(pm_straightline_fcst) - SUM(case when manager = overlap_manager and lower(l6_territory) not like '%gaming%'then deduped_straightline_fcst else 0 end) deduped_straightline_fcst,
SUM(l2y_am_revenue) + SUM(l2y_pm_revenue) - SUM(case when manager = overlap_manager and lower(l6_territory) not like '%gaming%'then deduped_l2y_revenue else 0 end) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_am_revenue_qtd) + SUM(l2yq_pm_revenue_qtd) - SUM(case when manager = overlap_manager and lower(l6_territory) not like '%gaming%' then l2yq_deduped_revenue_qtd else 0 end) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
regional_director lead,
ds

from <TABLE:gbg_scaled_compensation>

where ds = '<DATEID>'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
regional_director,
ds

UNION ALL

SELECT
'Regional Director' compensation_type,
region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(deduped_cq_revenue) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(deduped_quota) deduped_quota,
SUM(deduped_straightline_fcst) deduped_straightline_fcst,
SUM(deduped_l2y_revenue) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_deduped_revenue_qtd) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
lead,
ds

from <TABLE:gbg_scaled_compensation_rd>

where ds = '<DATEID>'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
ds,
lead

UNION ALL

SELECT
'L8 Manager' compensation_type,
region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(deduped_cq_revenue) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(deduped_quota) deduped_quota,
SUM(deduped_straightline_fcst) deduped_straightline_fcst,
SUM(deduped_l2y_revenue) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_deduped_revenue_qtd) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
lead,
ds

from <TABLE:gbg_scaled_compensation_lead>

where ds = '<DATEID>'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
ds,
lead

UNION ALL

SELECT
'L10 Manager' compensation_type,
region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
SUM(cq_am_revenue) cq_am_revenue,
SUM(cq_pm_revenue) cq_pm_revenue,
SUM(deduped_cq_revenue) deduped_cq_revenue,
SUM(advertiser_quota) advertiser_quota,

SUM(agency_quota) agency_quota,
-- Coding for RVP
SUM(deduped_quota) deduped_quota,
SUM(deduped_straightline_fcst) deduped_straightline_fcst,
SUM(deduped_l2y_revenue) deduped_l2y_revenue,
SUM(l2y_am_revenue) l2y_am_revenue,
SUM(l2y_pm_revenue) l2y_pm_revenue,
SUM(l2yq_am_revenue_qtd) l2yq_am_revenue_qtd,
SUM(l2yq_pm_revenue_qtd) l2yq_pm_revenue_qtd,
SUM(l2yq_deduped_revenue_qtd) l2yq_deduped_revenue_qtd,
SUM(am_straightline_fcst) am_straightline_fcst,
SUM(pm_straightline_fcst) pm_straightline_fcst,
L6_territory,
l8_territory,
L10_territory,
l10_lead lead,
ds

from <TABLE:gbg_scaled_compensation_l10_lead>

where ds = '<DATEID>'

group by

region,
manager,
directs,
days_total_in_quarter,
days_closed_in_quarter,
L6_territory,
l8_territory,
L10_territory,
ds,
l10_lead
    """,
)


if is_test():
    pass
else:

    tableau_refresh_gbg_scaled_compensation = TableauPublishOperator(
        dep_list=[
            gbg_scaled_compensation_v,
        ],
        refresh_cfg_id=29611,
        num_retries=2,
    )
