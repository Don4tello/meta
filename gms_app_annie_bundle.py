#!/usr/bin/env python3
# @lint-ignore-every DSFMT [do not copy-paste]. Format the file to remove this comment. https://fburl.com/wiki/mmzh332m


from dataswarm.operators import (
    GlobalDefaults,
    PrestoInsertOperator,
    WaitForHivePartitionsOperator,
    TableauPublishOperator,
)
from dataswarm.operators.runcontext import is_test


GlobalDefaults.set(
    user="mmazur",
    schedule="0 0 * * 6",
    num_retries=1,
    depends_on_past=False,
    macros={
        "QUARTERID": "CAST(DATE_TRUNC('quarter',DATE('<DATEID>')) AS VARCHAR)",
    },
    secure_group="gms_central_analytics",
    oncall="gms_central_analytics",
    partition="ds=<DATEID>",
    extra_emails=[],
)

wait_for_fct_sow_appannie_bundle = WaitForHivePartitionsOperator(
    table="fct_sow_appannie_bundle",
    namespace="platform",
    partitions_list=[
        "ds=<DATEID>/granularity=weekly",
    ],
)

gms_app_annie_bundle = PrestoInsertOperator(
    dep_list=[
        wait_for_fct_sow_appannie_bundle,
    ],
    table="<TABLE:gms_app_annie_bundle>",
    namespace="platform",
    partition="ds=<DATEID>",
    documentation={"description": """Staging Table prior to RTM Split"""},
    create=r"""
    CREATE TABLE IF NOT EXISTS <TABLE:gms_app_annie_bundle> (
     aa_gaming_category_name VARCHAR,
     app_name VARCHAR,
     ult_advertiser_name VARCHAR,
     platform VARCHAR,
     territory_l4_name VARCHAR,
     territory_l6_name VARCHAR,
     appannie_rev DOUBLE,
     fb_rev DOUBLE,
     appannie_downloads DOUBLE,
     fb_downloads DOUBLE,
     ds VARCHAR
    )
    WITH (
        format = 'DWRF',
        oncall = 'gms_central_analytics',
        partitioned_by = ARRAY['ds'],
        retention_days = <RETENTION:90>
    )
    """,
    select=r"""
WITH region as
(SELECT fb_region, fb_subregion, country_abbr, country
FROM d_geo_country:edw_bir01
where ds= '<LATEST_DS:d_geo_country:edw_bir01>'
)


SELECT
        ds,
        aa_gaming_category_name,
        app_name,
        ult_advertiser_name,
        platform,
        case when COALESCE(territory_l6_name, fb_subregion ) like '%APAC%' then 'APAC'
     when COALESCE(territory_l6_name, fb_subregion ) like '%EMEA%' then 'EMEA'
     when COALESCE(territory_l6_name, fb_subregion ) like '%AMER%' then 'NA'
     when COALESCE(territory_l6_name, fb_subregion ) like '%LATAM%' then 'LATAM'
     else 'Unknown' end territory_l4_name,
        COALESCE(territory_l6_name, fb_subregion ) territory_l6_name,
        SUM(aa_revenue) AS appannie_rev,
        SUM(fb_revenue) AS fb_rev,
        SUM(aa_downloads) AS appannie_downloads,
        SUM(fb_installs) AS fb_downloads

    FROM fct_sow_appannie_bundle
    left join region on app_country = country_abbr
    WHERE
        NOT FB_IS_EMPTY(aa_gaming_category_name)
        AND ds = '<DATEID>'
        and granularity='weekly'

    GROUP BY

        ds,
        aa_gaming_category_name,
        app_name,
        ult_advertiser_name,
        platform,
        case when COALESCE(territory_l6_name, fb_subregion ) like '%APAC%' then 'APAC'
     when COALESCE(territory_l6_name, fb_subregion ) like '%EMEA%' then 'EMEA'
     when COALESCE(territory_l6_name, fb_subregion ) like '%AMER%' then 'NA'
     when COALESCE(territory_l6_name, fb_subregion ) like '%LATAM%' then 'LATAM'
     else 'Unknown' end ,
        COALESCE(territory_l6_name, fb_subregion )
    """,
)
