# Python Classes to store data around presto tables
#!/usr/bin/env python3
# @lint-ignore-every DSFMT [do not copy-paste]. Format the file to remove this comment. https://fburl.com/wiki/mmzh332m


from dataswarm_commons.operators.schema import Column, HiveAnon, Table


class BpoCoverageAsisStg1:
    """@doc-string : python class for table bpo_coverage_asis_stg"""

    def __init__(self):
        self.name = "<TABLE:bpo_coverage_asis_stg_1>"
        self.create = """
               CREATE TABLE IF NOT EXISTS <TABLE:bpo_coverage_asis_stg_1> (
                        id_d_ad_account bigint,
                        ad_account_id bigint,
                        ad_account_name varchar,
                        advertiser_name varchar,
                        id_dh_territory bigint,
                        advertiser_country varchar,
                        advertiser_vertical varchar,
                        advertiser_sub_vertical varchar,
                        gvm_vertical_name varchar,
                        gvm_sub_vertical_name varchar,
                        specialty VARCHAR,
                        legacy_advertiser_sub_vertical VARCHAR,
                        legacy_advertiser_vertical VARCHAR,
                        region varchar,
                        sub_region varchar,
                        fraud_ind varchar,
                        is_deleted varchar,
                        is_house_account bigint,
                        is_fcast_eligible boolean,
                        advertiser_fbid varchar,
                        advertiser_sfid varchar,
                        ultimate_parent_fbid varchar,
                        ultimate_parent_sfid varchar,
                        ultimate_parent_name varchar,
                        planning_agency_name varchar,
                        planning_agency_fbid varchar,
                        planning_agency_sfid varchar,
                        planning_agency_ult_fbid varchar,
                        planning_agency_ult_sfid varchar,
                        planning_agency_ult_name varchar,
                        split_cp varchar,
                        split double,
                        id_d_employee bigint,
                        rep_fbid_cp varchar,
                        client_partner varchar,
                        cp_username varchar,
                        cp_start_date varchar,
                        cp_manager varchar,
                        cp_manager_username varchar,
                        l12fbid_cp varchar,
                        rep_fbid_am varchar,
                        account_manager varchar,
                        am_username varchar,
                        am_start_date varchar,
                        am_manager varchar,
                        am_manager_username varchar,
                        l12fbid_am varchar,
                        split_am varchar,
                        rep_fbid_pm varchar,
                        partner_manager varchar,
                        pm_username varchar,
                        pm_start_date varchar,
                        pm_manager varchar,
                        pm_manager_username varchar,
                        l12fbid_pm varchar,
                        split_pm varchar,
                        rep_fbid_sp varchar,
                        agency_partner varchar,
                        ap_username varchar,
                        ap_start_date varchar,
                        ap_manager varchar,
                        ap_manager_username varchar,
                        l12fbid_sp varchar,
                        split_sp varchar,
                        rep_fbid_ap varchar,
                        l12fbid_ap varchar,
                        split_ap varchar,
                        is_gpa boolean,
                        l12fbid_unmanaged varchar,
                        is_staged boolean,
                        reseller_fbid varchar,
                        reseller_sfid varchar,
                        reseller_name varchar,
                        rep_fbid_me varchar,
                        rep_fbid_rp varchar,
                        l12fbid_me varchar,
                        l12fbid_rp varchar,
                        split_me varchar,
                        split_rp varchar,
                        dq_flags_me map(varchar, varchar),
                        dq_flags_rp map(varchar, varchar),
                        rep_fbid_fcast varchar,
                        l12fbid_fcast varchar,
                        program varchar,
                        market varchar,
                        lwi_only_flag boolean,
                        newbie_flag boolean,
                        low_spender_90d_flag boolean,
                        ig_boost_only_flag boolean,
                        sss_flag boolean,
                        seasonal_flag boolean,
                        rep_role varchar,
                        is_closed boolean,
                        is_high_risk boolean,
                        is_pci boolean,
                        is_covered boolean,
                        is_banned boolean,
                        is_ou_eligible boolean,
                        id_d_customer_account_adv bigint,
                        advertiser_coverage_model_daa varchar,
                        advertiser_program_daa varchar,
                        agency_coverage_model_daa varchar,
                        is_gat boolean,
                        is_gcm boolean,
                        is_magic93 boolean,
                        newbie_type varchar,
                        l12_advertiser_territory varchar,
                        l10_advertiser_territory varchar,
                        l8_advertiser_territory varchar,
                        l6_advertiser_territory varchar,
                        l4_advertiser_territory varchar,
                        l2_advertiser_territory varchar,
                        l12_usern_advertiser_territory varchar,
                        l10_usern_advertiser_territory varchar,
                        l12_manager_advertiser_territory varchar,
                        l10_manager_advertiser_territory varchar,
                        l8_manager_advertiser_territory varchar,
                        l12_agency_territory varchar,
                        l10_agency_territory varchar,
                        l8_agency_territory varchar,
                        l6_agency_territory varchar,
                        l4_agency_territory varchar,
                        l2_agency_territory varchar,
                        l12_usern_agency_territory varchar,
                        l10_usern_agency_territory varchar,
                        l12_manager_agency_territory varchar,
                        l10_manager_agency_territory varchar,
                        l8_manager_agency_territory varchar,
                        ad_account_l4_fbid BIGINT,
                        ad_account_l8_fbid BIGINT,
                        ad_account_l10_fbid BIGINT,
                        ad_account_l12_fbid BIGINT,
                        gms_optimal_target double,
                        gms_liquidity_target double,
                        gso_optimal_target double,
                        gso_liquidity_target double,
                        smb_optimal_target double,
                        smb_liquidity_target double,
                        l4_optimal_target double,
                        l4_liquidity_target double,
                        l8_optimal_target double,
                        l8_liquidity_target double,
                        l10_optimal_target double,
                        l10_liquidity_target double,
                        l12_optimal_target double,
                        l12_liquidity_target double,
                        adv_quota DOUBLE,
                        agency_quota DOUBLE,
                        sbg_quota DOUBLE,
                        sales_forecast DOUBLE,
                        sales_forecast_prior DOUBLE,
                        optimal_goal DOUBLE,
                        liquidity_goal DOUBLE,
                        l12_reporting_territory VARCHAR,
                        l10_reporting_territory VARCHAR,
                        l8_reporting_territory  VARCHAR,
                        l6_reporting_territory  VARCHAR,
                        l4_reporting_territory  VARCHAR,
                        l2_reporting_territory  VARCHAR,
                        l12_reporting_terr_mgr VARCHAR,
                        l10_reporting_terr_mgr VARCHAR,
                        l8_reporting_terr_mgr VARCHAR,
                        l6_reporting_terr_mgr VARCHAR,
                        segmentation VARCHAR,
                        l6_manager_agency_territory VARCHAR,
                        rep_fbid_csm VARCHAR,
                        client_solutions_manager VARCHAR,
                        csm_username varchar,
                        csm_start_date varchar,
                        csm_manager varchar,
                        csm_manager_username varchar,
                        rep_fbid_asm VARCHAR,
                        agency_solutions_manager VARCHAR,
                        asm_username varchar,
                        asm_start_date varchar,
                        asm_manager varchar,
                        asm_manager_username varchar,
                        reseller_partner VARCHAR,
                        rp_username varchar,
                        rp_start_date varchar,
                        rp_manager varchar,
                        rp_manager_username varchar,
                        sales_adv_country_group VARCHAR,
                        sales_adv_subregion VARCHAR,
                        sales_adv_region VARCHAR,
                        program_optimal_target double,
                        program_liquidity_target double,
                        l6_optimal_target double,
                        l6_liquidity_target double,
                        l12_reseller_territory VARCHAR,
                        l10_reseller_territory VARCHAR,
                        l8_reseller_territory  VARCHAR,
                        l6_reseller_territory  VARCHAR,
                        l4_reseller_territory  VARCHAR,
                        l2_reseller_territory  VARCHAR,
                        l12_reseller_terr_mgr VARCHAR,
                        l10_reseller_terr_mgr VARCHAR,
                        l8_reseller_terr_mgr  VARCHAR,
                        l6_reseller_terr_mgr  VARCHAR,
                        reseller_quota DOUBLE,
                        country_agc VARCHAR,
                        market_agc VARCHAR,
                        region_agc VARCHAR,
                        sub_region_agc VARCHAR,
                        business_type_adv VARCHAR,
                        business_type_agc VARCHAR,
                        planning_agency_operating_co VARCHAR,
                        program_agency VARCHAR,
                        china_export_advertiser VARCHAR,
                        export_advertiser_country VARCHAR,
                        billing_country_adv VARCHAR,
                        billing_region_adv VARCHAR,
                        billing_country_agc VARCHAR,
                        billing_region_agc VARCHAR,
                        hq_country_adv VARCHAR,
                        hq_region_adv VARCHAR,
                        hq_country_agc VARCHAR,
                        hq_region_agc VARCHAR,
                        inm_optimal_goal DOUBLE,
                        agc_optimal_goal DOUBLE,
                        legacy_gvm_vertical_name_v2 VARCHAR,
                        legacy_gvm_sub_vertical_name_v2 VARCHAR,
                        ultimate_parent_vertical_name_v2 VARCHAR,
                        revenue_segment VARCHAR,
                        subsegment VARCHAR,
                        scaled_dr_resilience_goal DOUBLE,
                        inm_dr_resilience_goal DOUBLE,
                        am_lwi_ind VARCHAR,
                        sales_forecast_prior_2w DOUBLE,
                        ts VARCHAR,
                        ds varchar
                       )
                        WITH (
                                        partitioned_by = ARRAY['ds'],
                                        retention_days = <RETENTION:90>,
                                        uii=false,
                                        bucketed_by=ARRAY['ad_account_id'],
                                        bucket_count=256


                                       ) """
        self.select = """

                            WITH acc_reduction AS (
                        SELECT
                            ad_account_id,
                            reduction

                        FROM <TABLE:bpo_coverage_asis_stg_reduction>
                        WHERE ds = '<DATEID>'
                   ),
                   am_lwi_ind as (
                       SELECT account_id,
                              am_lwi_ind

                       FROM <TABLE:bpo_coverage_asis_stg_lwi_am_ind>

                       WHERE ds ='<DATEID>'
                   ),
                    d_geo AS (
                        SELECT DISTINCT
                            country_abbr,
                            country
                        FROM d_geo_country
                        WHERE
                            ds = '<DATEID>'
                   ),

                    ACC AS (
                            SELECT
                                NULL AS id_d_ad_account,
                                    acc.ad_account_id,
                                    NULL AS ad_account_sfid,
                                    CASE WHEN revenue_segment in ('SBG Longtail Net', 'SBG Ineligible')
                                         THEN 'SBG'
                                         ELSE advertiser_name
                                    END advertiser_name,

                                    NULL AS id_dh_territory,
                                    NULL AS id_dh_account_sub_vertical,
                                    coalesce(d_geo.country, 'Unknown') advertiser_country,
                                    advertiser_vertical,
                                    advertiser_sub_vertical,
                                    gvm_vertical_name,
                                    gvm_sub_vertical_name,
                                    specialty,
                                    legacy_advertiser_vertical,
                                    legacy_advertiser_sub_vertical,
                                    region,
                                    sub_region,
                                    CAST(fraud_ind AS VARCHAR) AS fraud_ind,
                                    NULL AS is_deleted,
                                    CAST(is_house_account AS BIGINT) AS is_house_account,
                                    is_fcast_eligible,
                                    CAST(ad_account_l12_fbid AS VARCHAR) AS ad_account_l12_fbid,
                                    CAST(ad_account_l10_fbid AS VARCHAR) AS ad_account_l10_fbid,
                                    CAST(ad_account_l8_fbid AS VARCHAR) AS ad_account_l8_fbid,
                                    CAST(ad_account_l4_fbid AS VARCHAR) AS ad_account_l4_fbid,
                                    CASE WHEN revenue_segment in ('SBG Longtail Net', 'SBG Ineligible')
                                         THEN '-999'
                                         ELSE CAST(advertiser_fbid AS VARCHAR)
                                    END AS advertiser_fbid,
                                    CASE WHEN revenue_segment in ('SBG Longtail Net', 'SBG Ineligible')
                                         THEN '-999'
                                         ELSE advertiser_sfid
                                    END AS advertiser_sfid,
                                    CASE WHEN revenue_segment in ('SBG Longtail Net', 'SBG Ineligible')
                                         THEN '-999'
                                         ELSE ultimate_parent_fbid
                                    END AS  ultimate_parent_fbid,
                                    CASE WHEN revenue_segment in ('SBG Longtail Net', 'SBG Ineligible')
                                         THEN '-999'
                                         ELSE ultimate_parent_sfid
                                    END AS ultimate_parent_sfid,
                                    CASE WHEN revenue_segment in ('SBG Longtail Net', 'SBG Ineligible')
                                         THEN 'SBG'
                                         ELSE ultimate_parent_name
                                    END AS ultimate_parent_name,
                                    planning_agency_name,
                                    planning_agency_fbid,
                                    planning_agency_sfid,
                                    planning_agency_ult_fbid,
                                    planning_agency_ult_sfid,
                                    planning_agency_ult_name,
                                    planning_quarter,
                                    split_cp,
                                    split,
                                    rep_fbid_cp,
                                    CAST(l12fbid_cp AS VARCHAR) AS l12fbid_cp,
                                    CAST(l10fbid_cp AS VARCHAR) AS l10fbid_cp,
                                    CAST(l8fbid_cp AS VARCHAR) AS l8fbid_cp,
                                    CAST(l4fbid_cp AS VARCHAR) AS l4fbid_cp,
                                    rep_fbid_am,
                                    CAST(l12fbid_am AS VARCHAR) AS l12fbid_am,
                                    CAST(l10fbid_am AS VARCHAR) AS l10fbid_am,
                                    CAST(l8fbid_am AS VARCHAR) AS l8fbid_am,
                                    CAST(l4fbid_am AS VARCHAR) AS l4fbid_am,
                                    split_am,
                                    rep_fbid_pm,
                                    CAST(l12fbid_pm AS VARCHAR) AS l12fbid_pm,
                                    CAST(l10fbid_pm AS VARCHAR) AS l10fbid_pm,
                                    CAST(l8fbid_pm AS VARCHAR) AS l8fbid_pm,
                                    CAST(l4fbid_pm AS VARCHAR) AS l4fbid_pm,
                                    split_pm,
                                    rep_fbid_sp,
                                    CAST(l12fbid_sp AS VARCHAR) AS l12fbid_sp,
                                    CAST(l10fbid_sp AS VARCHAR) AS l10fbid_sp,
                                    CAST(l8fbid_sp AS VARCHAR) AS l8fbid_sp,
                                    CAST(l4fbid_sp AS VARCHAR) AS l4fbid_sp,
                                    split_sp,
                                    rep_fbid_ap,
                                    CAST(l12fbid_ap AS VARCHAR) AS l12fbid_ap,
                                    CAST(l10fbid_ap AS VARCHAR) AS l10fbid_ap,
                                    CAST(l8fbid_ap AS VARCHAR) AS l8fbid_ap,
                                    CAST(l4fbid_ap AS VARCHAR) AS l4fbid_ap,
                                    split_ap,
                                    NULL AS dq_flags_cp,
                                    NULL AS dq_flags_am,
                                    NULL AS dq_flags_pm,
                                    NULL AS dq_flags_sp,
                                    NULL AS dq_flags_ap,
                                    is_gpa,
                                    CAST(l12fbid_unmanaged AS VARCHAR) AS l12fbid_unmanaged,
                                    CAST(l8fbid_unmanaged AS VARCHAR) AS l8fbid_unmanaged,
                                    CAST(l4fbid_unmanaged AS VARCHAR) AS l4fbid_unmanaged,
                                    NULL AS requested_coverage,
                                    NULL AS is_staged,
                                    NULL AS dq_flags_rp,
                                    rep_fbid_rp,
                                    CAST(l12fbid_rp AS VARCHAR) AS l12fbid_rp,
                                    CAST(l10fbid_rp AS VARCHAR) AS l10fbid_rp,
                                    CAST(l8fbid_rp AS VARCHAR) AS l8fbid_rp,
                                    CAST(l4fbid_rp AS VARCHAR) AS l4fbid_rp,
                                    reseller_name,
                                    reseller_sfid,
                                    reseller_fbid,
                                    split_rp,
                                    rep_fbid_me,
                                    CAST(l12fbid_me AS VARCHAR) AS l12fbid_me,
                                    CAST(l10fbid_me AS VARCHAR) AS l10fbid_me,
                                    CAST(l8fbid_me AS VARCHAR) AS l8fbid_me,
                                    CAST(l4fbid_me AS VARCHAR) AS l4fbid_me,
                                    split_me,
                                    NULL AS column_to_delete,
                                    NULL AS dq_flags_me,
                                    CAST(l10fbid_unmanaged AS VARCHAR) AS l10fbid_unmanaged,
                                    advertiser_first_spend_date,
                                    last_200spend_date,
                                    advertiser_rev_90d,
                                    is_closed,
                                    is_high_risk,
                                    is_pci,
                                    is_covered,
                                    is_banned,
                                    is_ou_eligible,
                                    commission_types,
                                    ad_account_name,
                                    NULL AS id_d_customer_account_adv,
                                    NULL AS advertiser_coverage_model_daa,
                                    NULL AS advertiser_program_daa,
                                    NULL AS agency_coverage_model_daa,
                                    rep_cp,
                                    rep_am,
                                    rep_pm,
                                    rep_sp,
                                    rep_ap,
                                    rep_rp,
                                    rep_me,
                                    is_gat,
                                    is_gcm,
                                    is_magic93,
                                    member_roles,
                                    has_me,
                                    program_longtail,
                                    newbie_type,
                                    programs_array,
                                    NULL AS column_to_delete2,
                                    rep_fbid_fcast,
                                    CAST(l12fbid_fcast AS VARCHAR) AS l12fbid_fcast,
                                    CAST(l10fbid_fcast AS VARCHAR) AS l10fbid_fcast,
                                    CAST(l8fbid_fcast AS VARCHAR) AS l8fbid_fcast,
                                    CAST(l4fbid_fcast AS VARCHAR) AS l4fbid_fcast,
                                    program,
                                    rep_role,
                                    NULL AS id_d_account_team_cp,
                                    NULL AS commission_sched_id_cp,
                                    NULL AS id_d_account_team_am,
                                    um_program,
                                    NULL AS um_program_area,
                                    NULL AS commission_sched_id_am,
                                    NULL AS id_d_account_team_pm,
                                    NULL AS commission_sched_id_pm,
                                    NULL AS id_d_account_team_sp,
                                    NULL AS commission_sched_id_sp,
                                    NULL AS id_d_account_team_ap,
                                    NULL AS commission_sched_id_ap,
                                    NULL AS id_d_account_team_rp,
                                    NULL AS commission_sched_id_rp,
                                    NULL AS id_d_account_team_me,
                                    NULL AS commission_sched_id_me,
                                    is_ou_engaged,
                                    market,
                                    ultimate_vertical,
                                    rep_mehr,
                                    rep_fbid_mehr,
                                    CAST(l12fbid_mehr AS VARCHAR) AS l12fbid_mehr,
                                    CAST(l10fbid_mehr AS VARCHAR) AS l10fbid_mehr,
                                    CAST(l8fbid_mehr AS VARCHAR) AS l8fbid_mehr,
                                    CAST(l4fbid_mehr AS VARCHAR) AS l4fbid_mehr,
                                    split_mehr,
                                    NULL AS id_d_account_team_mehr,
                                    NULL AS commission_sched_id_mehr,
                                    NULL AS dq_flags_mehr,
                                    sbg_quota,
                                    optimal_product_goal,
                                    optimal_liquidity_goal,
                                    adv_quota,
                                    agency_quota,
                                    reseller_quota,
                                    sales_forecast,
                                    sales_forecast_prior,
                                    programs_array_paa,
                                    segment_cq,
                                    paced_revenue_goal,
                                    paced_optimal_spend_goal,
                                    paced_optimal_liquidity_goal,
                                    rep_csm,
                                    rep_fbid_csm,
                                    CAST(l12fbid_csm AS VARCHAR) AS l12fbid_csm,
                                    CAST(l10fbid_csm AS VARCHAR) AS l10fbid_csm,
                                    CAST(l8fbid_csm AS VARCHAR) AS l8fbid_csm,
                                    CAST(l4fbid_csm AS VARCHAR) AS l4fbid_csm,
                                    split_csm,
                                    NULL AS id_d_account_team_csm,
                                    NULL AS commission_sched_id_csm,
                                    NULL AS dq_flags_csm,
                                    rep_asm,
                                    rep_fbid_asm,
                                    CAST(l12fbid_asm AS VARCHAR) AS l12fbid_asm,
                                    CAST(l10fbid_asm AS VARCHAR) AS l10fbid_asm,
                                    CAST(l8fbid_asm AS VARCHAR) AS l8fbid_asm,
                                    CAST(l4fbid_asm AS VARCHAR) AS l4fbid_asm,
                                    split_asm,
                                    NULL AS id_d_account_team_asm,
                                    NULL AS commission_sched_id_asm,
                                    NULL AS dq_flags_asm,
                                    china_export_advertiser,
                                    export_advertiser_country,
                                    billing_country_adv,
                                    billing_region_adv,
                                    billing_country_agc,
                                    billing_region_agc,
                                    hq_country_adv,
                                    hq_region_adv,
                                    hq_country_agc,
                                    hq_region_agc,
                                    is_big6,
                                    l12fbid_engaged,
                                    l10fbid_engaged,
                                    l8fbid_engaged,
                                    l4fbid_engaged,
                                    program_agency,
                                    bm_id,
                                    l12_reporting,
                                    l10_reporting,
                                    l8_reporting,
                                    l6_reporting,
                                    l4_reporting,
                                    planning_agency_operating_co,
                                    country_agc,
                                    market_agc,
                                    sub_region_agc,
                                    region_agc,
                                    business_type_adv,
                                    business_type_agc,
                                    segment,
                                    country_adv,
                                    market_adv,
                                    sub_region_adv,
                                    region_adv,
                                    ds,
                                    reduction,
                                    adv_quota*inm_optimal_pct_goal inm_optimal_goal,
                                    agency_quota*agc_optimal_pct_goal agc_optimal_goal,
                                    legacy_gvm_vertical_name_v2 ,
                                    legacy_gvm_sub_vertical_name_v2 ,
                                    ultimate_parent_vertical_name_v2,
                                    inm_dr_resilience_goal,
                                    scaled_dr_resilience_goal,
                                    CASE
                                        WHEN lower(revenue_segment) like('%sbg%')
                                        THEN coalesce(segment,revenue_segment)
                                        ELSE revenue_segment
                                    END subsegment,
                                    revenue_segment,
                            CAST(
                                COALESCE(l12fbid_cp, l12fbid_am, l12fbid_engaged, l12fbid_unmanaged) AS BIGINT
                           ) l12_advertiser_fbid,
                            CAST(COALESCE(l12fbid_ap, l12fbid_pm) AS BIGINT) l12_agency_fbid,
                            CAST(
                                COALESCE(
                                    l12fbid_cp,
                                    l12fbid_am,
                                    l12fbid_pm,
                                    l12fbid_ap,
                                    l12fbid_rp,
                                    l12fbid_engaged,
                                    l12fbid_unmanaged
                               ) AS BIGINT
                           ) l12_reporting_fbid,
                            CAST(l12fbid_rp AS BIGINT) l12_reseller_fbid,
                            am_lwi_ind,
                            sales_forecast_prior_2w

                        FROM bpo_coverage_ad_acct_quota acc

                        LEFT JOIN acc_reduction red
                            ON acc.ad_account_id = red.ad_account_id

                         LEFT JOIN am_lwi_ind am_lwi_ind
                    on am_lwi_ind.account_Id = acc.ad_account_id

                        LEFT JOIN d_geo
                            ON LOWER(acc.advertiser_country) = LOWER(d_geo.country_abbr)

                        WHERE
                            ds = '<DATEID>'




           ),
            Terr AS (
                SELECT
                    CAST(l12_territory_fbid AS BIGINT) l12_territory_fbid,
                    l10_territory_fbid,
                    l8_territory_fbid,
                    l6_territory_fbid,
                    l4_territory_fbid,
                    l2_territory_fbid,
                    l12_territory_id,
                    l12_territory_name,
                    l10_territory_name,
                    l8_territory_name,
                    l6_territory_name,
                    l4_territory_name,
                    l2_territory_name,
                    user_name_mgr_12,
                    user_name_mgr_10,
                    user_name_mgr_8,
                    user_name_mgr_6,
                    full_name_mgr_12,
                    full_name_mgr_10,
                    full_name_mgr_8,
                    full_name_mgr_6,
                    full_name_mgr_4,
                    full_name_mgr_2

                FROM sales_ops_forecast_territory_flat flat
                WHERE
                    flat.ds = '<DATEID>'

           ),
            EMP AS (
                SELECT
                    workplace_fbid,
                    preferred_name,
                    unix_username,
                    manager_employee_id,
                    employee_id,
                    personal_fbid,
                    hire_date,
                    manager,
                    manager_username

                FROM <TABLE:bpo_coverage_asis_stg_emp_map>


                WHERE
                    Ds = '<DATEID>'
           )

            SELECT
                id_d_ad_account,
                acc.ad_account_id,
                ad_account_name,
                advertiser_name,
                id_dh_territory,
                advertiser_country,
                advertiser_vertical,
                advertiser_sub_vertical,
                gvm_vertical_name,
                gvm_sub_vertical_name,
                specialty,
                legacy_advertiser_vertical,
                legacy_advertiser_sub_vertical,
                NULL region,
                NULL sub_region,
                fraud_ind,
                is_deleted,
                is_house_account,
                is_fcast_eligible,
                advertiser_fbid,
                advertiser_sfid,
                ultimate_parent_fbid,
                ultimate_parent_sfid,
                ultimate_parent_name,
                planning_agency_name,
                planning_agency_fbid,
                planning_agency_sfid,
                planning_agency_ult_fbid,
                planning_agency_ult_sfid,
                planning_agency_ult_name,
                split_cp,
                split,
                rep_fbid_cp,
                NULL as id_d_employee,
                cp.preferred_name as client_partner,
                cp.unix_username as cp_username,
                cp.hire_date as cp_start_date,
                cp.manager as cp_manager,
                cp.manager_username  as cp_manager_username,
                l12fbid_cp,
                rep_fbid_am,
                am.preferred_name as account_manager,
                am.unix_username as am_username,
                am.hire_date as am_start_date,
                am.manager as am_manager,
                am.manager_username  as am_manager_username,
                l12fbid_am,
                split_am,
                rep_fbid_pm,
                pm.preferred_name as partner_manager,
                pm.unix_username as pm_username,
                pm.hire_date as pm_start_date,
                pm.manager as pm_manager,
                pm.manager_username  as pm_manager_username,
                l12fbid_pm,
                split_pm,
                rep_fbid_sp,
                ap.preferred_name as agency_partner,
                ap.unix_username as ap_username,
                ap.hire_date as ap_start_date,
                ap.manager as ap_manager,
                ap.manager_username  as ap_manager_username,
                l12fbid_sp,
                split_sp,
                rep_fbid_ap,
                l12fbid_ap,
                split_ap,
                is_gpa,
                l12fbid_unmanaged,
                is_staged,
                reseller_fbid,
                reseller_sfid,
                reseller_name,
                rep_fbid_me,
                rep_fbid_rp,
                l12fbid_me,
                l12fbid_rp,
                split_me,
                split_rp,
                dq_flags_me,
                dq_flags_rp,
                rep_fbid_fcast,
                l12fbid_fcast,
                program,
                market,
                NULL lwi_only_flag,
                NULL newbie_flag,
                NULL low_spender_90d_flag,
                NULL ig_boost_only_flag,
                NULL sss_flag,
                NULL seasonal_flag,
                rep_role,
                is_closed,
                is_high_risk,
                is_pci,
                is_covered,
                is_banned,
                is_ou_eligible,
                id_d_customer_account_adv,
                advertiser_coverage_model_daa,
                advertiser_program_daa,
                agency_coverage_model_daa,
                is_gat,
                is_gcm,
                is_magic93,
                newbie_type,
                COALESCE(adv.l12_territory_name,'Unassigned') l12_advertiser_territory,
                COALESCE(adv.l10_territory_name,'Unassigned') l10_advertiser_territory,
                COALESCE(adv.l8_territory_name,'Unassigned') l8_advertiser_territory,
                COALESCE(adv.l6_territory_name,'Unassigned') l6_advertiser_territory,
                COALESCE(adv.l4_territory_name,'Unassigned') l4_advertiser_territory,
                COALESCE(adv.l2_territory_name,'Unassigned') l2_advertiser_territory,
                adv.user_name_mgr_12 l12_usern_advertiser_territory,
                adv.user_name_mgr_10 l10_usern_advertiser_territory,
                adv.full_name_mgr_12 l12_manager_advertiser_territory,
                adv.user_name_mgr_10 l10_manager_advertiser_territory,
                adv.user_name_mgr_8 l8_manager_advertiser_territory,
                agc.l12_territory_name AS l12_agency_territory,
                agc.l10_territory_name AS l10_agency_territory,
                agc.l8_territory_name AS l8_agency_territory,
                agc.l6_territory_name AS l6_agency_territory,
                agc.l4_territory_name AS l4_agency_territory,
                agc.l2_territory_name AS l2_agency_territory,
                agc.user_name_mgr_12 AS l12_usern_agency_territory,
                agc.user_name_mgr_10 AS l10_usern_agency_territory,
                agc.user_name_mgr_12 AS l12_manager_agency_territory,
                agc.user_name_mgr_10 AS l10_manager_agency_territory,
                agc.user_name_mgr_8 AS l8_manager_agency_territory,
                CAST(ad_account_l4_fbid AS BIGINT) ad_account_l4_fbid,
                CAST(ad_account_l8_fbid AS BIGINT) ad_account_l8_fbid,
                CAST(ad_account_l10_fbid AS BIGINT) ad_account_l10_fbid,
                CAST(ad_account_l12_fbid AS BIGINT) ad_account_l12_fbid,
                NULL gms_optimal_target,
                NULL gms_liquidity_target,
                NULL gso_optimal_target,
                NULL gso_liquidity_target,
                NULL smb_optimal_target,
                NULL smb_liquidity_target,
                NULL l4_optimal_target,
                NULL l4_liquidity_target,
                NULL l6_optimal_target,
                NULL l6_liquidity_target,
                NULL l8_optimal_target,
                NULL l8_liquidity_target,
                NULL l10_optimal_target,
                NULL l10_liquidity_target,
                NULL l12_optimal_target,
                NULL l12_liquidity_target,
                adv_quota,
                agency_quota,
                sales_forecast,
                sales_forecast_prior,
                sbg_quota,
                optimal_product_goal optimal_goal,
                optimal_liquidity_goal liquidity_goal,
                COALESCE(reporting.l12_territory_name,'Unassigned') as l12_reporting_territory,
                COALESCE(reporting.l10_territory_name,'Unassigned') as l10_reporting_territory,
                COALESCE(reporting.l8_territory_name,'Unassigned') as l8_reporting_territory,
                COALESCE(reporting.l6_territory_name,'Unassigned') as l6_reporting_territory,
                COALESCE(reporting.l4_territory_name,'Unassigned') as l4_reporting_territory,
                COALESCE(reporting.l2_territory_name,'Unassigned') as l2_reporting_territory,
                segment_cq segmentation,
                reporting.user_name_mgr_12 l12_reporting_terr_mgr,
                reporting.user_name_mgr_10 l10_reporting_terr_mgr,
                reporting.user_name_mgr_8 l8_reporting_terr_mgr,
                reporting.user_name_mgr_6 l6_reporting_terr_mgr,
                agc.full_name_mgr_6 l6_manager_agency_territory,
                csm.preferred_name as client_solutions_manager,
                csm.unix_username as csm_username,
                csm.hire_date as csm_start_date,
                csm.manager as csm_manager,
                csm.manager_username  as csm_manager_username,
                rp.preferred_name as reseller_partner,
                rp.unix_username as rp_username,
                rp.hire_date as rp_start_date,
                rp.preferred_name as rp_manager,
                rp.manager_username  as rp_manager_username,
                asm.preferred_name as agency_solutions_manager,
                asm.unix_username as asm_username,
                asm.hire_date as asm_start_date,
                asm.manager as asm_manager,
                asm.manager_username as asm_manager_username,
                rep_fbid_asm,
                rep_fbid_csm,
                acc.region sales_adv_region,
                acc.sub_region sales_adv_subregion,
                NULL sales_adv_country_group,
                NULL program_optimal_target,
                NULL program_liquidity_target,
                COALESCE(terr_rp.l12_territory_name, 'unmanaged') l12_reseller_territory,
                COALESCE(terr_rp.l10_territory_name, 'unmanaged') l10_reseller_territory,
                COALESCE(terr_rp.l8_territory_name, 'unmanaged') l8_reseller_territory,
                COALESCE(terr_rp.l6_territory_name, 'unmanaged') l6_reseller_territory,
                COALESCE(terr_rp.l4_territory_name, 'unmanaged') l4_reseller_territory,
                COALESCE(terr_rp.l2_territory_name, 'unmanaged') l2_reseller_territory,
                COALESCE(terr_rp.user_name_mgr_12, 'unmanaged') l12_reseller_terr_mgr,
                COALESCE(terr_rp.user_name_mgr_10, 'unmanaged') l10_reseller_terr_mgr,
                COALESCE(terr_rp.user_name_mgr_8, 'unmanaged') l8_reseller_terr_mgr,
                COALESCE(terr_rp.user_name_mgr_6, 'unmanaged') l6_reseller_terr_mgr,
                reseller_quota,
                country_agc,
                market_agc,
                region_agc,
                sub_region_agc,
                business_type_adv,
                business_type_agc,
                planning_agency_operating_co,
                program_agency,
                china_export_advertiser,
                export_advertiser_country,
                billing_country_adv,
                billing_region_adv,
                billing_country_agc,
                billing_region_agc,
                hq_country_adv,
                hq_region_adv,
                hq_country_agc,
                hq_region_agc,
                inm_optimal_goal,
                agc_optimal_goal,
                CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts,
                legacy_gvm_vertical_name_v2,
                legacy_gvm_sub_vertical_name_v2,
                ultimate_parent_vertical_name_v2,
                subsegment,
                inm_dr_resilience_goal,
                scaled_dr_resilience_goal,
                revenue_segment,
                am_lwi_ind,
                sales_forecast_prior_2w

                FROM ACC


                LEFT JOIN terr reporting
                    ON reporting.l12_territory_fbid = acc.l12_reporting_fbid

                LEFT JOIN terr agc
                    ON agc.l12_territory_fbid = acc.l12_agency_fbid

                LEFT JOIN terr terr_rp
                    ON terr_rp.l12_territory_fbid = acc.l12_reseller_fbid

                LEFT JOIN terr adv
                    on adv.l12_territory_fbid = acc.l12_advertiser_fbid



                LEFT JOIN emp cp
                    ON CAST(rep_fbid_cp AS BIGINT) = cp.personal_fbid

                LEFT JOIN emp csm
                    ON CAST(rep_fbid_csm AS BIGINT) = csm.personal_fbid


                LEFT JOIN emp am
                    ON CAST(rep_fbid_am AS BIGINT) = am.personal_fbid


                LEFT JOIN emp pm
                    ON CAST(rep_fbid_pm AS BIGINT) = pm.personal_fbid

                LEFT JOIN emp ap
                    ON CAST(rep_fbid_ap AS BIGINT) = ap.personal_fbid

                LEFT JOIN emp rp
                    ON CAST(rep_fbid_rp AS BIGINT) = rp.personal_fbid


                LEFT JOIN emp asm
                    ON CAST(rep_fbid_asm as BIGINT) = asm.personal_fbid



                WHERE
                        (     reduction= 1
                            OR adv_quota IS NOT NULL
                            OR agency_quota IS NOT NULL
                            OR sbg_quota IS NOT NULL
                            OR sales_forecast IS NOT NULL
                            OR reseller_quota IS NOT NULL)
        """

    def get_name(self):
        """@docstring returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """@docstring returns the create statement as a string"""
        return self.create

    def get_select(self):
        """@docstring returns the select statement as a string"""
        return self.select


class BpoCoverageAsisStg2:
    """@docstring
    Object to store DDL and DML for table bpo_coverage_asis_stg_2
    """

    def __init__(self):
        self.name = "<TABLE:bpo_coverage_asis_stg_2>"
        self.create = """
                CREATE TABLE IF NOT EXISTS <TABLE:bpo_coverage_asis_stg_2>(
                    date_id varchar,
                    id_d_ad_account bigint,
                    ad_account_id bigint,
                    ad_account_name varchar,
                    advertiser_name varchar,
                    id_dh_territory bigint,
                    advertiser_country varchar,
                    advertiser_vertical varchar,
                    advertiser_sub_vertical varchar,
                    gvm_vertical_name varchar,
                    gvm_sub_vertical_name varchar,
                    specialty VARCHAR,
                    legacy_advertiser_sub_vertical VARCHAR,
                    legacy_advertiser_vertical VARCHAR,
                    region varchar,
                    sub_region varchar,
                    fraud_ind varchar,
                    is_deleted varchar,
                    is_house_account bigint,
                    is_fcast_eligible boolean,
                    advertiser_fbid varchar,
                    advertiser_sfid varchar,
                    ultimate_parent_fbid varchar,
                    ultimate_parent_sfid varchar,
                    ultimate_parent_name varchar,
                    planning_agency_name varchar,
                    planning_agency_fbid varchar,
                    planning_agency_sfid varchar,
                    planning_agency_ult_fbid varchar,
                    planning_agency_ult_sfid varchar,
                    planning_agency_ult_name varchar,
                    split_cp varchar,
                    split double,
                    id_d_employee bigint,
                    rep_fbid_cp varchar,
                    client_partner varchar,
                    cp_username varchar,
                    cp_start_date varchar,
                    cp_manager varchar,
                    cp_manager_username varchar,
                    l12fbid_cp varchar,
                    rep_fbid_am varchar,
                    account_manager varchar,
                    am_username varchar,
                    am_start_date varchar,
                    am_manager varchar,
                    am_manager_username varchar,
                    l12fbid_am varchar,
                    split_am varchar,
                    rep_fbid_pm varchar,
                    partner_manager varchar,
                    pm_username varchar,
                    pm_start_date varchar,
                    pm_manager varchar,
                    pm_manager_username varchar,
                    l12fbid_pm varchar,
                    split_pm varchar,
                    rep_fbid_sp varchar,
                    agency_partner varchar,
                    ap_username varchar,
                    ap_start_date varchar,
                    ap_manager varchar,
                    ap_manager_username varchar,
                    l12fbid_sp varchar,
                    split_sp varchar,
                    rep_fbid_ap varchar,
                    l12fbid_ap varchar,
                    split_ap varchar,
                    is_gpa boolean,
                    l12fbid_unmanaged varchar,
                    is_staged boolean,
                    reseller_fbid varchar,
                    reseller_sfid varchar,
                    reseller_name varchar,
                    rep_fbid_me varchar,
                    rep_fbid_rp varchar,
                    l12fbid_me varchar,
                    l12fbid_rp varchar,
                    split_me varchar,
                    split_rp varchar,
                    dq_flags_me map(varchar, varchar),
                    dq_flags_rp map(varchar, varchar),
                    rep_fbid_fcast varchar,
                    l12fbid_fcast varchar,
                    program varchar,
                    market varchar,
                    lwi_only_flag boolean,
                    newbie_flag boolean,
                    low_spender_90d_flag boolean,
                    ig_boost_only_flag boolean,
                    sss_flag boolean,
                    seasonal_flag boolean,
                    rep_role varchar,
                    is_closed boolean,
                    is_high_risk boolean,
                    is_pci boolean,
                    is_covered boolean,
                    is_banned boolean,
                    is_ou_eligible boolean,
                    id_d_customer_account_adv bigint,
                    advertiser_coverage_model_daa varchar,
                    advertiser_program_daa varchar,
                    agency_coverage_model_daa varchar,
                    is_gat boolean,
                    is_gcm boolean,
                    is_magic93 boolean,
                    newbie_type varchar,
                    l12_advertiser_territory varchar,
                    l10_advertiser_territory varchar,
                    l8_advertiser_territory varchar,
                    l6_advertiser_territory varchar,
                    l4_advertiser_territory varchar,
                    l2_advertiser_territory varchar,
                    l12_usern_advertiser_territory varchar,
                    l10_usern_advertiser_territory varchar,
                    l12_manager_advertiser_territory varchar,
                    l10_manager_advertiser_territory varchar,
                    l8_manager_advertiser_territory varchar,
                    l12_agency_territory varchar,
                    l10_agency_territory varchar,
                    l8_agency_territory varchar,
                    l6_agency_territory varchar,
                    l4_agency_territory varchar,
                    l2_agency_territory varchar,
                    l12_usern_agency_territory varchar,
                    l10_usern_agency_territory varchar,
                    l12_manager_agency_territory varchar,
                    l10_manager_agency_territory varchar,
                    l8_manager_agency_territory varchar,
                    ad_account_l4_fbid BIGINT,
                    ad_account_l8_fbid BIGINT,
                    ad_account_l10_fbid BIGINT,
                    ad_account_l12_fbid BIGINT,
                    adv_quota DOUBLE,
                    agency_quota DOUBLE,
                    sales_forecast DOUBLE,
                    sales_forecast_prior DOUBLE,
                    sbg_quota DOUBLE,
                    optimal_goal DOUBLE,
                    liquidity_goal DOUBLE,
                    gms_optimal_target double,
                    gms_liquidity_target double,
                    gso_optimal_target double,
                    gso_liquidity_target double,
                    smb_optimal_target double,
                    smb_liquidity_target double,
                    l4_optimal_target double,
                    l4_liquidity_target double,
                    l8_optimal_target double,
                    l8_liquidity_target double,
                    l10_optimal_target double,
                    l10_liquidity_target double,
                    l12_optimal_target double,
                    l12_liquidity_target double,
                    l12_reporting_territory VARCHAR,
                    l10_reporting_territory VARCHAR,
                    l8_reporting_territory  VARCHAR,
                    l6_reporting_territory  VARCHAR,
                    l4_reporting_territory  VARCHAR,
                    l2_reporting_territory  VARCHAR,
                    segmentation VARCHAR,
                    l12_reporting_terr_mgr VARCHAR,
                    l10_reporting_terr_mgr VARCHAR,
                    l8_reporting_terr_mgr VARCHAR,
                    l6_reporting_terr_mgr VARCHAR,
                    l6_manager_agency_territory VARCHAR,
                    rep_fbid_csm VARCHAR COMMENT 'FBID for Client Solutions Manager https://fburl.com/wut/cad62z1j',
                    client_solutions_manager VARCHAR COMMENT 'full name for Client Solutions Manager',
                    csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
                    csm_start_date varchar COMMENT 'Start Date of CSM for a given ad account',
                    csm_manager varchar COMMENT 'Full name for a CSMs manager',
                    csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
                    rep_fbid_asm VARCHAR COMMENT 'FBID for Agency Solutions Manager',
                    agency_solutions_manager VARCHAR COMMENT 'Full name for ASM' ,
                    asm_username varchar COMMENT 'UnixName for ASM',
                    asm_start_date varchar COMMENT 'start date of an asm for a given ad account ',
                    asm_manager varchar COMMENT 'Full name for an  ASMs manager',
                    asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
                    reseller_partner VARCHAR COMMENT 'Full name for reseller partner',
                    rp_username varchar COMMENT 'UnixName for reseller partner',
                    rp_start_date varchar COMMENT 'start date for a reseller partner for a given ad account',
                    rp_manager varchar COMMENT 'Full name for an RPs manager',
                    rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
                    sales_adv_country_group VARCHAR,
                    sales_adv_subregion VARCHAR,
                    sales_adv_region VARCHAR,
                    program_optimal_target double,
                    program_liquidity_target double,
                    l6_optimal_target double,
                    l6_liquidity_target double,
                    l12_reseller_territory VARCHAR,
                    l10_reseller_territory VARCHAR,
                    l8_reseller_territory  VARCHAR,
                    l6_reseller_territory  VARCHAR,
                    l4_reseller_territory  VARCHAR,
                    l2_reseller_territory  VARCHAR,
                    l12_reseller_terr_mgr VARCHAR,
                    l10_reseller_terr_mgr VARCHAR,
                    l8_reseller_terr_mgr  VARCHAR,
                    l6_reseller_terr_mgr  VARCHAR,
                    reseller_quota DOUBLE,
                    country_agc VARCHAR,
                    market_agc VARCHAR,
                    region_agc VARCHAR,
                    sub_region_agc VARCHAR,
                    business_type_adv VARCHAR,
                    business_type_agc VARCHAR,
                    planning_agency_operating_co VARCHAR,
                    program_agency VARCHAR,
                    china_export_advertiser VARCHAR,
                    export_advertiser_country VARCHAR,
                    billing_country_adv VARCHAR,
                    billing_region_adv VARCHAR,
                    billing_country_agc VARCHAR,
                    billing_region_agc VARCHAR,
                    hq_country_adv VARCHAR,
                    hq_region_adv VARCHAR,
                    hq_country_agc VARCHAR,
                    hq_region_agc VARCHAR,
                    inm_optimal_goal DOUBLE,
                    agc_optimal_goal DOUBLE,
                    legacy_gvm_vertical_name_v2 VARCHAR,
                    legacy_gvm_sub_vertical_name_v2 VARCHAR,
                    ultimate_parent_vertical_name_v2 VARCHAR,
                    revenue_segment VARCHAR,
                    subsegment VARCHAR,
                    inm_dr_resilience_goal DOUBLE,
                    scaled_dr_resilience_goal DOUBLE,
                    am_lwi_ind VARCHAR,
                    sales_forecast_prior_2w DOUBLE,
                    ts VARCHAR,
                    ds varchar
                   )
                        WITH (partitioned_by = ARRAY['ds'],
                            retention_days = <RETENTION:90>,
                            uii=false)
                                    """
        self.select = """
                WITH
                DATES as
                ( Select date_id from d_date
                where
                quarter_id = '<quarter_id>' and DS = '<LATEST_DS:d_date>'
               )
                            SELECT
                            date_id
                            ,id_d_ad_account
                            ,ad_account_id
                            ,ad_account_name
                            ,advertiser_name
                            ,id_dh_territory
                            ,advertiser_country
                            ,advertiser_vertical
                            ,advertiser_sub_vertical
                            ,gvm_vertical_name
                            ,gvm_sub_vertical_name
                            ,specialty
                            ,legacy_advertiser_sub_vertical
                            ,legacy_advertiser_vertical
                            ,region
                            ,sub_region
                            ,fraud_ind
                            ,is_deleted
                            ,is_house_account
                            ,is_fcast_eligible
                            ,advertiser_fbid
                            ,advertiser_sfid
                            ,ultimate_parent_fbid
                            ,ultimate_parent_sfid
                            ,ultimate_parent_name
                            ,planning_agency_name
                            ,planning_agency_fbid
                            ,planning_agency_sfid
                            ,planning_agency_ult_fbid
                            ,planning_agency_ult_sfid
                            ,planning_agency_ult_name
                            ,id_d_employee
                            ,split_cp
                            ,split
                            ,rep_fbid_cp
                            ,client_partner
                            ,cp_username
                            ,cp_start_date
                            ,cp_manager
                            ,cp_manager_username
                            ,l12fbid_cp
                            ,rep_fbid_am
                            ,account_manager
                            ,am_username
                            ,am_start_date
                            ,am_manager
                            ,am_manager_username
                            ,l12fbid_am
                            ,split_am
                            ,rep_fbid_pm
                            ,partner_manager
                            ,pm_username
                            ,pm_start_date
                            ,pm_manager
                            ,pm_manager_username
                            ,l12fbid_pm
                            ,split_pm
                            ,rep_fbid_sp
                            ,agency_partner
                            ,ap_username
                            ,ap_start_date
                            ,ap_manager
                            ,ap_manager_username
                            ,l12fbid_sp
                            ,split_sp
                            ,rep_fbid_ap
                            ,l12fbid_ap
                            ,split_ap
                            ,is_gpa
                            ,l12fbid_unmanaged
                            ,is_staged
                            ,reseller_fbid
                            ,reseller_sfid
                            ,reseller_name
                            ,rep_fbid_me
                            ,rep_fbid_rp
                            ,l12fbid_me
                            ,l12fbid_rp
                            ,split_me
                            ,split_rp
                            ,dq_flags_me
                            ,dq_flags_rp
                            ,rep_fbid_fcast
                            ,l12fbid_fcast
                            ,program
                            ,market
                            ,lwi_only_flag
                            ,newbie_flag
                            ,low_spender_90d_flag
                            ,ig_boost_only_flag
                            ,sss_flag
                            ,seasonal_flag
                            ,rep_role
                            ,is_closed
                            ,is_high_risk
                            ,is_pci
                            ,is_covered
                            ,is_banned
                            ,is_ou_eligible
                            ,id_d_customer_account_adv
                            ,advertiser_coverage_model_daa
                            ,advertiser_program_daa
                            ,agency_coverage_model_daa
                            ,is_gat
                            ,is_gcm
                            ,is_magic93
                            ,newbie_type
                            ,l12_advertiser_territory
                            ,l10_advertiser_territory
                            ,l8_advertiser_territory
                            ,l4_advertiser_territory
                            ,l6_advertiser_territory
                            ,l2_advertiser_territory
                            ,l12_usern_advertiser_territory
                            ,l10_usern_advertiser_territory
                            ,l12_manager_advertiser_territory
                            ,l10_manager_advertiser_territory
                            ,l8_manager_advertiser_territory
                            ,case when program_agency = 'Unmanaged Agency' then 'Unmanaged Agency' else l12_agency_territory end l12_agency_territory
                            ,case when program_agency = 'Unmanaged Agency' then 'Unmanaged Agency' else l10_agency_territory end l10_agency_territory
                            ,case when program_agency = 'Unmanaged Agency' then 'Unmanaged Agency' else l8_agency_territory end l8_agency_territory
                            ,case when program_agency = 'Unmanaged Agency' then 'Unmanaged Agency' else l6_agency_territory end l6_agency_territory
                            ,case when program_agency = 'Unmanaged Agency' then 'Unmanaged Agency' else l4_agency_territory end l4_agency_territory
                            ,case when program_agency = 'Unmanaged Agency' then 'Unmanaged Agency' else l2_agency_territory end l2_agency_territory
                            ,l12_usern_agency_territory
                            ,l10_usern_agency_territory
                            ,l12_manager_agency_territory
                            ,l10_manager_agency_territory
                            ,l8_manager_agency_territory
                            ,ad_account_l4_fbid
                            ,ad_account_l8_fbid
                            ,ad_account_l10_fbid
                            ,ad_account_l12_fbid
                            ,adv_quota
                            ,agency_quota
                            ,sales_forecast
                            ,sales_forecast_prior
                            ,sbg_quota
                            ,optimal_goal
                            ,liquidity_goal
                            ,gms_optimal_target
                            ,gms_liquidity_target
                            ,gso_optimal_target
                            ,gso_liquidity_target
                            ,smb_optimal_target
                            ,smb_liquidity_target
                            ,l4_optimal_target
                            ,l4_liquidity_target
                            ,l8_optimal_target
                            ,l8_liquidity_target
                            ,l10_optimal_target
                            ,l10_liquidity_target
                            ,l12_optimal_target
                            ,l12_liquidity_target
                            ,l12_reporting_territory
                            ,l10_reporting_territory
                            ,l8_reporting_territory
                            ,l4_reporting_territory
                            ,l6_reporting_territory
                            ,l2_reporting_territory
                            ,segmentation
                            ,l12_reporting_terr_mgr
                            ,l10_reporting_terr_mgr
                            ,l8_reporting_terr_mgr
                            ,l6_reporting_terr_mgr
                            ,l6_manager_agency_territory
                            ,rep_fbid_csm
                            ,client_solutions_manager
                            ,csm_username
                            ,csm_start_date
                            ,csm_manager
                            ,csm_manager_username
                            ,rep_fbid_asm
                            ,agency_solutions_manager
                            ,asm_username
                            ,asm_start_date
                            ,asm_manager
                            ,asm_manager_username
                            ,reseller_partner
                            ,rp_username
                            ,rp_start_date
                            ,rp_manager
                            ,rp_manager_username
                            ,sales_adv_country_group
                            ,sales_adv_subregion
                            ,sales_adv_region
                            ,program_optimal_target
                            ,program_liquidity_target
                            ,l6_optimal_target
                            ,l6_liquidity_target
                            ,l12_reseller_territory
                            ,l10_reseller_territory
                            ,l8_reseller_territory
                            ,l6_reseller_territory
                            ,l4_reseller_territory
                            ,l2_reseller_territory
                            ,l12_reseller_terr_mgr
                            ,l10_reseller_terr_mgr
                            ,l8_reseller_terr_mgr
                            ,l6_reseller_terr_mgr
                            ,reseller_quota
                            ,country_agc
                            ,market_agc
                            ,region_agc
                            ,sub_region_agc
                            ,business_type_adv
                            ,business_type_agc
                            ,planning_agency_operating_co
                            ,program_agency
                            ,china_export_advertiser
                            ,export_advertiser_country
                            ,billing_country_adv
                            ,billing_region_adv
                            ,billing_country_agc
                            ,billing_region_agc
                            ,hq_country_adv
                            ,hq_region_adv
                            ,hq_country_agc
                            ,hq_region_agc
                            ,inm_optimal_goal
                            ,agc_optimal_goal
                            ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
                            ,legacy_gvm_vertical_name_v2
                            ,legacy_gvm_sub_vertical_name_v2
                            ,ultimate_parent_vertical_name_v2
                            ,revenue_segment
                            ,subsegment
                            ,inm_dr_resilience_goal
                            ,scaled_dr_resilience_goal
                            ,am_lwi_ind
                            ,sales_forecast_prior_2w

                                                        FROM <TABLE:bpo_coverage_asis_stg_1>

                                                        CROSS join dates
                                                        WHERE ds = '<DATEID>'
                                                    AND  lower(l4_reporting_territory) not like '%test%'

                           """

    def get_name(self):
        """@docstring returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """@docstring returns the create statement as a string"""
        return self.create

    def get_select(self):
        """@docstring returns the select statement as a string"""
        return self.select


class BpoCoverageAsisStg3:
    """@docstring"""

    def __init__(self):
        self.name = "<TABLE:bpo_coverage_asis_stg_3>"
        self.select = """
            WITH cq_rev as (
                         SELECT  date_id,
                               account_id ad_account_id,
                               SUM(asis_rec_rev)  as cq_revenue,
                               SUM(optimal_revn) cq_optimal,
                               SUM(product_resilient_rec_rev) product_resilient_rec_rev,
                               SUM(ebr_usd_rec_rev) ebr_usd_rec_rev,
                               SUM(capi_ebr_revenue) capi_ebr_revenue





                        FROM staging_bpt_fct_l4_gms_rec_rev_data
                        WHERE
                             ds = '<DATEID>'
                             and quarter_id = '<quarter_id>'

                        GROUP BY 1, 2

                       ),
            pq_rev as (
                       SELECT
                             cast((cast(date_id as date) + interval '3' month) as varchar) as date_id,
                             account_id ad_account_id,
                             SUM(asis_rec_rev) as pq_revenue,
                             SUM(optimal_revn) pq_optimal,
                             SUM(product_resilient_rec_rev) product_resilient_rec_rev,
                             SUM(ebr_usd_rec_rev) ebr_usd_rec_rev,
                             SUM(capi_ebr_revenue) capi_ebr_revenue



                        FROM staging_bpt_fct_l4_gms_rec_rev_data

                        WHERE
                             ds = '<DATEID>'
                             and quarter_id = '<prev_quarter_id>'

                        GROUP By 1, 2



                      ),
              ly_rev as (
                SELECT
                       cast((cast(date_id as date) + interval '12' month) as varchar) as date_id,
                       account_id ad_account_id,
                       SUM(asis_rec_rev) as ly_revenue,
                       SUM(optimal_revn) ly_optimal,
                       SUM(product_resilient_rec_rev) product_resilient_rec_rev,
                       SUM(ebr_usd_rec_rev) ebr_usd_rec_rev,
                       SUM(capi_ebr_revenue) capi_ebr_revenue




                        FROM staging_bpt_fct_l4_gms_rec_rev_data
                        WHERE
                             ds = '<DATEID>'
                             and quarter_id = cast((cast('<quarter_id>' as date) - interval '1' year) as varchar)

                        GROUP BY 1, 2

           ),

           l2y_rev as (
                SELECT
                    cast((cast(date_id as date) + interval '24' month) as varchar) as date_id,
                    account_id ad_account_id,
                    SUM(asis_rec_rev) as l2y_revenue
                FROM staging_bpt_fct_l4_gms_rec_rev_data
                WHERE
                    ds = '<DATEID>'
                    and quarter_id = cast((cast('<quarter_id>' as date) - interval '2' year) as varchar)
                GROUP BY
                    1, 2

           ),

           l2yq_rev as (
                SELECT
                    ad_account_id,
                    SUM(l2y_revenue) as l2yq_revenue
                FROM l2y_rev
                group by 1

           ),

            lyq_rev as (
                SELECT
                    ad_account_id,
                    SUM(ly_revenue) as lyq_revenue,
                    SUM(ly_optimal) as lyq_optimal,
                    SUM(product_resilient_rec_rev) product_resilient_rec_rev,
                    SUM(ebr_usd_rec_rev) ebr_usd_rec_rev,
                    SUM(capi_ebr_revenue) capi_ebr_revenue


                FROM ly_rev

                group by 1

           ),

                    quarter_date as (Select
                    cast('<quarter_id>' as date) as quarter_id,
                    (cast('<quarter_id>' as date) + INTERVAL '3' MONTH) next_quarter_id,
                            max(cast(date_id as date)) asofdate

                    from
                    staging_bpt_fct_l4_gms_rec_rev_data
                        where (asis_rec_rev is not null or asis_rec_rev > 0) and ds = '<DATEID>'
                            group by 1,2),
                        quarter_dates as
                            ( Select

                    quarter_id,
                    next_quarter_id,
                    asofdate,
                    DATE_DIFF('day',(asofdate),next_quarter_id- INTERVAL '1' DAY) days_left_in_quarter,
                    DATE_DIFF('day',(asofdate - INTERVAL '7' DAY),next_quarter_id- INTERVAL '1' DAY) days_left_in_quarter_prior,
                    DATE_DIFF('day', quarter_id, next_quarter_id) days_total_in_quarter,
                        DATE_DIFF('day', quarter_id, asofdate + INTERVAL '1' DAY) days_closed_in_quarter

                        from
                        quarter_date),
                        L7d_rev as (
                            SELECT
                                    ad_account_id,
                                    Sum(case when date_id >= '<DATEID-6>' then cq_revenue else null end) l7d_revenue,
                                    Sum(case when date_id >= '<DATEID-13>' and date_id <= '<DATEID-7>' then cq_revenue else null end)  L7d_revenue_prior,
                                    SUM(case when days_closed_in_quarter >= 7 then
                                    ((case when date_id >= '<DATEID-6>' then
                                    cq_revenue else null end) / 7) else
                                    ((case when date_id >= '<DATEID>' then
                                    cq_revenue else null end) / days_closed_in_quarter) end) L7d_avg_revenue,
                                    SUM(case when days_closed_in_quarter >= 14 then  (case when date_id >= '<DATEID-13>' and date_id <= '<DATEID-7>' then cq_revenue else null end) / 7 else (case when date_id >= '<DATEID-13>' and date_id < '<DATEID-7>' then cq_revenue else null end) / (days_closed_in_quarter-7) end) L7d_avg_revenue_prior
                            FROM cq_rev
                            cross join quarter_dates
                            WHERE
                                    date_id >= '<DATEID-13>'

                                    group by 1
                           ),
                        L14d_rev as (
                            SELECT
                                    ad_account_id,
                                    Sum(case when date_id >= '<DATEID-13>' then cq_revenue else null end) l14d_revenue,
                                    Sum(case when date_id >= '<DATEID-27>' and date_id <= '<DATEID-14>' then cq_revenue else null end)  L14d_revenue_prior,
                                    SUM(case when days_closed_in_quarter >= 14 then
                                    ((case when date_id >= '<DATEID-13>' then
                                    cq_revenue else null end) / 14) else
                                    ((case when date_id >= '<DATEID>' then
                                    cq_revenue else null end) / days_closed_in_quarter) end) L14d_avg_revenue,
                                    SUM(case when days_closed_in_quarter >= 28 then  (case when date_id >= '<DATEID-27>' and date_id <= '<DATEID-14>' then cq_revenue else null end) / 14 else (case when date_id >= '<DATEID-27>' and date_id < '<DATEID-14>' then cq_revenue else null end) / (days_closed_in_quarter-14) end) L14d_avg_revenue_prior
                            FROM cq_rev
                            cross join quarter_dates
                            WHERE
                                    date_id >= '<DATEID-27>'

                                    group by 1
                           )

                        Select
                        coverage.date_id
                        ,coverage.id_d_ad_account
                        ,coverage.ad_account_id
                        ,coverage.ad_account_name
                        ,coverage.advertiser_name
                        ,coverage.id_dh_territory
                        ,coverage.advertiser_country
                        ,coverage.advertiser_vertical
                        ,coverage.advertiser_sub_vertical
                        ,coverage.gvm_vertical_name
                        ,coverage.gvm_sub_vertical_name
                        ,coverage.specialty
                        ,coverage.legacy_advertiser_sub_vertical
                        ,coverage.legacy_advertiser_vertical
                        ,coverage.region
                        ,coverage.sub_region
                        ,coverage.fraud_ind
                        ,coverage.is_deleted
                        ,coverage.is_house_account
                        ,coverage.is_fcast_eligible
                        ,coverage.advertiser_fbid
                        ,coverage.advertiser_sfid
                        ,coverage.ultimate_parent_fbid
                        ,coverage.ultimate_parent_sfid
                        ,coverage.ultimate_parent_name
                        ,coverage.planning_agency_name
                        ,coverage.planning_agency_fbid
                        ,coverage.planning_agency_sfid
                        ,coverage.planning_agency_ult_fbid
                        ,coverage.planning_agency_ult_sfid
                        ,coverage.planning_agency_ult_name
                        ,coverage.split_cp
                        ,coverage.split
                        ,coverage.id_d_employee
                        ,coverage.rep_fbid_cp
                        ,coverage.client_partner
                        ,coverage.cp_username
                        ,coverage.cp_start_date
                        ,coverage.cp_manager
                        ,coverage.cp_manager_username
                        ,coverage.l12fbid_cp
                        ,coverage.rep_fbid_am
                        ,coverage.account_manager
                        ,coverage.am_username
                        ,coverage.am_start_date
                        ,coverage.am_manager
                        ,coverage.am_manager_username
                        ,coverage.l12fbid_am
                        ,coverage.split_am
                        ,coverage.rep_fbid_pm
                        ,coverage.partner_manager
                        ,coverage.pm_username
                        ,coverage.pm_start_date
                        ,coverage.pm_manager
                        ,coverage.pm_manager_username
                        ,coverage.l12fbid_pm
                        ,coverage.split_pm
                        ,coverage.rep_fbid_sp
                        ,coverage.agency_partner
                        ,coverage.ap_username
                        ,coverage.ap_start_date
                        ,coverage.ap_manager
                        ,coverage.ap_manager_username
                        ,coverage.l12fbid_sp
                        ,coverage.split_sp
                        ,coverage.rep_fbid_ap
                        ,coverage.l12fbid_ap
                        ,coverage.split_ap
                        ,coverage.is_gpa
                        ,coverage.l12fbid_unmanaged
                        ,coverage.is_staged
                        ,coverage.reseller_fbid
                        ,coverage.reseller_sfid
                        ,coverage.reseller_name
                        ,coverage.rep_fbid_me
                        ,coverage.rep_fbid_rp
                        ,coverage.l12fbid_me
                        ,coverage.l12fbid_rp
                        ,coverage.split_me
                        ,coverage.split_rp
                        ,coverage.dq_flags_me
                        ,coverage.dq_flags_rp
                        ,coverage.rep_fbid_fcast
                        ,coverage.l12fbid_fcast
                        ,coverage.program
                        ,coverage.market
                        ,coverage.lwi_only_flag
                        ,coverage.newbie_flag
                        ,coverage.low_spender_90d_flag
                        ,coverage.ig_boost_only_flag
                        ,coverage.sss_flag
                        ,coverage.seasonal_flag
                        ,coverage.rep_role
                        ,coverage.is_closed
                        ,coverage.is_high_risk
                        ,coverage.is_pci
                        ,coverage.is_covered
                        ,coverage.is_banned
                        ,coverage.is_ou_eligible
                        ,coverage.id_d_customer_account_adv
                        ,coverage.advertiser_coverage_model_daa
                        ,coverage.advertiser_program_daa
                        ,coverage.agency_coverage_model_daa
                        ,coverage.is_gat
                        ,coverage.is_gcm
                        ,coverage.is_magic93
                        ,coverage.newbie_type
                        ,coverage.l12_advertiser_territory
                        ,coverage.l10_advertiser_territory
                        ,coverage.l8_advertiser_territory
                        ,coverage.l6_advertiser_territory
                        ,coverage.l4_advertiser_territory
                        ,coverage.l2_advertiser_territory
                        ,coverage.l12_usern_advertiser_territory
                        ,coverage.l10_usern_advertiser_territory
                        ,coverage.l12_manager_advertiser_territory
                        ,coverage.l10_manager_advertiser_territory
                        ,coverage.l8_manager_advertiser_territory
                        ,coverage.l12_agency_territory
                        ,coverage.l10_agency_territory
                        ,coverage.l8_agency_territory
                        ,coverage.l6_agency_territory
                        ,coverage.l4_agency_territory
                        ,coverage.l2_agency_territory
                        ,coverage.l12_usern_agency_territory
                        ,coverage.l10_usern_agency_territory
                        ,coverage.l12_manager_agency_territory
                        ,coverage.l10_manager_agency_territory
                        ,coverage.l8_manager_agency_territory
                        ,coverage.ad_account_l4_fbid
                        ,coverage.ad_account_l8_fbid
                        ,coverage.ad_account_l10_fbid
                        ,coverage.ad_account_l12_fbid
                        ,coverage.l12_reporting_territory
                        ,coverage.l10_reporting_territory
                        ,coverage.l8_reporting_territory
                        ,coverage.l6_reporting_territory
                        ,coverage.l4_reporting_territory
                        ,coverage.l2_reporting_territory
                        ,if(coverage.is_fcast_eligible, cq_rev.cq_revenue*coverage.split) cq_revenue
                        ,if(coverage.is_fcast_eligible, cq_rev.cq_optimal*coverage.split) cq_optimal
                        ,NULL cq_liquidity
                        ,if(coverage.is_fcast_eligible, pq_rev.pq_revenue*coverage.split) pq_revenue
                        ,if(coverage.is_fcast_eligible, pq_rev.pq_optimal*coverage.split) pq_optimal
                        ,NULL pq_liquidity
                        ,if(coverage.is_fcast_eligible, ly_rev.ly_revenue*coverage.split) ly_revenue
                        ,if(coverage.is_fcast_eligible, l2y_rev.l2y_revenue*coverage.split) l2y_revenue
                        ,if(coverage.is_fcast_eligible, ly_rev.ly_optimal*coverage.split) ly_optimal
                        ,NULL ly_liquidity
                        ,if(coverage.is_fcast_eligible, lyq_rev.lyq_revenue*coverage.split) lyq_revenue
                        ,if(coverage.is_fcast_eligible, l2yq_rev.l2yq_revenue*coverage.split) l2yq_revenue
                        ,if(coverage.is_fcast_eligible, lyq_rev.lyq_optimal*coverage.split) lyq_optimal
                        ,NULL lyq_liquidity
                        ,sbg_quota revenue_quota
                        ,optimal_goal optimal_quota
                        ,liquidity_goal liquidity_quota
                        ,coverage.adv_quota advertiser_quota
                        ,coverage.agency_quota agency_quota
                        ,coverage.sales_forecast sales_forecast
                        ,coverage.sales_forecast_prior sales_forecast_prior
                        ,coverage.sbg_quota sbg_quota
                        ,segmentation
                        ,l12_reporting_terr_mgr
                        ,l10_reporting_terr_mgr
                        ,l8_reporting_terr_mgr
                        ,l6_reporting_terr_mgr
                        ,l6_manager_agency_territory
                        ,coverage.rep_fbid_csm
                        ,coverage.client_solutions_manager
                        ,coverage.csm_username
                        ,coverage.csm_start_date
                        ,coverage.csm_manager
                        ,coverage.csm_manager_username
                        ,coverage.rep_fbid_asm
                        ,coverage.agency_solutions_manager
                        ,coverage.asm_username
                        ,coverage.asm_start_date
                        ,coverage.asm_manager
                        ,coverage.asm_manager_username
                        ,coverage.reseller_partner
                        ,coverage.rp_username
                        ,coverage.rp_start_date
                        ,coverage.rp_manager
                        ,coverage.rp_manager_username
                        ,coverage.sales_adv_country_group
                        ,coverage.sales_adv_subregion
                        ,coverage.sales_adv_region
                        ,l12_reseller_territory
                        ,l10_reseller_territory
                        ,l8_reseller_territory
                        ,l6_reseller_territory
                        ,l4_reseller_territory
                        ,l2_reseller_territory
                        ,l12_reseller_terr_mgr
                        ,l10_reseller_terr_mgr
                        ,l8_reseller_terr_mgr
                        ,l6_reseller_terr_mgr
                        ,reseller_quota
                        ,country_agc
                        ,market_agc
                        ,region_agc
                        ,sub_region_agc
                        ,business_type_adv
                        ,business_type_agc
                        ,planning_agency_operating_co
                        ,program_agency
                        ,china_export_advertiser
                        ,export_advertiser_country
                        ,billing_country_adv
                        ,billing_region_adv
                        ,billing_country_agc
                        ,billing_region_agc
                        ,hq_country_adv
                        ,hq_region_adv
                        ,hq_country_agc
                        ,hq_region_agc
                        ,inm_optimal_goal
                        ,agc_optimal_goal
                        ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
                        ,IF(coverage.is_fcast_eligible, l7d_rev.L7d_revenue*coverage.split) L7d_revenue
                        ,IF(coverage.is_fcast_eligible, l7d_rev.L7d_revenue_prior*coverage.split) L7d_revenue_prior
                        ,IF(coverage.is_fcast_eligible, l7d_rev.L7d_avg_revenue*coverage.split) L7d_avg_revenue
                        ,IF(coverage.is_fcast_eligible, l7d_rev.L7d_avg_revenue_prior*coverage.split) L7d_avg_revenue_prior
                        ,legacy_gvm_vertical_name_v2
                        ,legacy_gvm_sub_vertical_name_v2
                        ,ultimate_parent_vertical_name_v2
                        ,revenue_segment
                        ,subsegment
                        ,inm_dr_resilience_goal
                        ,scaled_dr_resilience_goal
                        ,CASE
                             WHEN revenue_segment = 'GBG In-Market'
                             THEN adv_quota * inm_dr_resilience_goal -- get dollar value of quota that will be goaled as dr resilient
                             WHEN revenue_segment = 'GBG Scaled'
                             THEN scaled_dr_resilience_goal -- dollar value divide by Scaled quota to get pct
                             ELSE 0
                        END dr_resilience_goal
                       ,IF(coverage.is_fcast_eligible, cq_rev.product_resilient_rec_rev * coverage.split) cq_product_resilient_rec_rev
                       ,IF(coverage.is_fcast_eligible, cq_rev.ebr_usd_rec_rev * coverage.split) cq_ebr_usd_rec_rev
                       ,IF(coverage.is_fcast_eligible, cq_rev.capi_ebr_revenue * coverage.split) cq_capi_ebr_revenue
                       ,IF(coverage.is_fcast_eligible, pq_rev.product_resilient_rec_rev * coverage.split) pq_product_resilient_rec_rev
                       ,IF(coverage.is_fcast_eligible, pq_rev.ebr_usd_rec_rev * coverage.split) pq_ebr_usd_rec_rev
                       ,IF(coverage.is_fcast_eligible, pq_rev.capi_ebr_revenue * coverage.split) pq_capi_ebr_revenue
                       ,IF(coverage.is_fcast_eligible, ly_rev.product_resilient_rec_rev * coverage.split) ly_product_resilient_rec_rev
                       ,IF(coverage.is_fcast_eligible, ly_rev.ebr_usd_rec_rev * coverage.split) ly_ebr_usd_rec_rev
                       ,IF(coverage.is_fcast_eligible, ly_rev.capi_ebr_revenue * coverage.split) ly_capi_ebr_revenue
                       ,IF(coverage.is_fcast_eligible, lyq_rev.product_resilient_rec_rev * coverage.split) lyq_product_resilient_rec_rev
                       ,IF(coverage.is_fcast_eligible, lyq_rev.ebr_usd_rec_rev * coverage.split) lyq_ebr_usd_rec_rev
                       ,IF(coverage.is_fcast_eligible, lyq_rev.capi_ebr_revenue * coverage.split) lyq_capi_ebr_revenue
                       ,am_lwi_ind
                       ,IF(coverage.is_fcast_eligible, l14d_rev.L14d_revenue*coverage.split) L14d_revenue
                        ,IF(coverage.is_fcast_eligible, l14d_rev.L14d_revenue_prior*coverage.split) L14d_revenue_prior
                        ,IF(coverage.is_fcast_eligible, l14d_rev.L14d_avg_revenue*coverage.split) L14d_avg_revenue
                        ,IF(coverage.is_fcast_eligible, l14d_rev.L14d_avg_revenue_prior*coverage.split) L14d_avg_revenue_prior
                        ,coverage.sales_forecast_prior_2w


                                        from <TABLE:bpo_coverage_asis_stg_2> coverage

                                        left join cq_rev on
                                        coverage.ad_account_id = cq_rev.ad_account_id
                                        and cq_rev.date_id = coverage.date_id

                                        left join pq_rev on
                                        coverage.ad_account_id = pq_rev.ad_account_id
                                        and pq_rev.date_id = coverage.date_id

                                        left join ly_rev on
                                        coverage.ad_account_id = ly_rev.ad_account_id
                                        and ly_rev.date_id = coverage.date_id

                                        left join l2y_rev on
                                        coverage.ad_account_id = l2y_rev.ad_account_id
                                        and l2y_rev.date_id = coverage.date_id

                                        left join lyq_rev on
                                        coverage.ad_account_id = lyq_rev.ad_account_id

                                        left join l2yq_rev on
                                        coverage.ad_account_id = l2yq_rev.ad_account_id

                                        left join l7d_rev on
                                        coverage.ad_account_id = l7d_rev.ad_account_id

                                        left join l14d_rev on
                                        coverage.ad_account_id = l14d_rev.ad_account_id

                                        where ds = '<DATEID>'
                                        AND (sbg_quota IS NOT NULL
                                        OR optimal_goal IS NOT NULL
                                        OR liquidity_goal IS NOT NULL
                                        OR coverage.adv_quota IS NOT NULL
                                        OR coverage.agency_quota IS NOT NULL
                                        OR coverage.sales_forecast IS NOT NULL
                                        OR coverage.sales_forecast_prior IS NOT NULL
                                        OR coverage.sbg_quota  IS NOT NULL
                                        OR cq_revenue IS NOT NULL
                                        OR pq_revenue IS NOT NULL
                                        OR ly_revenue IS NOT NULL
                                        OR lyq_revenue IS NOT NULL
                                        OR l2yq_revenue IS NOT NULL
                                        OR l2y_revenue IS NOT NULL
                                        OR reseller_quota IS NOT NULL
                                        OR inm_optimal_goal IS NOT NULL
                                        or agc_optimal_goal IS NOT NULL
                                        or l7d_revenue IS NOT NULL
                                        or l7d_revenue_prior IS NOT NULL
                                        or l14d_revenue IS NOT NULL
                                        or l14d_revenue_prior IS NOT NULL
                                       )

           """
        self.create = """
                CREATE TABLE IF NOT EXISTS <TABLE:bpo_coverage_asis_stg_3> (
                date_id varchar,
                id_d_ad_account bigint,
                ad_account_id bigint,
                ad_account_name varchar,
                advertiser_name varchar,
                id_dh_territory bigint,
                advertiser_country varchar,
                advertiser_vertical varchar,
                advertiser_sub_vertical varchar,
                gvm_vertical_name varchar,
                gvm_sub_vertical_name varchar,
                specialty VARCHAR,
                legacy_advertiser_sub_vertical VARCHAR,
                legacy_advertiser_vertical VARCHAR,
                region varchar,
                sub_region varchar,
                fraud_ind varchar,
                is_deleted varchar,
                is_house_account bigint,
                is_fcast_eligible boolean,
                advertiser_fbid varchar,
                advertiser_sfid varchar,
                ultimate_parent_fbid varchar,
                ultimate_parent_sfid varchar,
                ultimate_parent_name varchar,
                planning_agency_name varchar,
                planning_agency_fbid varchar,
                planning_agency_sfid varchar,
                planning_agency_ult_fbid varchar,
                planning_agency_ult_sfid varchar,
                planning_agency_ult_name varchar,
                split_cp varchar,
                split double,
                id_d_employee bigint,
                rep_fbid_cp varchar,
                client_partner varchar,
                cp_username varchar,
                cp_start_date varchar,
                cp_manager varchar,
                cp_manager_username varchar,
                l12fbid_cp varchar,
                rep_fbid_am varchar,
                account_manager varchar,
                am_username varchar,
                am_start_date varchar,
                am_manager varchar,
                am_manager_username varchar,
                l12fbid_am varchar,
                split_am varchar,
                rep_fbid_pm varchar,
                partner_manager varchar,
                pm_username varchar,
                pm_start_date varchar,
                pm_manager varchar,
                pm_manager_username varchar,
                l12fbid_pm varchar,
                split_pm varchar,
                rep_fbid_sp varchar,
                agency_partner varchar,
                ap_username varchar,
                ap_start_date varchar,
                ap_manager varchar,
                ap_manager_username varchar,
                l12fbid_sp varchar,
                split_sp varchar,
                rep_fbid_ap varchar,
                l12fbid_ap varchar,
                split_ap varchar,
                is_gpa boolean,
                l12fbid_unmanaged varchar,
                is_staged boolean,
                reseller_fbid varchar,
                reseller_sfid varchar,
                reseller_name varchar,
                rep_fbid_me varchar,
                rep_fbid_rp varchar,
                l12fbid_me varchar,
                l12fbid_rp varchar,
                split_me varchar,
                split_rp varchar,
                dq_flags_me map(varchar, varchar),
                dq_flags_rp map(varchar, varchar),
                rep_fbid_fcast varchar,
                l12fbid_fcast varchar,
                program varchar,
                market varchar,
                lwi_only_flag boolean,
                newbie_flag boolean,
                low_spender_90d_flag boolean,
                ig_boost_only_flag boolean,
                sss_flag boolean,
                seasonal_flag boolean,
                rep_role varchar,
                is_closed boolean,
                is_high_risk boolean,
                is_pci boolean,
                is_covered boolean,
                is_banned boolean,
                is_ou_eligible boolean,
                id_d_customer_account_adv bigint,
                advertiser_coverage_model_daa varchar,
                advertiser_program_daa varchar,
                agency_coverage_model_daa varchar,
                is_gat boolean,
                is_gcm boolean,
                is_magic93 boolean,
                newbie_type varchar,
                l12_advertiser_territory varchar,
                l10_advertiser_territory varchar,
                l8_advertiser_territory varchar,
                l6_advertiser_territory varchar,
                l4_advertiser_territory varchar,
                l2_advertiser_territory varchar,
                l12_usern_advertiser_territory varchar,
                l10_usern_advertiser_territory varchar,
                l12_manager_advertiser_territory varchar,
                l10_manager_advertiser_territory varchar,
                l8_manager_advertiser_territory varchar,
                l12_agency_territory varchar,
                l10_agency_territory varchar,
                l8_agency_territory varchar,
                l6_agency_territory varchar,
                l4_agency_territory varchar,
                l2_agency_territory varchar,
                l12_usern_agency_territory varchar,
                l10_usern_agency_territory varchar,
                l12_manager_agency_territory varchar,
                l10_manager_agency_territory varchar,
                l8_manager_agency_territory varchar,
                ad_account_l4_fbid BIGINT,
                ad_account_l8_fbid BIGINT,
                ad_account_l10_fbid BIGINT,
                ad_account_l12_fbid BIGINT,
                cq_revenue DOUBLE,
                cq_optimal DOUBLE,
                cq_liquidity DOUBLE,
                pq_revenue DOUBLE,
                pq_optimal DOUBLE,
                pq_liquidity DOUBLE,
                ly_revenue DOUBLE,
                l2y_revenue DOUBLE,
                ly_optimal DOUBLE,
                ly_liquidity DOUBLE,
                lyq_revenue DOUBLE,
                l2yq_revenue DOUBLE,
                lyq_optimal DOUBLE,
                lyq_liquidity DOUBLE,
                revenue_quota DOUBLE,
                optimal_quota DOUBLE,
                liquidity_quota DOUBLE,
                advertiser_quota DOUBLE,
                agency_quota DOUBLE,
                sales_forecast DOUBLE,
                sales_forecast_prior DOUBLE,
                sbg_quota DOUBLE,
                l12_reporting_territory VARCHAR,
                l10_reporting_territory VARCHAR,
                l8_reporting_territory  VARCHAR,
                l6_reporting_territory  VARCHAR,
                l4_reporting_territory  VARCHAR,
                l2_reporting_territory  VARCHAR,
                segmentation VARCHAR,
                l12_reporting_terr_mgr VARCHAR,
                l10_reporting_terr_mgr VARCHAR,
                l8_reporting_terr_mgr VARCHAR,
                l6_reporting_terr_mgr VARCHAR,
                l6_manager_agency_territory VARCHAR,
                rep_fbid_csm VARCHAR COMMENT 'FBID for Client Solutions Manager https://fburl.com/wut/cad62z1j',
                client_solutions_manager VARCHAR COMMENT 'full name for Client Solutions Manager',
                csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
                csm_start_date varchar COMMENT 'Start Date of CSM for a given ad account',
                csm_manager varchar COMMENT 'Full name for a CSMs manager',
                csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
                rep_fbid_asm VARCHAR COMMENT 'FBID for Agency Solutions Manager',
                agency_solutions_manager VARCHAR COMMENT 'Full name for ASM' ,
                asm_username varchar COMMENT 'UnixName for ASM',
                asm_start_date varchar COMMENT 'start date of an asm for a given ad account ',
                asm_manager varchar COMMENT 'Full name for an  ASMs manager',
                asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
                reseller_partner VARCHAR COMMENT 'Full name for reseller partner',
                rp_username varchar COMMENT 'UnixName for reseller partner',
                rp_start_date varchar COMMENT 'start date for a reseller partner for a given ad account',
                rp_manager varchar COMMENT 'Full name for an RPs manager',
                rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
                sales_adv_country_group VARCHAR,
                sales_adv_subregion VARCHAR,
                sales_adv_region VARCHAR,
                l12_reseller_territory VARCHAR,
                l10_reseller_territory VARCHAR,
                l8_reseller_territory  VARCHAR,
                l6_reseller_territory  VARCHAR,
                l4_reseller_territory  VARCHAR,
                l2_reseller_territory  VARCHAR,
                l12_reseller_terr_mgr VARCHAR,
                l10_reseller_terr_mgr VARCHAR,
                l8_reseller_terr_mgr  VARCHAR,
                l6_reseller_terr_mgr  VARCHAR,
                reseller_quota DOUBLE,
                country_agc VARCHAR,
                market_agc VARCHAR,
                region_agc VARCHAR,
                sub_region_agc VARCHAR,
                business_type_adv VARCHAR,
                business_type_agc VARCHAR,
                planning_agency_operating_co VARCHAR,
                program_agency VARCHAR,
                china_export_advertiser VARCHAR,
                export_advertiser_country VARCHAR,
                billing_country_adv VARCHAR,
                billing_region_adv VARCHAR,
                billing_country_agc VARCHAR,
                billing_region_agc VARCHAR,
                hq_country_adv VARCHAR,
                hq_region_adv VARCHAR,
                hq_country_agc VARCHAR,
                hq_region_agc VARCHAR,
                inm_optimal_goal DOUBLE,
                agc_optimal_goal DOUBLE,
                ts VARCHAR,
                L7d_revenue DOUBLE,
                L7d_revenue_prior DOUBLE,
                L7d_avg_revenue DOUBLE,
                L7d_avg_revenue_prior DOUBLE,
                legacy_gvm_vertical_name_v2 VARCHAR,
                legacy_gvm_sub_vertical_name_v2 VARCHAR,
                ultimate_parent_vertical_name_v2 VARCHAR,
                revenue_segment VARCHAR,
                subsegment VARCHAR,
                inm_dr_resilience_goal DOUBLE,
                scaled_dr_resilience_goal DOUBLE,
                dr_resilience_goal DOUBLE,
                cq_product_resilient_rec_rev DOUBLE,
                cq_ebr_usd_rec_rev DOUBLE,
                cq_capi_ebr_revenue DOUBLE,
                pq_product_resilient_rec_rev DOUBLE,
                pq_ebr_usd_rec_rev DOUBLE,
                pq_capi_ebr_revenue DOUBLE,
                ly_product_resilient_rec_rev DOUBLE,
                ly_ebr_usd_rec_rev DOUBLE,
                ly_capi_ebr_revenue DOUBLE,
                lyq_product_resilient_rec_rev DOUBLE,
                lyq_ebr_usd_rec_rev DOUBLE,
                lyq_capi_ebr_revenue DOUBLE,
                am_lwi_ind VARCHAR,
                L14d_revenue DOUBLE,
                L14d_revenue_prior DOUBLE,
                L14d_avg_revenue DOUBLE,
                L14d_avg_revenue_prior DOUBLE,
                sales_forecast_prior_2w DOUBLE,
                ds VARCHAR
               )
                   WITH (   partitioned_by = ARRAY['ds'],
                            retention_days = <RETENTION:90>,
                            uii=false)
                            """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select


class BpoCoverageAsisStg4:
    """@docstring"""

    def __init__(self):
        self.name = "<TABLE:bpo_coverage_asis_stg_4>"
        self.create = """
            CREATE TABLE IF NOT EXISTS <TABLE:bpo_coverage_asis_stg_4> (
            date_id varchar,
            id_dh_territory bigint,
            advertiser_name varchar,
            advertiser_country varchar,
            advertiser_vertical varchar,
            advertiser_sub_vertical varchar,
            gvm_vertical_name varchar,
            gvm_sub_vertical_name varchar,
            specialty VARCHAR,
            legacy_advertiser_sub_vertical VARCHAR,
            legacy_advertiser_vertical VARCHAR,
            region varchar,
            sub_region varchar,
            advertiser_fbid varchar,
            advertiser_sfid varchar,
            ultimate_parent_fbid varchar,
            ultimate_parent_sfid varchar,
            ultimate_parent_name varchar,
            planning_agency_name varchar,
            planning_agency_fbid varchar,
            planning_agency_sfid varchar,
            planning_agency_ult_fbid varchar,
            planning_agency_ult_sfid varchar,
            planning_agency_ult_name varchar,
            split double,
            id_d_employee bigint,
            client_partner varchar,
            cp_username varchar,
            cp_start_date varchar,
            cp_manager varchar,
            cp_manager_username varchar,
            account_manager varchar,
            am_username varchar,
            am_start_date varchar,
            am_manager varchar,
            am_manager_username varchar,
            partner_manager varchar,
            pm_username varchar,
            pm_start_date varchar,
            pm_manager varchar,
            pm_manager_username varchar,
            agency_partner varchar,
            ap_username varchar,
            ap_start_date varchar,
            ap_manager varchar,
            ap_manager_username varchar,
            reseller_fbid varchar,
            reseller_sfid varchar,
            reseller_name varchar,
            program varchar,
            id_d_customer_account_adv bigint,
            advertiser_coverage_model_daa varchar,
            advertiser_program_daa varchar,
            agency_coverage_model_daa varchar,
            is_gat boolean,
            is_gcm boolean,
            is_magic93 boolean,
            l12_advertiser_territory varchar,
            l10_advertiser_territory varchar,
            l8_advertiser_territory varchar,
            l6_advertiser_territory varchar,
            l4_advertiser_territory varchar,
            l2_advertiser_territory varchar,
            l12_usern_advertiser_territory varchar,
            l10_usern_advertiser_territory varchar,
            l12_manager_advertiser_territory varchar,
            l10_manager_advertiser_territory varchar,
            l8_manager_advertiser_territory varchar,
            l12_agency_territory varchar,
            l10_agency_territory varchar,
            l8_agency_territory varchar,
            l6_agency_territory varchar,
            l4_agency_territory varchar,
            l2_agency_territory varchar,
            l12_usern_agency_territory varchar,
            l10_usern_agency_territory varchar,
            l12_manager_agency_territory varchar,
            l10_manager_agency_territory varchar,
            l8_manager_agency_territory varchar,
            ad_account_l4_fbid BIGINT,
            ad_account_l8_fbid BIGINT,
            ad_account_l10_fbid BIGINT,
            ad_account_l12_fbid BIGINT,
            cq_revenue DOUBLE,
            cq_optimal DOUBLE,
            cq_liquidity DOUBLE,
            pq_revenue DOUBLE,
            pq_optimal DOUBLE,
            pq_liquidity DOUBLE,
            ly_revenue DOUBLE,
            l2y_revenue DOUBLE,
            ly_optimal DOUBLE,
            ly_liquidity DOUBLE,
            lyq_revenue DOUBLE,
            l2yq_revenue DOUBLE,
            lyq_optimal DOUBLE,
            lyq_liquidity DOUBLE,
            revenue_quota DOUBLE,
            optimal_quota DOUBLE,
            liquidity_quota DOUBLE,
            advertiser_quota double,
            agency_quota double,
            sbg_quota DOUBLE,
            sales_forecast DOUBLE,
            sales_forecast_prior DOUBLE,
            rep_fbid_am VARCHAR,
            rep_fbid_pm VARCHAR,
            rep_fbid_cp VARCHAR,
            rep_fbid_rp VARCHAR,
            rep_fbid_sp VARCHAR,
            rep_fbid_ap VARCHAR,
            rep_fbid_me VARCHAR,
            l12_reporting_territory VARCHAR,
            l10_reporting_territory VARCHAR,
            l8_reporting_territory  VARCHAR,
            l6_reporting_territory  VARCHAR,
            l4_reporting_territory  VARCHAR,
            l2_reporting_territory  VARCHAR,
            segmentation VARCHAR,
            l12_reporting_terr_mgr VARCHAR,
            l10_reporting_terr_mgr VARCHAR,
            l8_reporting_terr_mgr VARCHAR,
            l6_reporting_terr_mgr VARCHAR,
            l6_manager_agency_territory VARCHAR,
            rep_fbid_csm VARCHAR COMMENT 'FBID for Client Solutions Manager https://fburl.com/wut/cad62z1j',
            client_solutions_manager VARCHAR COMMENT 'full name for Client Solutions Manager',
            csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
            csm_start_date varchar COMMENT 'Start Date of CSM for a given ad account',
            csm_manager varchar COMMENT 'Full name for a CSMs manager',
            csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
            rep_fbid_asm VARCHAR COMMENT 'FBID for Agency Solutions Manager',
            agency_solutions_manager VARCHAR COMMENT 'Full name for ASM' ,
            asm_username varchar COMMENT 'UnixName for ASM',
            asm_start_date varchar COMMENT 'start date of an asm for a given ad account ',
            asm_manager varchar COMMENT 'Full name for an  ASMs manager',
            asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
            reseller_partner VARCHAR COMMENT 'Full name for reseller partner',
            rp_username varchar COMMENT 'UnixName for reseller partner',
            rp_start_date varchar COMMENT 'start date for a reseller partner for a given ad account',
            rp_manager varchar COMMENT 'Full name for an RPs manager',
            rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
            sales_adv_country_group VARCHAR,
            sales_adv_subregion VARCHAR,
            sales_adv_region VARCHAR,
            market VARCHAR,
            l12_reseller_territory VARCHAR,
            l10_reseller_territory VARCHAR,
            l8_reseller_territory  VARCHAR,
            l6_reseller_territory  VARCHAR,
            l4_reseller_territory  VARCHAR,
            l2_reseller_territory  VARCHAR,
            l12_reseller_terr_mgr VARCHAR,
            l10_reseller_terr_mgr VARCHAR,
            l8_reseller_terr_mgr  VARCHAR,
            l6_reseller_terr_mgr  VARCHAR,
            reseller_quota DOUBLE,
            country_agc VARCHAR,
            market_agc VARCHAR,
            region_agc VARCHAR,
            sub_region_agc VARCHAR,
            business_type_adv VARCHAR,
            business_type_agc VARCHAR,
            planning_agency_operating_co VARCHAR,
            program_agency VARCHAR,
            china_export_advertiser VARCHAR,
            export_advertiser_country VARCHAR,
            billing_country_adv VARCHAR,
            billing_region_adv VARCHAR,
            billing_country_agc VARCHAR,
            billing_region_agc VARCHAR,
            hq_country_adv VARCHAR,
            hq_region_adv VARCHAR,
            hq_country_agc VARCHAR,
            hq_region_agc VARCHAR,
            inm_optimal_goal DOUBLE,
            agc_optimal_goal DOUBLE,
            ts VARCHAR,
            L7d_revenue DOUBLE,
            L7d_revenue_prior DOUBLE,
            L7d_avg_revenue DOUBLE,
            L7d_avg_revenue_prior DOUBLE,
            legacy_gvm_vertical_name_v2 VARCHAR,
            legacy_gvm_sub_vertical_name_v2 VARCHAR,
            ultimate_parent_vertical_name_v2 VARCHAR,
            revenue_segment VARCHAR,
            subsegment VARCHAR,
            am_goal DOUBLE,
            lwi_goal DOUBLE,
            dr_resilience_goal DOUBLE,
            cq_product_resilient_rec_rev DOUBLE,
            cq_ebr_usd_rec_rev DOUBLE,
            cq_capi_ebr_revenue DOUBLE,
            pq_product_resilient_rec_rev DOUBLE,
            pq_ebr_usd_rec_rev DOUBLE,
            pq_capi_ebr_revenue DOUBLE,
            ly_product_resilient_rec_rev DOUBLE,
            ly_ebr_usd_rec_rev DOUBLE,
            ly_capi_ebr_revenue DOUBLE,
            lyq_product_resilient_rec_rev DOUBLE,
            lyq_ebr_usd_rec_rev DOUBLE,
            lyq_capi_ebr_revenue DOUBLE,
            am_lwi_ind VARCHAR,
            L14d_revenue DOUBLE,
            L14d_revenue_prior DOUBLE,
            L14d_avg_revenue DOUBLE,
            L14d_avg_revenue_prior DOUBLE,
            sales_forecast_prior_2w DOUBLE,
            ds varchar
              )
               WITH (
                     partitioned_by = ARRAY['ds'],
                     retention_days = <RETENTION:90>,
                     uii=false
                    )"""
        self.select = """

                        WITH     sbg_optimal as (
                        SELECT
                                l4,
                                revenue_segment,
                                me_eligible_segment,
                                ads_manager_goal am_goal,
                                lwi_goal
                            FROM sbg_l4_optimal_goals
                            WHERE
                                ds = '<quarter_id>'
                   )

                SELECT
                coverage.date_id
                ,coverage.id_dh_territory
                ,coverage.advertiser_name
                ,coverage.advertiser_country
                ,coverage.advertiser_vertical
                ,coverage.advertiser_sub_vertical
                ,coverage.gvm_vertical_name
                ,coverage.gvm_sub_vertical_name
                ,coverage.specialty
                ,coverage.legacy_advertiser_sub_vertical
                ,coverage.legacy_advertiser_vertical
                ,coverage.region
                ,coverage.sub_region
                ,coverage.advertiser_fbid
                ,coverage.advertiser_sfid
                ,coverage.ultimate_parent_fbid
                ,coverage.ultimate_parent_sfid
                ,coverage.ultimate_parent_name
                ,coverage.planning_agency_name
                ,coverage.planning_agency_fbid
                ,coverage.planning_agency_sfid
                ,coverage.planning_agency_ult_fbid
                ,coverage.planning_agency_ult_sfid
                ,coverage.planning_agency_ult_name
                ,coverage.split
                ,coverage.id_d_employee
                ,coverage.rep_fbid_cp
                ,coverage.client_partner
                ,coverage.cp_username
                ,coverage.cp_start_date
                ,coverage.cp_manager
                ,coverage.cp_manager_username
                ,coverage.rep_fbid_am
                ,coverage.account_manager
                ,coverage.am_username
                ,coverage.am_start_date
                ,coverage.am_manager
                ,coverage.am_manager_username
                ,coverage.rep_fbid_pm
                ,coverage.partner_manager
                ,coverage.pm_username
                ,coverage.pm_start_date
                ,coverage.pm_manager
                ,coverage.pm_manager_username
                ,coverage.rep_fbid_sp
                ,coverage.agency_partner
                ,coverage.ap_username
                ,coverage.ap_start_date
                ,coverage.ap_manager
                ,coverage.ap_manager_username
                ,coverage.rep_fbid_ap
                ,coverage.l12fbid_ap
                ,coverage.split_ap
                ,coverage.reseller_fbid
                ,coverage.reseller_sfid
                ,coverage.reseller_name
                ,coverage.program
                ,coverage.id_d_customer_account_adv
                ,coverage.advertiser_coverage_model_daa
                ,coverage.advertiser_program_daa
                ,coverage.agency_coverage_model_daa
                ,coverage.is_gat
                ,coverage.is_gcm
                ,coverage.is_magic93
                ,coverage.l12_advertiser_territory
                ,coverage.l10_advertiser_territory
                ,coverage.l8_advertiser_territory
                ,coverage.l6_advertiser_territory
                ,coverage.l4_advertiser_territory
                ,coverage.l2_advertiser_territory
                ,coverage.l12_usern_advertiser_territory
                ,coverage.l10_usern_advertiser_territory
                ,coverage.l12_manager_advertiser_territory
                ,coverage.l10_manager_advertiser_territory
                ,coverage.l8_manager_advertiser_territory
                ,coverage.l12_agency_territory
                ,coverage.l10_agency_territory
                ,coverage.l8_agency_territory
                ,coverage.l6_agency_territory
                ,coverage.l4_agency_territory
                ,coverage.l2_agency_territory
                ,coverage.l12_usern_agency_territory
                ,coverage.l10_usern_agency_territory
                ,coverage.l12_manager_agency_territory
                ,coverage.l10_manager_agency_territory
                ,coverage.l8_manager_agency_territory
                ,coverage.ad_account_l4_fbid
                ,coverage.ad_account_l8_fbid
                ,coverage.ad_account_l10_fbid
                ,coverage.ad_account_l12_fbid
                ,rep_fbid_rp
                ,rep_fbid_me
                ,coverage.l12_reporting_territory
                ,coverage.l10_reporting_territory
                ,coverage.l8_reporting_territory
                ,coverage.l6_reporting_territory
                ,coverage.l4_reporting_territory
                ,coverage.l2_reporting_territory
                ,coverage.l6_manager_agency_territory
                ,segmentation
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,l12_reseller_territory
                ,l10_reseller_territory
                ,l8_reseller_territory
                ,l6_reseller_territory
                ,l4_reseller_territory
                ,l2_reseller_territory
                ,l12_reseller_terr_mgr
                ,l10_reseller_terr_mgr
                ,l8_reseller_terr_mgr
                ,l6_reseller_terr_mgr
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
                ,SUM(inm_optimal_goal) inm_optimal_goal
                ,SUM(agc_optimal_goal) agc_optimal_goal
                ,SUM(reseller_quota) reseller_quota
                ,SUM(cq_revenue) cq_revenue
                ,SUM(cq_optimal) cq_optimal
                ,SUM(cq_liquidity) cq_liquidity
                ,SUM(pq_revenue) pq_revenue
                ,SUM(pq_optimal) pq_optimal
                ,SUM(pq_liquidity) pq_liquidity
                ,SUM(ly_revenue) ly_revenue
                ,SUM(l2y_revenue) l2y_revenue
                ,SUM(ly_optimal) ly_optimal
                ,SUM(ly_liquidity) ly_liquidity
                ,SUM(lyq_revenue) lyq_revenue
                ,SUM(l2yq_revenue) l2yq_revenue
                ,SUM(lyq_optimal) lyq_optimal
                ,SUM(lyq_liquidity) lyq_liquidity
                ,SUM(revenue_quota) revenue_quota
                ,SUM(optimal_quota) optimal_quota
                ,SUM(liquidity_quota) liquidity_quota
                ,SUM(advertiser_quota) advertiser_quota
                ,SUM(agency_quota) agency_quota
                ,SUM(sales_forecast) sales_forecast
                ,SUM(sales_forecast_prior) sales_forecast_prior
                ,SUM(sbg_quota) sbg_quota
                ,SUM(L7d_revenue) L7d_revenue
                ,SUM(L7d_revenue_prior) L7d_revenue_prior
                ,SUM(L7d_avg_revenue) L7d_avg_revenue
                ,SUM(L7d_avg_revenue_prior) L7d_avg_revenue_prior
                ,legacy_gvm_vertical_name_v2
                ,legacy_gvm_sub_vertical_name_v2
                ,ultimate_parent_vertical_name_v2
                ,coverage.revenue_segment
                ,coverage.subsegment
                ,am_lwi_ind
                ,MAX(COALESCE(subseg.am_goal,seg.am_goal)) am_goal
                ,MAX(COALESCE(subseg.lwi_goal,seg.lwi_goal)) lwi_goal
                ,SUM(dr_resilience_goal) dr_resilience_goal
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
                ,SUM(L14d_revenue) L14d_revenue
                ,SUM(L14d_revenue_prior) L14d_revenue_prior
                ,SUM(L14d_avg_revenue) L14d_avg_revenue
                ,SUM(L14d_avg_revenue_prior) L14d_avg_revenue_prior
                ,SUM(sales_forecast_prior_2w) sales_forecast_prior_2w




                from <TABLE:bpo_coverage_asis_stg_3> coverage

                LEFT JOIN sbg_optimal subseg
                                    ON subseg.l4 = coverage.l4_reporting_territory
                                    AND subseg.me_eligible_segment = coverage.subsegment
                LEFT JOIN sbg_optimal seg
                                    ON seg.l4 = coverage.l4_reporting_territory
                                    AND seg.revenue_segment = coverage.revenue_segment

                where ds = '<DATEID>'

                AND
                ( cq_revenue IS NOT NULL
                OR pq_revenue  IS NOT NULL
                OR lyq_revenue IS NOT NULL
                OR l2yq_revenue is not null
                OR advertiser_quota IS NOT NULL
                OR agency_quota IS NOT NULL
                OR sbg_quota IS NOT NULL
                OR sales_forecast  IS NOT NULL
                OR reseller_quota IS NOT NULL
                or inm_optimal_goal is not null
                or agc_optimal_goal is not null
                or L7d_revenue is not NULL
                or L7d_revenue_prior is not null
                or L14d_revenue is not NULL
                or L14d_revenue_prior is not null
               )

                group by
                coverage.date_id
                ,coverage.id_dh_territory
                ,coverage.advertiser_name
                ,coverage.advertiser_country
                ,coverage.advertiser_vertical
                ,coverage.advertiser_sub_vertical
                ,coverage.gvm_vertical_name
                ,coverage.gvm_sub_vertical_name
                ,coverage.specialty
                ,coverage.legacy_advertiser_sub_vertical
                ,coverage.legacy_advertiser_vertical
                ,coverage.region
                ,coverage.sub_region
                ,coverage.advertiser_fbid
                ,coverage.advertiser_sfid
                ,coverage.ultimate_parent_fbid
                ,coverage.ultimate_parent_sfid
                ,coverage.ultimate_parent_name
                ,coverage.planning_agency_name
                ,coverage.planning_agency_fbid
                ,coverage.planning_agency_sfid
                ,coverage.planning_agency_ult_fbid
                ,coverage.planning_agency_ult_sfid
                ,coverage.planning_agency_ult_name
                ,coverage.split
                ,coverage.id_d_employee
                ,coverage.rep_fbid_cp
                ,coverage.client_partner
                ,coverage.cp_username
                ,coverage.cp_start_date
                ,coverage.cp_manager
                ,coverage.cp_manager_username
                ,coverage.rep_fbid_am
                ,coverage.account_manager
                ,coverage.am_username
                ,coverage.am_start_date
                ,coverage.am_manager
                ,coverage.am_manager_username
                ,coverage.rep_fbid_pm
                ,coverage.partner_manager
                ,coverage.pm_username
                ,coverage.pm_start_date
                ,coverage.pm_manager
                ,coverage.pm_manager_username
                ,coverage.rep_fbid_sp
                ,coverage.agency_partner
                ,coverage.ap_username
                ,coverage.ap_start_date
                ,coverage.ap_manager
                ,coverage.ap_manager_username
                ,coverage.rep_fbid_ap
                ,coverage.l12fbid_ap
                ,coverage.split_ap
                ,coverage.reseller_fbid
                ,coverage.reseller_sfid
                ,coverage.reseller_name
                ,coverage.program
                ,coverage.id_d_customer_account_adv
                ,coverage.advertiser_coverage_model_daa
                ,coverage.advertiser_program_daa
                ,coverage.agency_coverage_model_daa
                ,coverage.is_gat
                ,coverage.is_gcm
                ,coverage.is_magic93
                ,coverage.l12_advertiser_territory
                ,coverage.l10_advertiser_territory
                ,coverage.l8_advertiser_territory
                ,coverage.l6_advertiser_territory
                ,coverage.l4_advertiser_territory
                ,coverage.l2_advertiser_territory
                ,coverage.l12_usern_advertiser_territory
                ,coverage.l10_usern_advertiser_territory
                ,coverage.l12_manager_advertiser_territory
                ,coverage.l10_manager_advertiser_territory
                ,coverage.l8_manager_advertiser_territory
                ,coverage.l12_agency_territory
                ,coverage.l10_agency_territory
                ,coverage.l8_agency_territory
                ,coverage.l6_agency_territory
                ,coverage.l4_agency_territory
                ,coverage.l2_agency_territory
                ,coverage.l12_usern_agency_territory
                ,coverage.l10_usern_agency_territory
                ,coverage.l12_manager_agency_territory
                ,coverage.l10_manager_agency_territory
                ,coverage.l8_manager_agency_territory
                ,coverage.ad_account_l4_fbid
                ,coverage.ad_account_l8_fbid
                ,coverage.ad_account_l10_fbid
                ,coverage.ad_account_l12_fbid
                ,rep_fbid_rp
                ,rep_fbid_me
                ,coverage.l12_reporting_territory
                ,coverage.l10_reporting_territory
                ,coverage.l8_reporting_territory
                ,coverage.l6_reporting_territory
                ,coverage.l4_reporting_territory
                ,coverage.l2_reporting_territory
                ,coverage.l6_manager_agency_territory
                ,segmentation
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,l12_reseller_territory
                ,l10_reseller_territory
                ,l8_reseller_territory
                ,l6_reseller_territory
                ,l4_reseller_territory
                ,l2_reseller_territory
                ,l12_reseller_terr_mgr
                ,l10_reseller_terr_mgr
                ,l8_reseller_terr_mgr
                ,l6_reseller_terr_mgr
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,CAST(CURRENT_TIMESTAMP AS VARCHAR)
                ,legacy_gvm_vertical_name_v2
                ,legacy_gvm_sub_vertical_name_v2
                ,ultimate_parent_vertical_name_v2
                ,coverage.revenue_segment
                ,coverage.subsegment
                ,am_lwi_ind
            """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select


class GmsDailyCoreSignal:
    def __init__(self):
        self.name = "<TABLE:gms_daily_core_signal>"
        self.create = """
             CREATE TABLE IF NOT EXISTS <TABLE:gms_daily_core_signal>
                 (runtime VARCHAR,
                  dummy VARCHAR,
                  dummy1 VARCHAR,
                  dummy2 VARCHAR,
                  ds VARCHAR
                 )
              WITH (
                   partitioned_by=ARRAY['ds'],
                   retention_days=90,
                   uii=False
             )
              """
        self.select = """
        WITH FCT as (
                    SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR) runtime

                    FROM <TABLE:bpo_coverage_asis_stg_4>

                    WHERE ds='<DATEID>')
        SELECT FCT.runtime,
               NULL AS dummy,
               NULL AS dummy1,
               NULL AS dummy2,
               '<DATEID>' as ds


        FROM FCT

        """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select


class GmsDailyStg1Signal:
    def __init__(self):
        self.name = "<TABLE:gms_daily_stg_1_signal>"
        self.create = """
             CREATE TABLE IF NOT EXISTS <TABLE:gms_daily_stg_1_signal>
                 (runtime VARCHAR,
                  dummy VARCHAR,
                  dummy1 VARCHAR,
                  dummy2 VARCHAR,
                  ds VARCHAR
                 )
              WITH (
                   partitioned_by = ARRAY['ds'],
                   retention_days = 90,
                   uii=False
             )
              """
        self.select = """
        WITH FCT as (
                    SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR) runtime

                    FROM <TABLE:bpo_coverage_asis_stg_1>

                    WHERE ds='<DATEID>')
        SELECT FCT.runtime,
               NULL AS dummy,
               NULL AS dummy1,
               NULL AS dummy2


        FROM FCT

        """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select


class BpoGmsSolutionsAdAccountLevelSnapshot:

    """@docstring ad account level product table with daily snapshots"""

    def __init__(self):
        self.name = "<TABLE:bpo_gms_solutions_ad_account_level_snapshot>"
        self.create = """
            CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_solutions_ad_account_level_snapshot> (
            date_id varchar,
            id_d_ad_account bigint,
            ad_account_id bigint,
            ad_account_name varchar,
            id_dh_territory bigint,
            advertiser_name varchar,
            advertiser_country varchar,
            advertiser_vertical varchar,
            advertiser_sub_vertical varchar,
            gvm_vertical_name varchar,
            gvm_sub_vertical_name varchar,
            specialty VARCHAR,
            legacy_advertiser_sub_vertical VARCHAR,
            legacy_advertiser_vertical VARCHAR,
            region varchar,
            sub_region varchar,
            advertiser_fbid varchar,
            advertiser_sfid varchar,
            ultimate_parent_fbid varchar,
            ultimate_parent_sfid varchar,
            ultimate_parent_name varchar,
            planning_agency_name varchar,
            planning_agency_fbid varchar,
            planning_agency_sfid varchar,
            planning_agency_ult_fbid varchar,
            planning_agency_ult_sfid varchar,
            planning_agency_ult_name varchar,
            split double,
            client_partner varchar,
            cp_username varchar,
            cp_start_date varchar,
            cp_manager varchar,
            cp_manager_username varchar,
            account_manager varchar,
            am_username varchar,
            am_start_date varchar,
            am_manager varchar,
            am_manager_username varchar,
            partner_manager varchar,
            pm_username varchar,
            pm_start_date varchar,
            pm_manager varchar,
            pm_manager_username varchar,
            agency_partner varchar,
            ap_username varchar,
            ap_start_date varchar,
            ap_manager varchar,
            ap_manager_username varchar,
            reseller_fbid varchar,
            reseller_sfid varchar,
            reseller_name varchar,
            program varchar,
            id_d_customer_account_adv bigint,
            advertiser_coverage_model_daa varchar,
            advertiser_program_daa varchar,
            agency_coverage_model_daa varchar,
            is_gat boolean,
            is_gcm boolean,
            is_magic93 boolean,
            l12_advertiser_territory varchar,
            l10_advertiser_territory varchar,
            l8_advertiser_territory varchar,
            l6_advertiser_territory varchar,
            l4_advertiser_territory varchar,
            l2_advertiser_territory varchar,
            l12_usern_advertiser_territory varchar,
            l10_usern_advertiser_territory varchar,
            l12_manager_advertiser_territory varchar,
            l10_manager_advertiser_territory varchar,
            l8_manager_advertiser_territory varchar,
            l12_agency_territory varchar,
            l10_agency_territory varchar,
            l8_agency_territory varchar,
            l6_agency_territory varchar,
            l4_agency_territory varchar,
            l2_agency_territory varchar,
            l12_usern_agency_territory varchar,
            l10_usern_agency_territory varchar,
            l12_manager_agency_territory varchar,
            l10_manager_agency_territory varchar,
            l8_manager_agency_territory varchar,
            ad_account_l4_fbid BIGINT,
            ad_account_l8_fbid BIGINT,
            ad_account_l10_fbid BIGINT,
            ad_account_l12_fbid BIGINT,
            gms_optimal_target double,
            gms_liquidity_target double,
            gso_optimal_target double,
            gso_liquidity_target double,
            smb_optimal_target double,
            smb_liquidity_target double,
            l4_optimal_target double,
            l4_liquidity_target double,
            l8_optimal_target double,
            l8_liquidity_target double,
            l10_optimal_target double,
            l10_liquidity_target double,
            l12_optimal_target double,
            l12_liquidity_target double,
            canvas_ads_revn double,
            fb_feed_opt_in_revn double,
            app_install_revn double,
            dynamic_ads_revn double,
            carousel_revn double,
            cpas_revn double,
            playable_ads_revn double,
            app_event_optimisation_revn double,
            reach_and_frequency_revn double,
            sdk_revn double,
            page_likes_revn double,
            vertical_video_revn double,
            slideshow_video_revn double,
            click_to_messenger_revn double,
            short_form_video_revn double,
            video_revn double,
            dro_conversion_optimization_revn double,
            dro_landing_page_views_revn double,
            dro_app_installs_revn double,
            dro_lead_generation_revn double,
            dro_dra_offline_conversions_revn double,
            dro_messenger_replies_revn double,
            dro_store_visits_revn double,
            dro_dra_roas_revn double,
            reach_optimized_revn double,
            video_views_optimized_revn double,
            bao_mfv_revn double,
            bao_nonvideo_revn double,
            videoviews_mfc_revn double,
            pa_messenger_opt_in_revn double,
            pa_instream_revn double,
            pa_audience_network_revn double,
            pa_instant_articles_revn double,
            pa_instagram_story_revn double,
            po_messenger_opt_in_revn double,
            po_instream_revn double,
            po_audience_network_revn double,
            po_instagram_revn double,
            po_instant_articles_revn double,
            po_instagram_story_revn double,
            instagram_opt_in_revn double,
            instagram_stories_opt_in_revn double,
            audience_network_opt_in_revn double,
            messenger_opt_in_revn double,
            home_instream_opt_in_revn double,
            mobile_instream_opt_in_revn double,
            fb_pixel_revn double,
            lead_ads_revn double,
            mobile_first_video_revn double,
            collection_revn double,
            instream_video_revn double,
            messenger_revn double,
            instagram_revn double,
            fb_stories_opt_in_revn double,
            click_to_whatsapp_revn double,
            revenue double,
            optimal double,
            liquidity double,
            ig_stories_revn	double,
            messenger_ads_revn	double,
            audience_network_revn	double,
            web_conversion_revn	double,
            website_clicks_revn	double,
            platform_messenger_revn	double,
            facebook_revn	double,
            sponsored_messages_revn	double,
            mobile_feed_impressions	double,
            ig_stories_impressions	double,
            ig_stream_impressions	double,
            messenger_impressions	double,
            mobile_feed_revn	double,
            web_feed_impressions double,
            dr_revn	double,
            brand_revn double,
            l12_reporting_territory VARCHAR,
            l10_reporting_territory VARCHAR,
            l8_reporting_territory  VARCHAR,
            l6_reporting_territory  VARCHAR,
            l4_reporting_territory  VARCHAR,
            l2_reporting_territory  VARCHAR,
            segmentation VARCHAR,
            l6_manager_agency_territory VARCHAR,
            search_ads_revn DOUBLE COMMENT 'Recognized Search Ads Revenue',
            ig_feed_revn DOUBLE COMMENT 'Recognized Instagram Feed Revenue',
            marketplace_revn DOUBLE COMMENT 'Recognized MarketPlace Revenue',
            branded_content_revn DOUBLE COMMENT 'Recognized Branded Content Revenue',
            messenger_stories_revn DOUBLE COMMENT 'Recognized Messenger Stories Revenue',
            groups_revn DOUBLE COMMENT ' Recognized groups revenue ',
            fb_stories_revn DOUBLE COMMENT 'Delivered Revenue for FB stories Placement',
            l12_reporting_terr_mgr VARCHAR,
            l10_reporting_terr_mgr VARCHAR,
            l8_reporting_terr_mgr VARCHAR,
            l6_reporting_terr_mgr VARCHAR,
            pixel_issued double,
            pixel_total double,
            catalog_match double,
            catolog_total double,
            cbb_revn double,
            four_plus_placements_revn double,
            automatic_placement_revn double,
            rep_fbid_csm VARCHAR COMMENT 'FBID for Client Solutions Manager https://fburl.com/wut/cad62z1j',
            client_solutions_manager VARCHAR COMMENT 'full name for Client Solutions Manager',
            csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
            csm_start_date varchar COMMENT 'Start Date of CSM for a given ad account',
            csm_manager varchar COMMENT 'Full name for a CSMs manager',
            csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
            rep_fbid_asm VARCHAR COMMENT 'FBID for Agency Solutions Manager',
            agency_solutions_manager VARCHAR COMMENT 'Full name for ASM' ,
            asm_username varchar COMMENT 'UnixName for ASM',
            asm_start_date varchar COMMENT 'start date of an asm for a given ad account ',
            asm_manager varchar COMMENT 'Full name for an  ASMs manager',
            asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
            reseller_partner VARCHAR COMMENT 'Full name for reseller partner',
            rp_username varchar COMMENT 'UnixName for reseller partner',
            rp_start_date varchar COMMENT 'start date for a reseller partner for a given ad account',
            rp_manager varchar COMMENT 'Full name for an RPs manager',
            rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
            sales_adv_country_group VARCHAR,
            sales_adv_subregion VARCHAR,
            sales_adv_region VARCHAR,
            market VARCHAR,
            program_optimal_target double,
            program_liquidity_target double,
            l6_optimal_target double,
            l6_liquidity_target double,
            country_agc VARCHAR,
            market_agc VARCHAR,
            region_agc VARCHAR,
            sub_region_agc VARCHAR,
            business_type_adv VARCHAR,
            business_type_agc VARCHAR,
            planning_agency_operating_co VARCHAR,
            program_agency VARCHAR,
            china_export_advertiser VARCHAR,
            export_advertiser_country VARCHAR,
            billing_country_adv VARCHAR,
            billing_region_adv VARCHAR,
            billing_country_agc VARCHAR,
            billing_region_agc VARCHAR,
            hq_country_adv VARCHAR,
            hq_region_adv VARCHAR,
            hq_country_agc VARCHAR,
            hq_region_agc VARCHAR,
            resilient_revn DOUBLE,
            revenue_segment VARCHAR,
            ts VARCHAR,
            ds varchar
       )
        WITH (
                partitioned_by = ARRAY['ds'],
                        retention_days = <RETENTION:90>,
                        uii=false
                       )

         """
        self.select = """
           WITH
            products as (
            Select
                ad_account_id,
                date_id,
                SUM(canvas_ads_revn) canvas_ads_revn ,
                SUM(fb_feed_opt_in_revn) fb_feed_opt_in_revn ,
                SUM(app_install_revn) app_install_revn ,
                SUM(dynamic_ads_revn) dynamic_ads_revn ,
                SUM(carousel_revn) carousel_revn ,
                SUM(cpas_revn) cpas_revn ,
                SUM(playable_ads_revn) playable_ads_revn ,
                SUM(app_event_optimisation_revn) app_event_optimisation_revn ,
                SUM(reach_and_frequency_revn) reach_and_frequency_revn ,
                SUM(sdk_revn) sdk_revn ,
                SUM(page_likes_revn) page_likes_revn ,
                SUM(vertical_video_revn) vertical_video_revn ,
                SUM(slideshow_video_revn) slideshow_video_revn ,
                SUM(click_to_messenger_revn) click_to_messenger_revn ,
                SUM(short_form_video_revn) short_form_video_revn ,
                SUM(video_revn) video_revn ,
                SUM(0) dro_conversion_optimization_revn ,
                SUM(0) dro_landing_page_views_revn ,
                SUM(0) dro_app_installs_revn ,
                SUM(0) dro_lead_generation_revn ,
                SUM(0) dro_dra_offline_conversions_revn ,
                SUM(0) dro_messenger_replies_revn ,
                SUM(0) dro_store_visits_revn ,
                SUM(0) dro_dra_roas_revn ,
                SUM(reach_optimized_revn) reach_optimized_revn ,
                SUM(video_views_optimized_revn) video_views_optimized_revn ,
                SUM(0) bao_mfv_revn ,
                SUM(0) bao_nonvideo_revn ,
                SUM(videoviews_mfc_revn) videoviews_mfc_revn ,
                SUM(0) pa_messenger_opt_in_revn ,
                SUM(0) pa_instream_revn ,
                SUM(0) pa_audience_network_revn ,
                SUM(0) pa_instant_articles_revn ,
                SUM(0) pa_instagram_story_revn ,
                SUM(0) po_messenger_opt_in_revn ,
                SUM(0) po_instream_revn ,
                SUM(0) po_audience_network_revn ,
                SUM(0) po_instagram_revn ,
                SUM(0) po_instant_articles_revn ,
                SUM(0) po_instagram_story_revn ,
                SUM(instagram_opt_in_revn) instagram_opt_in_revn ,
                SUM(instagram_stories_opt_in_revn) instagram_stories_opt_in_revn ,
                SUM(audience_network_opt_in_revn) audience_network_opt_in_revn ,
                SUM(messenger_opt_in_revn) messenger_opt_in_revn ,
                SUM(home_instream_opt_in_revn) home_instream_opt_in_revn ,
                SUM(mobile_instream_opt_in_revn) mobile_instream_opt_in_revn ,
                SUM(fb_pixel_revn) fb_pixel_revn ,
                SUM(lead_ads_revn) lead_ads_revn ,
                SUM(mobile_first_video_revn) mobile_first_video_revn ,
                SUM(collection_revn) collection_revn ,
                SUM(instream_video_revn) instream_video_revn ,
                SUM(messenger_revn) messenger_revn ,
                SUM(instagram_revn) instagram_revn ,
                SUM(fb_stories_opt_in_revn) fb_stories_opt_in_revn ,
                SUM(click_to_whatsapp_revn) click_to_whatsapp_revn,
                SUM(ig_stories_revn) ig_stories_revn,
                SUM(messenger_ads_revn) messenger_ads_revn,
                SUM(audience_network_revn) audience_network_revn,
                SUM(web_conversion_revn) web_conversion_revn,
                SUM(website_clicks_revn) website_clicks_revn,
                SUM(platform_messenger_revn) platform_messenger_revn,
                SUM(facebook_revn) facebook_revn,
                SUM(sponsored_messages_revn) sponsored_messages_revn,
                SUM(mobile_feed_impressions) mobile_feed_impressions,
                SUM(ig_stories_impressions) ig_stories_impressions,
                SUM(ig_stream_impressions) ig_stream_impressions,
                SUM(messenger_impressions) messenger_impressions,
                SUM(mobile_feed_revn) mobile_feed_revn,
                SUM(web_feed_impressions) web_feed_impressions,
                SUM(dr_revn	) dr_revn,
                SUM(brand_revn) brand_revn,
                SUM(search_ads_revn) search_ads_revn,
                SUM(ig_feed_revn) ig_feed_revn,
                SUM(marketplace_revn) marketplace_revn,
                SUM(branded_content_revn) branded_content_revn,
                SUM(messenger_stories_revn) messenger_stories_revn,
                SUM(groups_revn) groups_revn,
                SUM(fb_stories_revn) fb_stories_revn,
                SUM(gms_solution_1_ind_revn) optimal_revn,
                SUM(gms_solution_7_ind_revn) liquidity_revn,
                SUM(total_revn) revenue,
                SUM(cbb_revn) cbb_revn,
                SUM(is_using_4_plus_placements_revn) four_plus_placements_revn,
                SUM(automatic_placements_revn) automatic_placement_revn,
                SUM(resilient_revn) resilient_revn
                from

                f_product_master_asis

                where ds >= cast((cast('<quarter_id>' as date) - interval '12' month) as varchar)
                and ds <= '<DATEID>'
                GROUP BY ad_account_id
                        ,date_id
           ),
            coverage as(
            Select
                coverage.id_d_ad_account
                ,coverage.ad_account_id
                ,coverage.ad_account_name
                ,coverage.id_dh_territory
                ,coverage.advertiser_name
                ,coverage.advertiser_country
                ,coverage.advertiser_vertical
                ,coverage.advertiser_sub_vertical
                ,coverage.gvm_vertical_name
                ,coverage.gvm_sub_vertical_name
                ,coverage.specialty
                ,coverage.legacy_advertiser_sub_vertical
                ,coverage.legacy_advertiser_vertical
                ,coverage.region
                ,coverage.sub_region
                ,coverage.advertiser_fbid
                ,coverage.advertiser_sfid
                ,coverage.ultimate_parent_fbid
                ,coverage.ultimate_parent_sfid
                ,coverage.ultimate_parent_name
                ,coverage.planning_agency_name
                ,coverage.planning_agency_fbid
                ,coverage.planning_agency_sfid
                ,coverage.planning_agency_ult_fbid
                ,coverage.planning_agency_ult_sfid
                ,coverage.planning_agency_ult_name
                ,coverage.split
                ,coverage.client_partner
                ,coverage.cp_username
                ,coverage.cp_start_date
                ,coverage.cp_manager
                ,coverage.cp_manager_username
                ,coverage.account_manager
                ,coverage.am_username
                ,coverage.am_start_date
                ,coverage.am_manager
                ,coverage.am_manager_username
                ,coverage.partner_manager
                ,coverage.pm_username
                ,coverage.pm_start_date
                ,coverage.pm_manager
                ,coverage.pm_manager_username
                ,coverage.agency_partner
                ,coverage.ap_username
                ,coverage.ap_start_date
                ,coverage.ap_manager
                ,coverage.ap_manager_username
                ,coverage.reseller_fbid
                ,coverage.reseller_sfid
                ,coverage.reseller_name
                ,coverage.program
                ,coverage.id_d_customer_account_adv
                ,coverage.advertiser_coverage_model_daa
                ,coverage.advertiser_program_daa
                ,coverage.agency_coverage_model_daa
                ,coverage.is_gat
                ,coverage.is_gcm
                ,coverage.is_magic93
                ,coverage.l12_advertiser_territory
                ,coverage.l10_advertiser_territory
                ,coverage.l8_advertiser_territory
                ,coverage.l6_advertiser_territory
                ,coverage.l4_advertiser_territory
                ,coverage.l2_advertiser_territory
                ,coverage.l12_usern_advertiser_territory
                ,coverage.l10_usern_advertiser_territory
                ,coverage.l12_manager_advertiser_territory
                ,coverage.l10_manager_advertiser_territory
                ,coverage.l8_manager_advertiser_territory
                ,coverage.l12_agency_territory
                ,coverage.l10_agency_territory
                ,coverage.l8_agency_territory
                ,coverage.l6_agency_territory
                ,coverage.l4_agency_territory
                ,coverage.l2_agency_territory
                ,coverage.l12_usern_agency_territory
                ,coverage.l10_usern_agency_territory
                ,coverage.l12_manager_agency_territory
                ,coverage.l10_manager_agency_territory
                ,coverage.l8_manager_agency_territory
                ,coverage.ad_account_l4_fbid
                ,coverage.ad_account_l8_fbid
                ,coverage.ad_account_l10_fbid
                ,coverage.ad_account_l12_fbid
                ,coverage.gms_optimal_target
                ,coverage.gms_liquidity_target
                ,coverage.gso_optimal_target
                ,coverage.gso_liquidity_target
                ,coverage.smb_optimal_target
                ,coverage.smb_liquidity_target
                ,coverage.l4_optimal_target
                ,coverage.l4_liquidity_target
                ,coverage.l6_optimal_target
                ,coverage.l6_liquidity_target
                ,coverage.l8_optimal_target
                ,coverage.l8_liquidity_target
                ,coverage.l10_optimal_target
                ,coverage.l10_liquidity_target
                ,coverage.l12_optimal_target
                ,coverage.l12_liquidity_target
                ,coverage.l12_reporting_territory
                ,coverage.l10_reporting_territory
                ,coverage.l8_reporting_territory
                ,coverage.l6_reporting_territory
                ,coverage.l4_reporting_territory
                ,coverage.l2_reporting_territory
                ,coverage.segmentation
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,l6_manager_agency_territory
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,program_optimal_target
                ,program_liquidity_target
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,revenue_segment
                FROM
                <TABLE:bpo_coverage_asis_stg_1> coverage
                WHERE ds = '<DATEID>' and is_gpa = false
                and is_fcast_eligible

           )
            --,
            --pixel as (
            --Select
            --ds,
            --ad_account_id,
            --Count(distinct(case when opportunity is not null then pixel_id else null end)) as pixel_issued,
            --Count(distinct(pixel_id)) as pixel_total
            --from gbg_scaled_pixel_raw
            --where cast(date_trunc('quarter',cast(ds as date)) as varchar) = '<quarter_id>'
            --group by 1,2
            --),
            --catalog as (
            --Select
            --ds,
            --ad_account_id,
            --sum(count_fires_match_any_catalog) catalog_match,
            --sum(count_fires) catalog_total
            --from f_smb_drive_pixel_match_rate_raw:ad_metrics
            --where cast(date_trunc('quarter',cast(ds as date)) as varchar) = '<quarter_id>'
            --group by 1,2
            --)
            Select
                products.date_id
                ,coverage.id_d_ad_account
                ,coverage.ad_account_id
                ,coverage.ad_account_name
                ,coverage.id_dh_territory
                ,coverage.advertiser_name
                ,coverage.advertiser_country
                ,coverage.advertiser_vertical
                ,coverage.advertiser_sub_vertical
                ,coverage.gvm_vertical_name
                ,coverage.gvm_sub_vertical_name
                ,coverage.specialty
                ,coverage.legacy_advertiser_sub_vertical
                ,coverage.legacy_advertiser_vertical
                ,coverage.region
                ,coverage.sub_region
                ,coverage.advertiser_fbid
                ,coverage.advertiser_sfid
                ,coverage.ultimate_parent_fbid
                ,coverage.ultimate_parent_sfid
                ,coverage.ultimate_parent_name
                ,coverage.planning_agency_name
                ,coverage.planning_agency_fbid
                ,coverage.planning_agency_sfid
                ,coverage.planning_agency_ult_fbid
                ,coverage.planning_agency_ult_sfid
                ,coverage.planning_agency_ult_name
                ,coverage.split
                ,coverage.client_partner
                ,coverage.cp_username
                ,coverage.cp_start_date
                ,coverage.cp_manager
                ,coverage.cp_manager_username
                ,coverage.account_manager
                ,coverage.am_username
                ,coverage.am_start_date
                ,coverage.am_manager
                ,coverage.am_manager_username
                ,coverage.partner_manager
                ,coverage.pm_username
                ,coverage.pm_start_date
                ,coverage.pm_manager
                ,coverage.pm_manager_username
                ,coverage.agency_partner
                ,coverage.ap_username
                ,coverage.ap_start_date
                ,coverage.ap_manager
                ,coverage.ap_manager_username
                ,coverage.reseller_fbid
                ,coverage.reseller_sfid
                ,coverage.reseller_name
                ,coverage.program
                ,coverage.id_d_customer_account_adv
                ,coverage.advertiser_coverage_model_daa
                ,coverage.advertiser_program_daa
                ,coverage.agency_coverage_model_daa
                ,coverage.is_gat
                ,coverage.is_gcm
                ,coverage.is_magic93
                ,coverage.l12_advertiser_territory
                ,coverage.l10_advertiser_territory
                ,coverage.l8_advertiser_territory
                ,coverage.l6_advertiser_territory
                ,coverage.l4_advertiser_territory
                ,coverage.l2_advertiser_territory
                ,coverage.l12_usern_advertiser_territory
                ,coverage.l10_usern_advertiser_territory
                ,coverage.l12_manager_advertiser_territory
                ,coverage.l10_manager_advertiser_territory
                ,coverage.l8_manager_advertiser_territory
                ,coverage.l12_agency_territory
                ,coverage.l10_agency_territory
                ,coverage.l8_agency_territory
                ,coverage.l6_agency_territory
                ,coverage.l4_agency_territory
                ,coverage.l2_agency_territory
                ,coverage.l12_usern_agency_territory
                ,coverage.l10_usern_agency_territory
                ,coverage.l12_manager_agency_territory
                ,coverage.l10_manager_agency_territory
                ,coverage.l8_manager_agency_territory
                ,coverage.ad_account_l4_fbid
                ,coverage.ad_account_l8_fbid
                ,coverage.ad_account_l10_fbid
                ,coverage.ad_account_l12_fbid
                ,coverage.l4_optimal_target
                ,coverage.gms_optimal_target
                ,coverage.gms_liquidity_target
                ,coverage.gso_optimal_target
                ,coverage.gso_liquidity_target
                ,coverage.smb_optimal_target
                ,coverage.smb_liquidity_target
                ,coverage.l4_liquidity_target
                ,coverage.l6_optimal_target
                ,coverage.l6_liquidity_target
                ,coverage.l8_optimal_target
                ,coverage.l8_liquidity_target
                ,coverage.l10_optimal_target
                ,coverage.l10_liquidity_target
                ,coverage.l12_optimal_target
                ,coverage.l12_liquidity_target
                ,coverage.l12_reporting_territory
                ,coverage.l10_reporting_territory
                ,coverage.l8_reporting_territory
                ,coverage.l6_reporting_territory
                ,coverage.l4_reporting_territory
                ,coverage.l2_reporting_territory
                ,coverage.revenue_segment
                ,products.canvas_ads_revn * split canvas_ads_revn
                ,products.fb_feed_opt_in_revn * split fb_feed_opt_in_revn
                ,products.app_install_revn * split app_install_revn
                ,products.dynamic_ads_revn * split dynamic_ads_revn
                ,products.carousel_revn * split carousel_revn
                ,products.cpas_revn * split cpas_revn
                ,products.playable_ads_revn * split playable_ads_revn
                ,products.app_event_optimisation_revn * split app_event_optimisation_revn
                ,products.reach_and_frequency_revn *coverage.split reach_and_frequency_revn
                ,products.sdk_revn *coverage.split sdk_revn
                ,products.page_likes_revn *coverage.split page_likes_revn
                ,products.vertical_video_revn *coverage.split vertical_video_revn
                ,products.slideshow_video_revn *coverage.split slideshow_video_revn
                ,products.click_to_messenger_revn *coverage.split click_to_messenger_revn
                ,products.short_form_video_revn *coverage.split short_form_video_revn
                ,products.video_revn *coverage.split video_revn
                ,products.dro_conversion_optimization_revn *coverage.split dro_conversion_optimization_revn
                ,products.dro_landing_page_views_revn *coverage.split dro_landing_page_views_revn
                ,products.dro_app_installs_revn *coverage.split dro_app_installs_revn
                ,products.dro_lead_generation_revn *coverage.split dro_lead_generation_revn
                ,products.dro_dra_offline_conversions_revn *coverage.split dro_dra_offline_conversions_revn
                ,products.dro_messenger_replies_revn *coverage.split dro_messenger_replies_revn
                ,products.dro_store_visits_revn *coverage.split dro_store_visits_revn
                ,products.dro_dra_roas_revn *coverage.split dro_dra_roas_revn
                ,products.reach_optimized_revn *coverage.split reach_optimized_revn
                ,products.video_views_optimized_revn *coverage.split video_views_optimized_revn
                ,products.bao_mfv_revn *coverage.split bao_mfv_revn
                ,products.bao_nonvideo_revn *coverage.split bao_nonvideo_revn
                ,products.videoviews_mfc_revn *coverage.split videoviews_mfc_revn
                ,products.pa_messenger_opt_in_revn *coverage.split pa_messenger_opt_in_revn
                ,products.pa_instream_revn *coverage.split pa_instream_revn
                ,products.pa_audience_network_revn *coverage.split pa_audience_network_revn
                ,products.pa_instant_articles_revn *coverage.split pa_instant_articles_revn
                ,products.pa_instagram_story_revn *coverage.split pa_instagram_story_revn
                ,products.po_messenger_opt_in_revn *coverage.split po_messenger_opt_in_revn
                ,products.po_instream_revn *coverage.split po_instream_revn
                ,products.po_audience_network_revn *coverage.split po_audience_network_revn
                ,products.po_instagram_revn *coverage.split po_instagram_revn
                ,products.po_instant_articles_revn *coverage.split po_instant_articles_revn
                ,products.po_instagram_story_revn *coverage.split po_instagram_story_revn
                ,products.instagram_opt_in_revn *coverage.split instagram_opt_in_revn
                ,products.instagram_stories_opt_in_revn *coverage.split instagram_stories_opt_in_revn
                ,products.audience_network_opt_in_revn *coverage.split audience_network_opt_in_revn
                ,products.messenger_opt_in_revn *coverage.split messenger_opt_in_revn
                ,products.home_instream_opt_in_revn *coverage.split home_instream_opt_in_revn
                ,products.mobile_instream_opt_in_revn *coverage.split mobile_instream_opt_in_revn
                ,products.fb_pixel_revn *coverage.split fb_pixel_revn
                ,products.lead_ads_revn *coverage.split lead_ads_revn
                ,products.mobile_first_video_revn *coverage.split mobile_first_video_revn
                ,products.collection_revn *coverage.split collection_revn
                ,products.instream_video_revn *coverage.split instream_video_revn
                ,products.messenger_revn *coverage.split messenger_revn
                ,products.instagram_revn *coverage.split instagram_revn
                ,products.fb_stories_opt_in_revn *coverage.split fb_stories_opt_in_revn
                ,products.click_to_whatsapp_revn *coverage.split click_to_whatsapp_revn
                ,products.revenue *split revenue
                ,products.optimal_revn *split optimal
                ,products.liquidity_revn *split liquidity
                ,products.ig_stories_revn *split ig_stories_revn
                ,products.messenger_ads_revn *split	messenger_ads_revn
                ,products.audience_network_revn *split audience_network_revn
                ,products.web_conversion_revn *split web_conversion_revn
                ,products.website_clicks_revn *split website_clicks_revn
                ,products.platform_messenger_revn *split platform_messenger_revn
                ,products.facebook_revn	*split facebook_revn
                ,products.sponsored_messages_revn *split sponsored_messages_revn
                ,products.mobile_feed_impressions *split mobile_feed_impressions
                ,products.ig_stories_impressions *split	 ig_stories_impressions
                ,products.ig_stream_impressions	*split ig_stream_impressions
                ,products.messenger_impressions *split messenger_impressions
                ,products.mobile_feed_revn *split mobile_feed_revn
                ,products.web_feed_impressions *split web_feed_impressions
                ,products.dr_revn *split dr_revn
                ,products.brand_revn *split brand_revn
                ,coverage.segmentation
                ,products.search_ads_revn * split search_ads_revn
                ,products.ig_feed_revn * split ig_feed_revn
                ,products.marketplace_revn * split marketplace_revn
                ,products.branded_content_revn * split branded_content_revn
                ,products.messenger_stories_revn * split messenger_stories_revn
                ,products.groups_revn * split groups_revn
                ,products.fb_stories_revn * split fb_stories_revn
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,null  pixel_issued
                ,null pixel_total
                ,null catalog_match
                ,null catolog_total
                ,products.cbb_revn * split cbb_revn
                ,products.four_plus_placements_revn * split four_plus_placements_revn
                ,products.automatic_placement_revn * split automatic_placement_revn
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,l6_manager_agency_territory
                ,program_optimal_target
                ,program_liquidity_target
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,products.resilient_revn * split resilient_revn
                ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts

            from products

            LEFT  join coverage on
            coverage.ad_account_id = products.ad_account_id

            --left join pixel on
            --products.ad_account_id = pixel.ad_account_id
            --and products.date_id = pixel.ds

            --left join catalog on
            --products.ad_account_id = catalog.ad_account_id
            --and products.date_id = catalog.ds

            where (
            revenue is not null
           -- or pixel.pixel_total is not null or
           -- catalog.catalog_total is not null
           )
            """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select


class BpoGmsSolutionsAdAccountLevel:

    """@docstring ad account level product table with daily snapshots"""

    def __init__(self):
        self.name = "<TABLE:bpo_gms_solutions_ad_account_level>"
        self.create = """
            CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_solutions_ad_account_level> (
            date_id varchar,
            id_d_ad_account bigint,
            ad_account_id bigint,
            ad_account_name varchar,
            id_dh_territory bigint,
            advertiser_name varchar,
            advertiser_country varchar,
            advertiser_vertical varchar,
            advertiser_sub_vertical varchar,
            gvm_vertical_name varchar,
            gvm_sub_vertical_name varchar,
            specialty VARCHAR,
            legacy_advertiser_sub_vertical VARCHAR,
            legacy_advertiser_vertical VARCHAR,
            region varchar,
            sub_region varchar,
            advertiser_fbid varchar,
            advertiser_sfid varchar,
            ultimate_parent_fbid varchar,
            ultimate_parent_sfid varchar,
            ultimate_parent_name varchar,
            planning_agency_name varchar,
            planning_agency_fbid varchar,
            planning_agency_sfid varchar,
            planning_agency_ult_fbid varchar,
            planning_agency_ult_sfid varchar,
            planning_agency_ult_name varchar,
            split double,
            client_partner varchar,
            cp_username varchar,
            cp_start_date varchar,
            cp_manager varchar,
            cp_manager_username varchar,
            account_manager varchar,
            am_username varchar,
            am_start_date varchar,
            am_manager varchar,
            am_manager_username varchar,
            partner_manager varchar,
            pm_username varchar,
            pm_start_date varchar,
            pm_manager varchar,
            pm_manager_username varchar,
            agency_partner varchar,
            ap_username varchar,
            ap_start_date varchar,
            ap_manager varchar,
            ap_manager_username varchar,
            reseller_fbid varchar,
            reseller_sfid varchar,
            reseller_name varchar,
            program varchar,
            id_d_customer_account_adv bigint,
            advertiser_coverage_model_daa varchar,
            advertiser_program_daa varchar,
            agency_coverage_model_daa varchar,
            is_gat boolean,
            is_gcm boolean,
            is_magic93 boolean,
            l12_advertiser_territory varchar,
            l10_advertiser_territory varchar,
            l8_advertiser_territory varchar,
            l6_advertiser_territory varchar,
            l4_advertiser_territory varchar,
            l2_advertiser_territory varchar,
            l12_usern_advertiser_territory varchar,
            l10_usern_advertiser_territory varchar,
            l12_manager_advertiser_territory varchar,
            l10_manager_advertiser_territory varchar,
            l8_manager_advertiser_territory varchar,
            l12_agency_territory varchar,
            l10_agency_territory varchar,
            l8_agency_territory varchar,
            l6_agency_territory varchar,
            l4_agency_territory varchar,
            l2_agency_territory varchar,
            l12_usern_agency_territory varchar,
            l10_usern_agency_territory varchar,
            l12_manager_agency_territory varchar,
            l10_manager_agency_territory varchar,
            l8_manager_agency_territory varchar,
            ad_account_l4_fbid BIGINT,
            ad_account_l8_fbid BIGINT,
            ad_account_l10_fbid BIGINT,
            ad_account_l12_fbid BIGINT,
            gms_optimal_target double,
            gms_liquidity_target double,
            gso_optimal_target double,
            gso_liquidity_target double,
            smb_optimal_target double,
            smb_liquidity_target double,
            l4_optimal_target double,
            l4_liquidity_target double,
            l8_optimal_target double,
            l8_liquidity_target double,
            l10_optimal_target double,
            l10_liquidity_target double,
            l12_optimal_target double,
            l12_liquidity_target double,
            canvas_ads_revn double,
            fb_feed_opt_in_revn double,
            app_install_revn double,
            dynamic_ads_revn double,
            carousel_revn double,
            cpas_revn double,
            playable_ads_revn double,
            app_event_optimisation_revn double,
            reach_and_frequency_revn double,
            sdk_revn double,
            page_likes_revn double,
            vertical_video_revn double,
            slideshow_video_revn double,
            click_to_messenger_revn double,
            short_form_video_revn double,
            video_revn double,
            dro_conversion_optimization_revn double,
            dro_landing_page_views_revn double,
            dro_app_installs_revn double,
            dro_lead_generation_revn double,
            dro_dra_offline_conversions_revn double,
            dro_messenger_replies_revn double,
            dro_store_visits_revn double,
            dro_dra_roas_revn double,
            reach_optimized_revn double,
            video_views_optimized_revn double,
            bao_mfv_revn double,
            bao_nonvideo_revn double,
            videoviews_mfc_revn double,
            pa_messenger_opt_in_revn double,
            pa_instream_revn double,
            pa_audience_network_revn double,
            pa_instant_articles_revn double,
            pa_instagram_story_revn double,
            po_messenger_opt_in_revn double,
            po_instream_revn double,
            po_audience_network_revn double,
            po_instagram_revn double,
            po_instant_articles_revn double,
            po_instagram_story_revn double,
            instagram_opt_in_revn double,
            instagram_stories_opt_in_revn double,
            audience_network_opt_in_revn double,
            messenger_opt_in_revn double,
            home_instream_opt_in_revn double,
            mobile_instream_opt_in_revn double,
            fb_pixel_revn double,
            lead_ads_revn double,
            mobile_first_video_revn double,
            collection_revn double,
            instream_video_revn double,
            messenger_revn double,
            instagram_revn double,
            fb_stories_opt_in_revn double,
            click_to_whatsapp_revn double,
            revenue double,
            optimal double,
            liquidity double,
            ig_stories_revn	double,
            messenger_ads_revn	double,
            audience_network_revn	double,
            web_conversion_revn	double,
            website_clicks_revn	double,
            platform_messenger_revn	double,
            facebook_revn	double,
            sponsored_messages_revn	double,
            mobile_feed_impressions	double,
            ig_stories_impressions	double,
            ig_stream_impressions	double,
            messenger_impressions	double,
            mobile_feed_revn	double,
            web_feed_impressions double,
            dr_revn	double,
            brand_revn double,
            l12_reporting_territory VARCHAR,
            l10_reporting_territory VARCHAR,
            l8_reporting_territory  VARCHAR,
            l6_reporting_territory  VARCHAR,
            l4_reporting_territory  VARCHAR,
            l2_reporting_territory  VARCHAR,
            segmentation VARCHAR,
            l6_manager_agency_territory VARCHAR,
            search_ads_revn DOUBLE COMMENT 'Recognized Search Ads Revenue',
            ig_feed_revn DOUBLE COMMENT 'Recognized Instagram Feed Revenue',
            marketplace_revn DOUBLE COMMENT 'Recognized MarketPlace Revenue',
            branded_content_revn DOUBLE COMMENT 'Recognized Branded Content Revenue',
            messenger_stories_revn DOUBLE COMMENT 'Recognized Messenger Stories Revenue',
            groups_revn DOUBLE COMMENT ' Recognized groups revenue ',
            fb_stories_revn DOUBLE COMMENT 'Delivered Revenue for FB stories Placement',
            l12_reporting_terr_mgr VARCHAR,
            l10_reporting_terr_mgr VARCHAR,
            l8_reporting_terr_mgr VARCHAR,
            l6_reporting_terr_mgr VARCHAR,
            pixel_issued double,
            pixel_total double,
            catalog_match double,
            catolog_total double,
            cbb_revn double,
            four_plus_placements_revn double,
            automatic_placement_revn double,
            rep_fbid_csm VARCHAR COMMENT 'FBID for Client Solutions Manager https://fburl.com/wut/cad62z1j',
            client_solutions_manager VARCHAR COMMENT 'full name for Client Solutions Manager',
            csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
            csm_start_date varchar COMMENT 'Start Date of CSM for a given ad account',
            csm_manager varchar COMMENT 'Full name for a CSMs manager',
            csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
            rep_fbid_asm VARCHAR COMMENT 'FBID for Agency Solutions Manager',
            agency_solutions_manager VARCHAR COMMENT 'Full name for ASM' ,
            asm_username varchar COMMENT 'UnixName for ASM',
            asm_start_date varchar COMMENT 'start date of an asm for a given ad account ',
            asm_manager varchar COMMENT 'Full name for an  ASMs manager',
            asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
            reseller_partner VARCHAR COMMENT 'Full name for reseller partner',
            rp_username varchar COMMENT 'UnixName for reseller partner',
            rp_start_date varchar COMMENT 'start date for a reseller partner for a given ad account',
            rp_manager varchar COMMENT 'Full name for an RPs manager',
            rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
            sales_adv_country_group VARCHAR,
            sales_adv_subregion VARCHAR,
            sales_adv_region VARCHAR,
            market VARCHAR,
            program_optimal_target double,
            program_liquidity_target double,
            l6_optimal_target double,
            l6_liquidity_target double,
            country_agc VARCHAR,
            market_agc VARCHAR,
            region_agc VARCHAR,
            sub_region_agc VARCHAR,
            business_type_adv VARCHAR,
            business_type_agc VARCHAR,
            planning_agency_operating_co VARCHAR,
            program_agency VARCHAR,
            china_export_advertiser VARCHAR,
            export_advertiser_country VARCHAR,
            billing_country_adv VARCHAR,
            billing_region_adv VARCHAR,
            billing_country_agc VARCHAR,
            billing_region_agc VARCHAR,
            hq_country_adv VARCHAR,
            hq_region_adv VARCHAR,
            hq_country_agc VARCHAR,
            hq_region_agc VARCHAR,
            resilient_revn  DOUBLE,
            revenue_segment VARCHAR,
            ts VARCHAR,
            ds varchar
       )
        WITH (
                partitioned_by = ARRAY['ds'],
                        retention_days = <RETENTION:90>,
                        uii=false
                       )

         """
        self.select = """
            Select
                date_id
                ,id_d_ad_account
                ,ad_account_id
                ,ad_account_name
                ,id_dh_territory
                ,advertiser_name
                ,advertiser_country
                ,advertiser_vertical
                ,advertiser_sub_vertical
                ,gvm_vertical_name
                ,gvm_sub_vertical_name
                ,specialty
                ,legacy_advertiser_sub_vertical
                ,legacy_advertiser_vertical
                ,region
                ,sub_region
                ,advertiser_fbid
                ,advertiser_sfid
                ,ultimate_parent_fbid
                ,ultimate_parent_sfid
                ,ultimate_parent_name
                ,planning_agency_name
                ,planning_agency_fbid
                ,planning_agency_sfid
                ,planning_agency_ult_fbid
                ,planning_agency_ult_sfid
                ,planning_agency_ult_name
                ,split
                ,client_partner
                ,cp_username
                ,cp_start_date
                ,cp_manager
                ,cp_manager_username
                ,account_manager
                ,am_username
                ,am_start_date
                ,am_manager
                ,am_manager_username
                ,partner_manager
                ,pm_username
                ,pm_start_date
                ,pm_manager
                ,pm_manager_username
                ,agency_partner
                ,ap_username
                ,ap_start_date
                ,ap_manager
                ,ap_manager_username
                ,reseller_fbid
                ,reseller_sfid
                ,reseller_name
                ,program
                ,id_d_customer_account_adv
                ,advertiser_coverage_model_daa
                ,advertiser_program_daa
                ,agency_coverage_model_daa
                ,is_gat
                ,is_gcm
                ,is_magic93
                ,l12_advertiser_territory
                ,l10_advertiser_territory
                ,l8_advertiser_territory
                ,l6_advertiser_territory
                ,l4_advertiser_territory
                ,l2_advertiser_territory
                ,l12_usern_advertiser_territory
                ,l10_usern_advertiser_territory
                ,l12_manager_advertiser_territory
                ,l10_manager_advertiser_territory
                ,l8_manager_advertiser_territory
                ,l12_agency_territory
                ,l10_agency_territory
                ,l8_agency_territory
                ,l6_agency_territory
                ,l4_agency_territory
                ,l2_agency_territory
                ,l12_usern_agency_territory
                ,l10_usern_agency_territory
                ,l12_manager_agency_territory
                ,l10_manager_agency_territory
                ,l8_manager_agency_territory
                ,ad_account_l4_fbid
                ,ad_account_l8_fbid
                ,ad_account_l10_fbid
                ,ad_account_l12_fbid
                ,l4_optimal_target
                ,gms_optimal_target
                ,gms_liquidity_target
                ,gso_optimal_target
                ,gso_liquidity_target
                ,smb_optimal_target
                ,smb_liquidity_target
                ,l4_liquidity_target
                ,l6_optimal_target
                ,l6_liquidity_target
                ,l8_optimal_target
                ,l8_liquidity_target
                ,l10_optimal_target
                ,l10_liquidity_target
                ,l12_optimal_target
                ,l12_liquidity_target
                ,l12_reporting_territory
                ,l10_reporting_territory
                ,l8_reporting_territory
                ,l6_reporting_territory
                ,l4_reporting_territory
                ,l2_reporting_territory
                ,canvas_ads_revn
                ,fb_feed_opt_in_revn
                ,app_install_revn
                ,dynamic_ads_revn
                ,carousel_revn
                ,cpas_revn
                ,playable_ads_revn
                ,app_event_optimisation_revn
                ,reach_and_frequency_revn
                ,sdk_revn
                ,page_likes_revn
                ,vertical_video_revn
                ,slideshow_video_revn
                ,click_to_messenger_revn
                ,short_form_video_revn
                ,video_revn
                ,dro_conversion_optimization_revn
                ,dro_landing_page_views_revn
                ,dro_app_installs_revn
                ,dro_lead_generation_revn
                ,dro_dra_offline_conversions_revn
                ,dro_messenger_replies_revn
                ,dro_store_visits_revn
                ,dro_dra_roas_revn
                ,reach_optimized_revn
                ,video_views_optimized_revn
                ,bao_mfv_revn
                ,bao_nonvideo_revn
                ,videoviews_mfc_revn
                ,pa_messenger_opt_in_revn
                ,pa_instream_revn
                ,pa_audience_network_revn
                ,pa_instant_articles_revn
                ,pa_instagram_story_revn
                ,po_messenger_opt_in_revn
                ,po_instream_revn
                ,po_audience_network_revn
                ,po_instagram_revn
                ,po_instant_articles_revn
                ,po_instagram_story_revn
                ,instagram_opt_in_revn
                ,instagram_stories_opt_in_revn
                ,audience_network_opt_in_revn
                ,messenger_opt_in_revn
                ,home_instream_opt_in_revn
                ,mobile_instream_opt_in_revn
                ,fb_pixel_revn
                ,lead_ads_revn
                ,mobile_first_video_revn
                ,collection_revn
                ,instream_video_revn
                ,messenger_revn
                ,instagram_revn
                ,fb_stories_opt_in_revn
                ,click_to_whatsapp_revn
                ,revenue
                ,optimal
                ,liquidity
                ,ig_stories_revn
                ,messenger_ads_revn
                ,audience_network_revn
                ,web_conversion_revn
                ,website_clicks_revn
                ,platform_messenger_revn
                ,facebook_revn
                ,sponsored_messages_revn
                ,mobile_feed_impressions
                ,ig_stories_impressions
                ,ig_stream_impressions
                ,messenger_impressions
                ,mobile_feed_revn
                ,web_feed_impressions
                ,dr_revn
                ,brand_revn
                ,segmentation
                ,search_ads_revn
                ,ig_feed_revn
                ,marketplace_revn
                ,branded_content_revn
                ,messenger_stories_revn
                ,groups_revn
                ,fb_stories_revn
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,pixel_issued
                ,pixel_total
                ,catalog_match
                ,catolog_total
                ,cbb_revn
                ,four_plus_placements_revn
                ,automatic_placement_revn
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,l6_manager_agency_territory
                ,program_optimal_target
                ,program_liquidity_target
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,resilient_revn
                ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
                ,revenue_segment
            from <TABLE:bpo_gms_solutions_ad_account_level_snapshot>

            where ds = '<DATEID>'

            """

        self.delete = """
            DELETE FROM <TABLE:bpo_gms_solutions_ad_account_level>
            where ds <> '<DATEID>'
            """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select

    def get_delete(self):
        """returns a delete statement that will remove all partitions
        from a table that are not '<DATEID>'"""
        return self.delete


class BpoGmsSolutionsSnapshot:
    """@docstring advertiser level solutions table"""

    def __init__(self):
        self.name = "<TABLE:bpo_gms_solutions_snapshot>"
        self.create = """
            CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_solutions_snapshot> (
                date_id varchar,
                id_dh_territory bigint,
                advertiser_name varchar,
                advertiser_country varchar,
                advertiser_vertical varchar,
                advertiser_sub_vertical varchar,
                gvm_vertical_name varchar,
                gvm_sub_vertical_name varchar,
                specialty VARCHAR,
                legacy_advertiser_sub_vertical VARCHAR,
                legacy_advertiser_vertical VARCHAR,
                region varchar,
                sub_region varchar,
                advertiser_fbid varchar,
                advertiser_sfid varchar,
                ultimate_parent_fbid varchar,
                ultimate_parent_sfid varchar,
                ultimate_parent_name varchar,
                planning_agency_name varchar,
                planning_agency_fbid varchar,
                planning_agency_sfid varchar,
                planning_agency_ult_fbid varchar,
                planning_agency_ult_sfid varchar,
                planning_agency_ult_name varchar,
                split double,
                client_partner varchar,
                cp_username varchar,
                cp_start_date varchar,
                cp_manager varchar,
                cp_manager_username varchar,
                account_manager varchar,
                am_username varchar,
                am_start_date varchar,
                am_manager varchar,
                am_manager_username varchar,
                partner_manager varchar,
                pm_username varchar,
                pm_start_date varchar,
                pm_manager varchar,
                pm_manager_username varchar,
                agency_partner varchar,
                ap_username varchar,
                ap_start_date varchar,
                ap_manager varchar,
                ap_manager_username varchar,
                reseller_fbid varchar,
                reseller_sfid varchar,
                reseller_name varchar,
                program varchar,
                id_d_customer_account_adv bigint,
                advertiser_coverage_model_daa varchar,
                advertiser_program_daa varchar,
                agency_coverage_model_daa varchar,
                is_gat boolean,
                is_gcm boolean,
                is_magic93 boolean,
                l12_advertiser_territory varchar,
                l10_advertiser_territory varchar,
                l8_advertiser_territory varchar,
                l6_advertiser_territory varchar,
                l4_advertiser_territory varchar,
                l2_advertiser_territory varchar,
                l12_usern_advertiser_territory varchar,
                l10_usern_advertiser_territory varchar,
                l12_manager_advertiser_territory varchar,
                l10_manager_advertiser_territory varchar,
                l8_manager_advertiser_territory varchar,
                l12_agency_territory varchar,
                l10_agency_territory varchar,
                l8_agency_territory varchar,
                l6_agency_territory varchar,
                l4_agency_territory varchar,
                l2_agency_territory varchar,
                l12_usern_agency_territory varchar,
                l10_usern_agency_territory varchar,
                l12_manager_agency_territory varchar,
                l10_manager_agency_territory varchar,
                l8_manager_agency_territory varchar,
                gms_optimal_target double,
                gms_liquidity_target double,
                gso_optimal_target double,
                gso_liquidity_target double,
                smb_optimal_target double,
                smb_liquidity_target double,
                l4_optimal_target double,
                l4_liquidity_target double,
                l8_optimal_target double,
                l8_liquidity_target double,
                l10_optimal_target double,
                l10_liquidity_target double,
                l12_optimal_target double,
                l12_liquidity_target double,
                canvas_ads_revn double,
                fb_feed_opt_in_revn double,
                app_install_revn double,
                dynamic_ads_revn double,
                carousel_revn double,
                cpas_revn double,
                playable_ads_revn double,
                app_event_optimisation_revn double,
                reach_and_frequency_revn double,
                sdk_revn double,
                page_likes_revn double,
                vertical_video_revn double,
                slideshow_video_revn double,
                click_to_messenger_revn double,
                short_form_video_revn double,
                video_revn double,
                dro_conversion_optimization_revn double,
                dro_landing_page_views_revn double,
                dro_app_installs_revn double,
                dro_lead_generation_revn double,
                dro_dra_offline_conversions_revn double,
                dro_messenger_replies_revn double,
                dro_store_visits_revn double,
                dro_dra_roas_revn double,
                reach_optimized_revn double,
                video_views_optimized_revn double,
                bao_mfv_revn double,
                bao_nonvideo_revn double,
                videoviews_mfc_revn double,
                pa_messenger_opt_in_revn double,
                pa_instream_revn double,
                pa_audience_network_revn double,
                pa_instant_articles_revn double,
                pa_instagram_story_revn double,
                po_messenger_opt_in_revn double,
                po_instream_revn double,
                po_audience_network_revn double,
                po_instagram_revn double,
                po_instant_articles_revn double,
                po_instagram_story_revn double,
                instagram_opt_in_revn double,
                instagram_stories_opt_in_revn double,
                audience_network_opt_in_revn double,
                messenger_opt_in_revn double,
                home_instream_opt_in_revn double,
                mobile_instream_opt_in_revn double,
                fb_pixel_revn double,
                lead_ads_revn double,
                mobile_first_video_revn double,
                collection_revn double,
                instream_video_revn double,
                messenger_revn double,
                instagram_revn double,
                fb_stories_opt_in_revn double,
                click_to_whatsapp_revn double,
                revenue double,
                optimal double,
                liquidity double,
                ig_stories_revn	double,
                messenger_ads_revn	double,
                audience_network_revn	double,
                web_conversion_revn	double,
                website_clicks_revn	double,
                platform_messenger_revn	double,
                facebook_revn	double,
                sponsored_messages_revn	double,
                mobile_feed_impressions	double,
                ig_stories_impressions	double,
                ig_stream_impressions	double,
                messenger_impressions	double,
                mobile_feed_revn	double,
                web_feed_impressions	double,
                dr_revn	double,
                brand_revn double,
                l12_reporting_territory VARCHAR,
                l10_reporting_territory VARCHAR,
                l8_reporting_territory  VARCHAR,
                l6_reporting_territory  VARCHAR,
                l4_reporting_territory  VARCHAR,
                l2_reporting_territory  VARCHAR,
                segmentation VARCHAR,
                search_ads_revn DOUBLE COMMENT 'Recognized Search Ads Revenue',
                ig_feed_revn DOUBLE COMMENT 'Recognized Instagram Feed Revenue',
                marketplace_revn DOUBLE COMMENT 'Recognized MarketPlace Revenue',
                branded_content_revn DOUBLE COMMENT 'Recognized Branded Content Revenue',
                messenger_stories_revn DOUBLE COMMENT 'Recognized Messenger Stories Revenue',
                groups_revn DOUBLE COMMENT ' Recognized groups revenue ',
                fb_stories_revn DOUBLE COMMENT 'Recognized Revenue for FB stories Placement',
                l12_reporting_terr_mgr VARCHAR,
                l10_reporting_terr_mgr VARCHAR,
                l8_reporting_terr_mgr VARCHAR,
                l6_reporting_terr_mgr VARCHAR,
                pixel_issued double,
                pixel_total double,
                catalog_match double,
                catolog_total double,
                cbb_revn double,
                four_plus_placements_revn double,
                automatic_placement_revn double,
                l6_manager_agency_territory VARCHAR,
                rep_fbid_csm VARCHAR COMMENT 'FBID for CSM https://fburl.com/wut/cad62z1j',
                client_solutions_manager VARCHAR COMMENT 'full name for Client Solutions Manager',
                csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
                csm_start_date varchar COMMENT 'Start Date of CSM for a given ad account',
                csm_manager varchar COMMENT 'Full name for a CSMs manager',
                csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
                rep_fbid_asm VARCHAR COMMENT 'FBID for Agency Solutions Manager',
                agency_solutions_manager VARCHAR COMMENT 'Full name for ASM' ,
                asm_username varchar COMMENT 'UnixName for ASM',
                asm_start_date varchar COMMENT 'start date of an asm for a given ad account ',
                asm_manager varchar COMMENT 'Full name for an  ASMs manager',
                asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
                reseller_partner VARCHAR COMMENT 'Full name for reseller partner',
                rp_username varchar COMMENT 'UnixName for reseller partner',
                rp_start_date varchar COMMENT 'start date for a reseller partner for a given ad account',
                rp_manager varchar COMMENT 'Full name for an RPs manager',
                rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
                sales_adv_country_group VARCHAR,
                sales_adv_subregion VARCHAR,
                sales_adv_region VARCHAR,
                market VARCHAR,
                program_optimal_target double,
                program_liquidity_target double,
                l6_optimal_target double,
                l6_liquidity_target double,
                country_agc VARCHAR,
                market_agc VARCHAR,
                region_agc VARCHAR,
                sub_region_agc VARCHAR,
                business_type_adv VARCHAR,
                business_type_agc VARCHAR,
                planning_agency_operating_co VARCHAR,
                program_agency VARCHAR,
                china_export_advertiser VARCHAR,
                export_advertiser_country VARCHAR,
                billing_country_adv VARCHAR,
                billing_region_adv VARCHAR,
                billing_country_agc VARCHAR,
                billing_region_agc VARCHAR,
                hq_country_adv VARCHAR,
                hq_region_adv VARCHAR,
                hq_country_agc VARCHAR,
                hq_region_agc VARCHAR,
                resilient_revn  DOUBLE,
                ts VARCHAR,
                revenue_segment VARCHAR,
                ds varchar
           )
            WITH (
                            partitioned_by = ARRAY['ds'],
                            retention_days = <RETENTION:90>,
                            uii=false
                           )"""
        self.select = """
            SELECT
                date_id
                ,id_dh_territory
                ,advertiser_name
                ,advertiser_country
                ,advertiser_vertical
                ,advertiser_sub_vertical
                ,gvm_vertical_name
                ,gvm_sub_vertical_name
                ,specialty
                ,NULL legacy_advertiser_sub_vertical
                ,NULL legacy_advertiser_vertical
                ,region
                ,sub_region
                ,advertiser_fbid
                ,advertiser_sfid
                ,ultimate_parent_fbid
                ,ultimate_parent_sfid
                ,ultimate_parent_name
                ,planning_agency_name
                ,planning_agency_fbid
                ,planning_agency_sfid
                ,planning_agency_ult_fbid
                ,planning_agency_ult_sfid
                ,planning_agency_ult_name
                ,split
                ,client_partner
                ,cp_username
                ,cp_start_date
                ,cp_manager
                ,cp_manager_username
                ,account_manager
                ,am_username
                ,am_start_date
                ,am_manager
                ,am_manager_username
                ,partner_manager
                ,pm_username
                ,pm_start_date
                ,pm_manager
                ,pm_manager_username
                ,agency_partner
                ,ap_username
                ,ap_start_date
                ,ap_manager
                ,ap_manager_username
                ,reseller_fbid
                ,reseller_sfid
                ,reseller_name
                ,program
                ,id_d_customer_account_adv
                ,advertiser_coverage_model_daa
                ,advertiser_program_daa
                ,agency_coverage_model_daa
                ,is_gat
                ,is_gcm
                ,is_magic93
                ,l12_advertiser_territory
                ,l10_advertiser_territory
                ,l8_advertiser_territory
                ,l6_advertiser_territory
                ,l4_advertiser_territory
                ,l2_advertiser_territory
                ,l12_usern_advertiser_territory
                ,l10_usern_advertiser_territory
                ,l12_manager_advertiser_territory
                ,l10_manager_advertiser_territory
                ,l8_manager_advertiser_territory
                ,l12_agency_territory
                ,l10_agency_territory
                ,l8_agency_territory
                ,l6_agency_territory
                ,l4_agency_territory
                ,l2_agency_territory
                ,l12_usern_agency_territory
                ,l10_usern_agency_territory
                ,l12_manager_agency_territory
                ,l10_manager_agency_territory
                ,l8_manager_agency_territory
                ,gms_optimal_target
                ,gms_liquidity_target
                ,gso_optimal_target
                ,gso_liquidity_target
                ,smb_optimal_target
                ,smb_liquidity_target
                ,l4_optimal_target
                ,l4_liquidity_target
                ,l6_optimal_target
                ,l6_liquidity_target
                ,l8_optimal_target
                ,l8_liquidity_target
                ,l10_optimal_target
                ,l10_liquidity_target
                ,l12_optimal_target
                ,l12_liquidity_target
                ,l12_reporting_territory
                ,l10_reporting_territory
                ,l8_reporting_territory
                ,l6_reporting_territory
                ,l4_reporting_territory
                ,l2_reporting_territory
                ,segmentation
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,l6_manager_agency_territory
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,program_optimal_target
                ,program_liquidity_target
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,revenue_segment
                ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
                ,SUM(canvas_ads_revn) canvas_ads_revn
                ,SUM(fb_feed_opt_in_revn) fb_feed_opt_in_revn
                ,SUM(app_install_revn) app_install_revn
                ,SUM(dynamic_ads_revn) dynamic_ads_revn
                ,SUM(carousel_revn) carousel_revn
                ,SUM(cpas_revn) cpas_revn
                ,SUM(playable_ads_revn) playable_ads_revn
                ,SUM(app_event_optimisation_revn) app_event_optimisation_revn
                ,SUM(reach_and_frequency_revn) reach_and_frequency_revn
                ,SUM(sdk_revn) sdk_revn
                ,SUM(page_likes_revn) page_likes_revn
                ,SUM(vertical_video_revn) vertical_video_revn
                ,SUM(slideshow_video_revn) slideshow_video_revn
                ,SUM(click_to_messenger_revn) click_to_messenger_revn
                ,SUM(short_form_video_revn) short_form_video_revn
                ,SUM(video_revn) video_revn
                ,SUM(dro_conversion_optimization_revn) dro_conversion_optimization_revn
                ,SUM(dro_landing_page_views_revn) dro_landing_page_views_revn
                ,SUM(dro_app_installs_revn) dro_app_installs_revn
                ,SUM(dro_lead_generation_revn) dro_lead_generation_revn
                ,SUM(dro_dra_offline_conversions_revn) dro_dra_offline_conversions_revn
                ,SUM(dro_messenger_replies_revn) dro_messenger_replies_revn
                ,SUM(dro_store_visits_revn) dro_store_visits_revn
                ,SUM(dro_dra_roas_revn) dro_dra_roas_revn
                ,SUM(reach_optimized_revn) reach_optimized_revn
                ,SUM(video_views_optimized_revn) video_views_optimized_revn
                ,SUM(bao_mfv_revn) bao_mfv_revn
                ,SUM(bao_nonvideo_revn) bao_nonvideo_revn
                ,SUM(videoviews_mfc_revn) videoviews_mfc_revn
                ,SUM(pa_messenger_opt_in_revn) pa_messenger_opt_in_revn
                ,SUM(pa_instream_revn) pa_instream_revn
                ,SUM(pa_audience_network_revn) pa_audience_network_revn
                ,SUM(pa_instant_articles_revn) pa_instant_articles_revn
                ,SUM(pa_instagram_story_revn) pa_instagram_story_revn
                ,SUM(po_messenger_opt_in_revn) po_messenger_opt_in_revn
                ,SUM(po_instream_revn) po_instream_revn
                ,SUM(po_audience_network_revn) po_audience_network_revn
                ,SUM(po_instagram_revn) po_instagram_revn
                ,SUM(po_instant_articles_revn) po_instant_articles_revn
                ,SUM(po_instagram_story_revn) po_instagram_story_revn
                ,SUM(instagram_opt_in_revn) instagram_opt_in_revn
                ,SUM(instagram_stories_opt_in_revn) instagram_stories_opt_in_revn
                ,SUM(audience_network_opt_in_revn) audience_network_opt_in_revn
                ,SUM(messenger_opt_in_revn) messenger_opt_in_revn
                ,SUM(home_instream_opt_in_revn) home_instream_opt_in_revn
                ,SUM(mobile_instream_opt_in_revn) mobile_instream_opt_in_revn
                ,SUM(fb_pixel_revn) fb_pixel_revn
                ,SUM(lead_ads_revn) lead_ads_revn
                ,SUM(mobile_first_video_revn) mobile_first_video_revn
                ,SUM(collection_revn) collection_revn
                ,SUM(instream_video_revn) instream_video_revn
                ,SUM(messenger_revn) messenger_revn
                ,SUM(instagram_revn) instagram_revn
                ,SUM(fb_stories_opt_in_revn) fb_stories_opt_in_revn
                ,SUM(click_to_whatsapp_revn) click_to_whatsapp_revn
                ,SUM(revenue) revenue
                ,SUM(optimal) optimal
                ,SUM(liquidity) liquidity
                ,SUM(ig_stories_revn) ig_stories_revn
                ,SUM(messenger_ads_revn) messenger_ads_revn
                ,SUM(audience_network_revn) audience_network_revn
                ,SUM(web_conversion_revn) web_conversion_revn
                ,SUM(website_clicks_revn) website_clicks_revn
                ,SUM(platform_messenger_revn) platform_messenger_revn
                ,SUM(facebook_revn) facebook_revn
                ,SUM(sponsored_messages_revn) sponsored_messages_revn
                ,SUM(mobile_feed_impressions) mobile_feed_impressions
                ,SUM(ig_stories_impressions) ig_stories_impressions
                ,SUM(ig_stream_impressions) ig_stream_impressions
                ,SUM(messenger_impressions) messenger_impressions
                ,SUM(mobile_feed_revn) mobile_feed_revn
                ,SUM(web_feed_impressions) web_feed_impressions
                ,SUM(dr_revn) dr_revn
                ,SUM(brand_revn) brand_revn
                ,SUM(search_ads_revn) search_ads_revn
                ,SUM(ig_feed_revn) ig_feed_revn
                ,SUM(marketplace_revn) marketplace_revn
                ,SUM(branded_content_revn) branded_content_revn
                ,SUM(messenger_stories_revn) messenger_stories_revn
                ,SUM(groups_revn) groups_revn
                ,SUM(fb_stories_revn) fb_stories_revn
                ,SUM(pixel_issued) pixel_issued
                ,SUM(pixel_total) pixel_total
                ,SUM(catalog_match) catalog_match
                ,SUM(catolog_total) catolog_total
                ,SUM(cbb_revn) cbb_revn
                ,SUM(four_plus_placements_revn) four_plus_placements_revn
                ,SUM(automatic_placement_revn) automatic_placement_revn
                ,SUM(resilient_revn) resilient_revn
            from <TABLE:bpo_gms_solutions_ad_account_level_snapshot>


    where ds = '<DATEID>'

                group by
                 date_id
                ,id_dh_territory
                ,advertiser_name
                ,advertiser_country
                ,advertiser_vertical
                ,advertiser_sub_vertical
                ,gvm_vertical_name
                ,gvm_sub_vertical_name
                ,specialty
                ,planning_agency_name
                ,planning_agency_fbid
                ,planning_agency_sfid
                ,planning_agency_ult_fbid
                ,planning_agency_ult_sfid
                ,planning_agency_ult_name
                ,region
                ,sub_region
                ,advertiser_fbid
                ,advertiser_sfid
                ,ultimate_parent_fbid
                ,ultimate_parent_sfid
                ,ultimate_parent_name
                ,split
                ,client_partner
                ,cp_username
                ,cp_start_date
                ,cp_manager
                ,cp_manager_username
                ,account_manager
                ,am_username
                ,am_start_date
                ,am_manager
                ,am_manager_username
                ,partner_manager
                ,pm_username
                ,pm_start_date
                ,pm_manager
                ,pm_manager_username
                ,agency_partner
                ,ap_username
                ,ap_start_date
                ,ap_manager
                ,ap_manager_username
                ,reseller_fbid
                ,reseller_sfid
                ,reseller_name
                ,program
                ,id_d_customer_account_adv
                ,advertiser_coverage_model_daa
                ,advertiser_program_daa
                ,agency_coverage_model_daa
                ,is_gat
                ,is_gcm
                ,is_magic93
                ,l12_advertiser_territory
                ,l10_advertiser_territory
                ,l8_advertiser_territory
                ,l6_advertiser_territory
                ,l4_advertiser_territory
                ,l2_advertiser_territory
                ,l12_usern_advertiser_territory
                ,l10_usern_advertiser_territory
                ,l12_manager_advertiser_territory
                ,l10_manager_advertiser_territory
                ,l8_manager_advertiser_territory
                ,l12_agency_territory
                ,l10_agency_territory
                ,l8_agency_territory
                ,l6_agency_territory
                ,l4_agency_territory
                ,l2_agency_territory
                ,l12_usern_agency_territory
                ,l10_usern_agency_territory
                ,l12_manager_agency_territory
                ,l10_manager_agency_territory
                ,l8_manager_agency_territory
                ,gms_optimal_target
                ,gms_liquidity_target
                ,gso_optimal_target
                ,gso_liquidity_target
                ,smb_optimal_target
                ,smb_liquidity_target
                ,l4_optimal_target
                ,l4_liquidity_target
                ,l6_optimal_target
                ,l6_liquidity_target
                ,l8_optimal_target
                ,l8_liquidity_target
                ,l10_optimal_target
                ,l10_liquidity_target
                ,l12_optimal_target
                ,l12_liquidity_target
                ,l12_reporting_territory
                ,l10_reporting_territory
                ,l8_reporting_territory
                ,l6_reporting_territory
                ,l4_reporting_territory
                ,l2_reporting_territory
                ,segmentation
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,l6_manager_agency_territory
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,program_optimal_target
                ,program_liquidity_target
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,CAST(CURRENT_TIMESTAMP AS VARCHAR)
                ,revenue_segment
                 """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select


class BpoGmsSolutions:
    """@docstring a 1 ds table for loading a tableau extract"""

    def __init__(self):
        self.name = "<TABLE:bpo_gms_solutions>"
        self.create = """
                 CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_solutions> (
                date_id varchar,
                id_dh_territory bigint,
                advertiser_name varchar,
                advertiser_country varchar,
                advertiser_vertical varchar,
                advertiser_sub_vertical varchar,
                gvm_vertical_name varchar,
                gvm_sub_vertical_name varchar,
                specialty VARCHAR,
                legacy_advertiser_sub_vertical VARCHAR,
                legacy_advertiser_vertical VARCHAR,
                region varchar,
                sub_region varchar,
                advertiser_fbid varchar,
                advertiser_sfid varchar,
                ultimate_parent_fbid varchar,
                ultimate_parent_sfid varchar,
                ultimate_parent_name varchar,
                planning_agency_name varchar,
                planning_agency_fbid varchar,
                planning_agency_sfid varchar,
                planning_agency_ult_fbid varchar,
                planning_agency_ult_sfid varchar,
                planning_agency_ult_name varchar,
                split double,
                client_partner varchar,
                cp_username varchar,
                cp_start_date varchar,
                cp_manager varchar,
                cp_manager_username varchar,
                account_manager varchar,
                am_username varchar,
                am_start_date varchar,
                am_manager varchar,
                am_manager_username varchar,
                partner_manager varchar,
                pm_username varchar,
                pm_start_date varchar,
                pm_manager varchar,
                pm_manager_username varchar,
                agency_partner varchar,
                ap_username varchar,
                ap_start_date varchar,
                ap_manager varchar,
                ap_manager_username varchar,
                reseller_fbid varchar,
                reseller_sfid varchar,
                reseller_name varchar,
                program varchar,
                id_d_customer_account_adv bigint,
                advertiser_coverage_model_daa varchar,
                advertiser_program_daa varchar,
                agency_coverage_model_daa varchar,
                is_gat boolean,
                is_gcm boolean,
                is_magic93 boolean,
                l12_advertiser_territory varchar,
                l10_advertiser_territory varchar,
                l8_advertiser_territory varchar,
                l6_advertiser_territory varchar,
                l4_advertiser_territory varchar,
                l2_advertiser_territory varchar,
                l12_usern_advertiser_territory varchar,
                l10_usern_advertiser_territory varchar,
                l12_manager_advertiser_territory varchar,
                l10_manager_advertiser_territory varchar,
                l8_manager_advertiser_territory varchar,
                l12_agency_territory varchar,
                l10_agency_territory varchar,
                l8_agency_territory varchar,
                l6_agency_territory varchar,
                l4_agency_territory varchar,
                l2_agency_territory varchar,
                l12_usern_agency_territory varchar,
                l10_usern_agency_territory varchar,
                l12_manager_agency_territory varchar,
                l10_manager_agency_territory varchar,
                l8_manager_agency_territory varchar,
                gms_optimal_target double,
                gms_liquidity_target double,
                gso_optimal_target double,
                gso_liquidity_target double,
                smb_optimal_target double,
                smb_liquidity_target double,
                l4_optimal_target double,
                l4_liquidity_target double,
                l8_optimal_target double,
                l8_liquidity_target double,
                l10_optimal_target double,
                l10_liquidity_target double,
                l12_optimal_target double,
                l12_liquidity_target double,
                canvas_ads_revn double,
                fb_feed_opt_in_revn double,
                app_install_revn double,
                dynamic_ads_revn double,
                carousel_revn double,
                cpas_revn double,
                playable_ads_revn double,
                app_event_optimisation_revn double,
                reach_and_frequency_revn double,
                sdk_revn double,
                page_likes_revn double,
                vertical_video_revn double,
                slideshow_video_revn double,
                click_to_messenger_revn double,
                short_form_video_revn double,
                video_revn double,
                dro_conversion_optimization_revn double,
                dro_landing_page_views_revn double,
                dro_app_installs_revn double,
                dro_lead_generation_revn double,
                dro_dra_offline_conversions_revn double,
                dro_messenger_replies_revn double,
                dro_store_visits_revn double,
                dro_dra_roas_revn double,
                reach_optimized_revn double,
                video_views_optimized_revn double,
                bao_mfv_revn double,
                bao_nonvideo_revn double,
                videoviews_mfc_revn double,
                pa_messenger_opt_in_revn double,
                pa_instream_revn double,
                pa_audience_network_revn double,
                pa_instant_articles_revn double,
                pa_instagram_story_revn double,
                po_messenger_opt_in_revn double,
                po_instream_revn double,
                po_audience_network_revn double,
                po_instagram_revn double,
                po_instant_articles_revn double,
                po_instagram_story_revn double,
                instagram_opt_in_revn double,
                instagram_stories_opt_in_revn double,
                audience_network_opt_in_revn double,
                messenger_opt_in_revn double,
                home_instream_opt_in_revn double,
                mobile_instream_opt_in_revn double,
                fb_pixel_revn double,
                lead_ads_revn double,
                mobile_first_video_revn double,
                collection_revn double,
                instream_video_revn double,
                messenger_revn double,
                instagram_revn double,
                fb_stories_opt_in_revn double,
                click_to_whatsapp_revn double,
                revenue double,
                optimal double,
                liquidity double,
                ig_stories_revn	double,
                messenger_ads_revn	double,
                audience_network_revn	double,
                web_conversion_revn	double,
                website_clicks_revn	double,
                platform_messenger_revn	double,
                facebook_revn	double,
                sponsored_messages_revn	double,
                mobile_feed_impressions	double,
                ig_stories_impressions	double,
                ig_stream_impressions	double,
                messenger_impressions	double,
                mobile_feed_revn	double,
                web_feed_impressions	double,
                dr_revn	double,
                brand_revn double,
                l12_reporting_territory VARCHAR,
                l10_reporting_territory VARCHAR,
                l8_reporting_territory  VARCHAR,
                l6_reporting_territory  VARCHAR,
                l4_reporting_territory  VARCHAR,
                l2_reporting_territory  VARCHAR,
                segmentation VARCHAR,
                search_ads_revn DOUBLE COMMENT 'Recognized Search Ads Revenue',
                ig_feed_revn DOUBLE COMMENT 'Recognized Instagram Feed Revenue',
                marketplace_revn DOUBLE COMMENT 'Recognized MarketPlace Revenue',
                branded_content_revn DOUBLE COMMENT 'Recognized Branded Content Revenue',
                messenger_stories_revn DOUBLE COMMENT 'Recognized Messenger Stories Revenue',
                groups_revn DOUBLE COMMENT ' Recognized groups revenue ',
                fb_stories_revn DOUBLE COMMENT 'Recognized Revenue for FB stories Placement',
                l12_reporting_terr_mgr VARCHAR,
                l10_reporting_terr_mgr VARCHAR,
                l8_reporting_terr_mgr VARCHAR,
                l6_reporting_terr_mgr VARCHAR,
                l6_manager_agency_territory VARCHAR,
                pixel_issued double,
                pixel_total double,
                catalog_match double,
                catolog_total double,
                cbb_revn double,
                four_plus_placements_revn double,
                automatic_placement_revn double,
                rep_fbid_csm VARCHAR COMMENT 'FBID for Client Solutions Manager https://fburl.com/wut/cad62z1j',
                client_solutions_manager VARCHAR COMMENT 'full name for Client Solutions Manager',
                csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
                csm_start_date varchar COMMENT 'Start Date of CSM for a given ad account',
                csm_manager varchar COMMENT 'Full name for a CSMs manager',
                csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
                rep_fbid_asm VARCHAR COMMENT 'FBID for Agency Solutions Manager',
                agency_solutions_manager VARCHAR COMMENT 'Full name for ASM' ,
                asm_username varchar COMMENT 'UnixName for ASM',
                asm_start_date varchar COMMENT 'start date of an asm for a given ad account ',
                asm_manager varchar COMMENT 'Full name for an  ASMs manager',
                asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
                reseller_partner VARCHAR COMMENT 'Full name for reseller partner',
                rp_username varchar COMMENT 'UnixName for reseller partner',
                rp_start_date varchar COMMENT 'start date for a reseller partner for a given ad account',
                rp_manager varchar COMMENT 'Full name for an RPs manager',
                rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
                sales_adv_country_group VARCHAR,
                sales_adv_subregion VARCHAR,
                sales_adv_region VARCHAR,
                market VARCHAR,
                program_optimal_target double,
                program_liquidity_target double,
                l6_optimal_target double,
                l6_liquidity_target double,
                country_agc VARCHAR,
                market_agc VARCHAR,
                region_agc VARCHAR,
                sub_region_agc VARCHAR,
                business_type_adv VARCHAR,
                business_type_agc VARCHAR,
                planning_agency_operating_co VARCHAR,
                program_agency VARCHAR,
                china_export_advertiser VARCHAR,
                export_advertiser_country VARCHAR,
                billing_country_adv VARCHAR,
                billing_region_adv VARCHAR,
                billing_country_agc VARCHAR,
                billing_region_agc VARCHAR,
                hq_country_adv VARCHAR,
                hq_region_adv VARCHAR,
                hq_country_agc VARCHAR,
                hq_region_agc VARCHAR,
                resilient_revn DOUBLE,
                ts VARCHAR,
                revenue_segment VARCHAR,
                ds varchar
           )
                      WITH (
                            partitioned_by = ARRAY['ds'],
                            retention_days = <RETENTION:90>,
                            uii=false
                           )
                      """
        self.select = """
            SELECT
                date_id
                ,id_dh_territory
                ,advertiser_name
                ,advertiser_country
                ,advertiser_vertical
                ,advertiser_sub_vertical
                ,gvm_vertical_name
                ,gvm_sub_vertical_name
                ,specialty
                ,legacy_advertiser_sub_vertical
                ,legacy_advertiser_vertical
                ,region
                ,sub_region
                ,advertiser_fbid
                ,advertiser_sfid
                ,ultimate_parent_fbid
                ,ultimate_parent_sfid
                ,ultimate_parent_name
                ,planning_agency_name
                ,planning_agency_fbid
                ,planning_agency_sfid
                ,planning_agency_ult_fbid
                ,planning_agency_ult_sfid
                ,planning_agency_ult_name
                ,split
                ,client_partner
                ,cp_username
                ,cp_start_date
                ,cp_manager
                ,cp_manager_username
                ,account_manager
                ,am_username
                ,am_start_date
                ,am_manager
                ,am_manager_username
                ,partner_manager
                ,pm_username
                ,pm_start_date
                ,pm_manager
                ,pm_manager_username
                ,agency_partner
                ,ap_username
                ,ap_start_date
                ,ap_manager
                ,ap_manager_username
                ,reseller_fbid
                ,reseller_sfid
                ,reseller_name
                ,program
                ,id_d_customer_account_adv
                ,advertiser_coverage_model_daa
                ,advertiser_program_daa
                ,agency_coverage_model_daa
                ,is_gat
                ,is_gcm
                ,is_magic93
                ,l12_advertiser_territory
                ,l10_advertiser_territory
                ,l8_advertiser_territory
                ,l6_advertiser_territory
                ,l4_advertiser_territory
                ,l2_advertiser_territory
                ,l12_usern_advertiser_territory
                ,l10_usern_advertiser_territory
                ,l12_manager_advertiser_territory
                ,l10_manager_advertiser_territory
                ,l8_manager_advertiser_territory
                ,l12_agency_territory
                ,l10_agency_territory
                ,l8_agency_territory
                ,l6_agency_territory
                ,l4_agency_territory
                ,l2_agency_territory
                ,l12_usern_agency_territory
                ,l10_usern_agency_territory
                ,l12_manager_agency_territory
                ,l10_manager_agency_territory
                ,l8_manager_agency_territory
                ,gms_optimal_target
                ,gms_liquidity_target
                ,gso_optimal_target
                ,gso_liquidity_target
                ,smb_optimal_target
                ,smb_liquidity_target
                ,l4_optimal_target
                ,l4_liquidity_target
                ,l6_optimal_target
                ,l6_liquidity_target
                ,l8_optimal_target
                ,l8_liquidity_target
                ,l10_optimal_target
                ,l10_liquidity_target
                ,l12_optimal_target
                ,l12_liquidity_target
                ,l12_reporting_territory
                ,l10_reporting_territory
                ,l8_reporting_territory
                ,l6_reporting_territory
                ,l4_reporting_territory
                ,l2_reporting_territory
                ,segmentation
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,canvas_ads_revn
                ,fb_feed_opt_in_revn
                ,app_install_revn
                ,dynamic_ads_revn
                ,carousel_revn
                ,cpas_revn
                ,playable_ads_revn
                ,app_event_optimisation_revn
                ,reach_and_frequency_revn
                ,sdk_revn
                ,page_likes_revn
                ,vertical_video_revn
                ,slideshow_video_revn
                ,click_to_messenger_revn
                ,short_form_video_revn
                ,video_revn
                ,dro_conversion_optimization_revn
                ,dro_landing_page_views_revn
                ,dro_app_installs_revn
                ,dro_lead_generation_revn
                ,dro_dra_offline_conversions_revn
                ,dro_messenger_replies_revn
                ,dro_store_visits_revn
                ,dro_dra_roas_revn
                ,reach_optimized_revn
                ,video_views_optimized_revn
                ,bao_mfv_revn
                ,bao_nonvideo_revn
                ,videoviews_mfc_revn
                ,pa_messenger_opt_in_revn
                ,pa_instream_revn
                ,pa_audience_network_revn
                ,pa_instant_articles_revn
                ,pa_instagram_story_revn
                ,po_messenger_opt_in_revn
                ,po_instream_revn
                ,po_audience_network_revn
                ,po_instagram_revn
                ,po_instant_articles_revn
                ,po_instagram_story_revn
                ,instagram_opt_in_revn
                ,instagram_stories_opt_in_revn
                ,audience_network_opt_in_revn
                ,messenger_opt_in_revn
                ,home_instream_opt_in_revn
                ,mobile_instream_opt_in_revn
                ,fb_pixel_revn
                ,lead_ads_revn
                ,mobile_first_video_revn
                ,collection_revn
                ,instream_video_revn
                ,messenger_revn
                ,instagram_revn
                ,fb_stories_opt_in_revn
                ,click_to_whatsapp_revn
                ,revenue
                ,optimal
                ,liquidity
                ,ig_stories_revn
                ,messenger_ads_revn
                ,audience_network_revn
                ,web_conversion_revn
                ,website_clicks_revn
                ,platform_messenger_revn
                ,facebook_revn
                ,sponsored_messages_revn
                ,mobile_feed_impressions
                ,ig_stories_impressions
                ,ig_stream_impressions
                ,messenger_impressions
                ,mobile_feed_revn
                ,web_feed_impressions
                ,dr_revn
                ,brand_revn
                ,search_ads_revn
                ,ig_feed_revn
                ,marketplace_revn
                ,branded_content_revn
                ,messenger_stories_revn
                ,groups_revn
                ,fb_stories_revn
                ,pixel_issued
                ,pixel_total
                ,catalog_match
                ,catolog_total
                ,cbb_revn
                ,four_plus_placements_revn
                ,automatic_placement_revn
                ,l6_manager_agency_territory
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,program_optimal_target
                ,program_liquidity_target
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,resilient_revn
                ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
                ,revenue_segment
            from <TABLE:bpo_gms_solutions_snapshot>


            where ds = '<DATEID>'"""
        self.delete = """DELETE FROM <TABLE:bpo_gms_solutions>
            WHERE ds != '<DATEID>'
            """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select

    def get_delete(self):
        """returns a delete statement that will remove all partitions
        from a table that are not '<DATEID>'"""
        return self.delete


class BpoGmsSolutionsFast:
    def __init__(self):
        self.name = "<TABLE:bpo_gms_solutions_fast>"
        self.create = """
                CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_solutions_fast> (
               ultimate_parent_fbid varchar,
               ultimate_parent_sfid varchar, --to be removed
               ultimate_parent_name varchar,
               program varchar,
               advertiser_coverage_model_daa varchar, --to be removed
               advertiser_program_daa varchar,--to be removed
               advertiser_vertical varchar,
               specialty VARCHAR,
               legacy_advertiser_sub_vertical VARCHAR,
               legacy_advertiser_vertical VARCHAR,
               gms_optimal_target double,
               gms_liquidity_target double,
               gso_optimal_target double,
               gso_liquidity_target double,
               smb_optimal_target double,
               smb_liquidity_target double,
               l4_optimal_target double,
               l4_liquidity_target double,
               l8_optimal_target double,
               l8_liquidity_target double,
               l10_optimal_target double,
               l10_liquidity_target double,
               l12_optimal_target double,
               l12_liquidity_target double,
               cp_username varchar,
               cp_manager_username varchar,
               am_username varchar,
               am_manager_username varchar,
               pm_username varchar,
               pm_manager_username varchar,
               ap_username varchar,
               ap_manager_username varchar,
               l12_reporting_terr_mgr VARCHAR,
               l10_reporting_terr_mgr VARCHAR,
               l8_reporting_terr_mgr VARCHAR,
               l6_reporting_terr_mgr VARCHAR,
               asofdate varchar,
               canvas_ads_revn double,
               canvas_ads_revn_prior double,
               ly_qtd_canvas_ads_revn double,
               lq_qtd_canvas_ads_revn double,
               l7d_avg_canvas_ads_revn double,
               L7d_canvas_ads_revn double,
               L7d_canvas_ads_revn_prior double,
               fb_feed_opt_in_revn double,
               fb_feed_opt_in_revn_prior double,
               ly_qtd_fb_feed_opt_in_revn double,
               lq_qtd_fb_feed_opt_in_revn double,
               l7d_avg_fb_feed_opt_in_revn double,
               L7d_fb_feed_opt_in_revn double,
               L7d_fb_feed_opt_in_revn_prior double,
               app_install_revn double,
               app_install_revn_prior double,
               ly_qtd_app_install_revn double,
               lq_qtd_app_install_revn double,
               l7d_avg_app_install_revn double,
               L7d_app_install_revn double,
               L7d_app_install_revn_prior double,
               dynamic_ads_revn double,
               dynamic_ads_revn_prior double,
               ly_qtd_dynamic_ads_revn double,
               lq_qtd_dynamic_ads_revn double,
               l7d_avg_dynamic_ads_revn double,
               L7d_dynamic_ads_revn double,
               L7d_dynamic_ads_revn_prior double,
               carousel_revn double,
               carousel_revn_prior double,
               ly_qtd_carousel_revn double,
               lq_qtd_carousel_revn double,
               l7d_avg_carousel_revn double,
               L7d_carousel_revn double,
               L7d_carousel_revn_prior double,
               cpas_revn double,
               cpas_revn_prior double,
               ly_qtd_cpas_revn double,
               lq_qtd_cpas_revn double,
               l7d_avg_cpas_revn double,
               L7d_cpas_revn double,
               L7d_cpas_revn_prior double,
               playable_ads_revn double,
               playable_ads_revn_prior double,
               ly_qtd_playable_ads_revn double,
               lq_qtd_playable_ads_revn double,
               l7d_avg_playable_ads_revn double,
               L7d_playable_ads_revn double,
               L7d_playable_ads_revn_prior double,
               app_event_optimisation_revn double,
               app_event_optimisation_revn_prior double,
               ly_qtd_app_event_optimisation_revn double,
               lq_qtd_app_event_optimisation_revn double,
               l7d_avg_app_event_optimisation_revn double,
               L7d_app_event_optimisation_revn double,
               L7d_app_event_optimisation_revn_prior double,
               reach_and_frequency_revn double,
               reach_and_frequency_revn_prior double,
               ly_qtd_reach_and_frequency_revn double,
               lq_qtd_reach_and_frequency_revn double,
               l7d_avg_reach_and_frequency_revn double,
               L7d_reach_and_frequency_revn double,
               L7d_reach_and_frequency_revn_prior double,
               sdk_revn double,
               sdk_revn_prior double,
               ly_qtd_sdk_revn double,
               lq_qtd_sdk_revn double,
               l7d_avg_sdk_revn double,
               L7d_sdk_revn double,
               L7d_sdk_revn_prior double,
               page_likes_revn double,
               page_likes_revn_prior double,
               ly_qtd_page_likes_revn double,
               lq_qtd_page_likes_revn double,
               l7d_avg_page_likes_revn double,
               L7d_page_likes_revn double,
               L7d_page_likes_revn_prior double,
               vertical_video_revn double,
               vertical_video_revn_prior double,
               ly_qtd_vertical_video_revn double,
               lq_qtd_vertical_video_revn double,
               l7d_avg_vertical_video_revn double,
               L7d_vertical_video_revn double,
               L7d_vertical_video_revn_prior double,
               slideshow_video_revn double,
               slideshow_video_revn_prior double,
               ly_qtd_slideshow_video_revn double,
               lq_qtd_slideshow_video_revn double,
               l7d_avg_slideshow_video_revn double,
               L7d_slideshow_video_revn double,
               L7d_slideshow_video_revn_prior double,
               click_to_messenger_revn double,
               click_to_messenger_revn_prior double,
               ly_qtd_click_to_messenger_revn double,
               lq_qtd_click_to_messenger_revn double,
               l7d_avg_click_to_messenger_revn double,
               L7d_click_to_messenger_revn double,
               L7d_click_to_messenger_revn_prior double,
               short_form_video_revn double,
               short_form_video_revn_prior double,
               ly_qtd_short_form_video_revn double,
               lq_qtd_short_form_video_revn double,
               l7d_avg_short_form_video_revn double,
               L7d_short_form_video_revn double,
               L7d_short_form_video_revn_prior double,
               video_revn double,
               video_revn_prior double,
               ly_qtd_video_revn double,
               lq_qtd_video_revn double,
               l7d_avg_video_revn double,
               L7d_video_revn double,
               L7d_video_revn_prior double,
               reach_optimized_revn double,
               reach_optimized_revn_prior double,
               ly_qtd_reach_optimized_revn double,
               lq_qtd_reach_optimized_revn double,
               l7d_avg_reach_optimized_revn double,
               L7d_reach_optimized_revn double,
               L7d_reach_optimized_revn_prior double,
               video_views_optimized_revn double,
               video_views_optimized_revn_prior double,
               ly_qtd_video_views_optimized_revn double,
               lq_qtd_video_views_optimized_revn double,
               l7d_avg_video_views_optimized_revn double,
               L7d_video_views_optimized_revn double,
               L7d_video_views_optimized_revn_prior double,
               videoviews_mfc_revn double,
               videoviews_mfc_revn_prior double,
               ly_qtd_videoviews_mfc_revn double,
               lq_qtd_videoviews_mfc_revn double,
               l7d_avg_videoviews_mfc_revn double,
               L7d_videoviews_mfc_revn double,
               L7d_videoviews_mfc_revn_prior double,
               instagram_opt_in_revn double,
               instagram_opt_in_revn_prior double,
               ly_qtd_instagram_opt_in_revn double,
               lq_qtd_instagram_opt_in_revn double,
               l7d_avg_instagram_opt_in_revn double,
               L7d_instagram_opt_in_revn double,
               L7d_instagram_opt_in_revn_prior double,
               instagram_stories_opt_in_revn double,
               instagram_stories_opt_in_revn_prior double,
               ly_qtd_instagram_stories_opt_in_revn double,
               lq_qtd_instagram_stories_opt_in_revn double,
               l7d_avg_instagram_stories_opt_in_revn double,
               L7d_instagram_stories_opt_in_revn double,
               L7d_instagram_stories_opt_in_revn_prior double,
               audience_network_opt_in_revn double,
               audience_network_opt_in_revn_prior double,
               ly_qtd_audience_network_opt_in_revn double,
               lq_qtd_audience_network_opt_in_revn double,
               l7d_avg_audience_network_opt_in_revn double,
               L7d_audience_network_opt_in_revn double,
               L7d_audience_network_opt_in_revn_prior double,
               messenger_opt_in_revn double,
               messenger_opt_in_revn_prior double,
               ly_qtd_messenger_opt_in_revn double,
               lq_qtd_messenger_opt_in_revn double,
               l7d_avg_messenger_opt_in_revn double,
               L7d_messenger_opt_in_revn double,
               L7d_messenger_opt_in_revn_prior double,
               home_instream_opt_in_revn double,
               home_instream_opt_in_revn_prior double,
               ly_qtd_home_instream_opt_in_revn double,
               lq_qtd_home_instream_opt_in_revn double,
               l7d_avg_home_instream_opt_in_revn double,
               L7d_home_instream_opt_in_revn double,
               L7d_home_instream_opt_in_revn_prior double,
               fb_pixel_revn double,
               fb_pixel_revn_prior double,
               ly_qtd_fb_pixel_revn double,
               lq_qtd_fb_pixel_revn double,
               l7d_avg_fb_pixel_revn double,
               L7d_fb_pixel_revn double,
               L7d_fb_pixel_revn_prior double,
               lead_ads_revn double,
               lead_ads_revn_prior double,
               ly_qtd_lead_ads_revn double,
               lq_qtd_lead_ads_revn double,
               l7d_avg_lead_ads_revn double,
               L7d_lead_ads_revn double,
               L7d_lead_ads_revn_prior double,
               mobile_first_video_revn double,
               mobile_first_video_revn_prior double,
               ly_qtd_mobile_first_video_revn double,
               lq_qtd_mobile_first_video_revn double,
               l7d_avg_mobile_first_video_revn double,
               L7d_mobile_first_video_revn double,
               L7d_mobile_first_video_revn_prior double,
               collection_revn double,
               collection_revn_prior double,
               ly_qtd_collection_revn double,
               lq_qtd_collection_revn double,
               l7d_avg_collection_revn double,
               L7d_collection_revn double,
               L7d_collection_revn_prior double,
               instream_video_revn double,
               instream_video_revn_prior double,
               ly_qtd_instream_video_revn double,
               lq_qtd_instream_video_revn double,
               l7d_avg_instream_video_revn double,
               L7d_instream_video_revn double,
               L7d_instream_video_revn_prior double,
               messenger_revn double,
               messenger_revn_prior double,
               ly_qtd_messenger_revn double,
               lq_qtd_messenger_revn double,
               l7d_avg_messenger_revn double,
               L7d_messenger_revn double,
               L7d_messenger_revn_prior double,
               instagram_revn double,
               instagram_revn_prior double,
               ly_qtd_instagram_revn double,
               lq_qtd_instagram_revn double,
               l7d_avg_instagram_revn double,
               L7d_instagram_revn double,
               L7d_instagram_revn_prior double,
               fb_stories_opt_in_revn double,
               fb_stories_opt_in_revn_prior double,
               ly_qtd_fb_stories_opt_in_revn double,
               lq_qtd_fb_stories_opt_in_revn double,
               l7d_avg_fb_stories_opt_in_revn double,
               L7d_fb_stories_opt_in_revn double,
               L7d_fb_stories_opt_in_revn_prior double,
               click_to_whatsapp_revn double,
               click_to_whatsapp_revn_prior double,
               ly_qtd_click_to_whatsapp_revn double,
               lq_qtd_click_to_whatsapp_revn double,
               l7d_avg_click_to_whatsapp_revn double,
               L7d_click_to_whatsapp_revn double,
               L7d_click_to_whatsapp_revn_prior double,
               revenue double,
               revenue_prior double,
               ly_qtd_revenue double,
               lq_qtd_revenue double,
               l7d_avg_revenue double,
               L7d_revenue double,
               L7d_revenue_prior double,
               l28d_revenue double,
               l28d_revenue_prior double,
               optimal double,
               optimal_prior double,
               ly_qtd_optimal double,
               lq_qtd_optimal double,
               l7d_avg_optimal double,
               L7d_optimal double,
               L7d_optimal_prior double,
               l28d_optimal double,
               l28d_optimal_prior double,
               liquidity double,
               liquidity_prior double,
               ly_qtd_liquidity double,
               lq_qtd_liquidity double,
               l7d_avg_liquidity double,
               L7d_liquidity double,
               L7d_liquidity_prior double,
               l28d_liquidity double,
               l28d_liquidity_prior double,
               ig_stories_revn double,
               ig_stories_revn_prior double,
               ly_qtd_ig_stories_revn double,
               lq_qtd_ig_stories_revn double,
               l7d_avg_ig_stories_revn double,
               L7d_ig_stories_revn double,
               L7d_ig_stories_revn_prior double,
               messenger_ads_revn double,
               messenger_ads_revn_prior double,
               ly_qtd_messenger_ads_revn double,
               lq_qtd_messenger_ads_revn double,
               l7d_avg_messenger_ads_revn double,
               L7d_messenger_ads_revn double,
               L7d_messenger_ads_revn_prior double,
               audience_network_revn double,
               audience_network_revn_prior double,
               ly_qtd_audience_network_revn double,
               lq_qtd_audience_network_revn double,
               l7d_avg_audience_network_revn double,
               L7d_audience_network_revn double,
               L7d_audience_network_revn_prior double,
               web_conversion_revn double,
               web_conversion_revn_prior double,
               ly_qtd_web_conversion_revn double,
               lq_qtd_web_conversion_revn double,
               l7d_avg_web_conversion_revn double,
               L7d_web_conversion_revn double,
               L7d_web_conversion_revn_prior double,
               website_clicks_revn double,
               website_clicks_revn_prior double,
               ly_qtd_website_clicks_revn double,
               lq_qtd_website_clicks_revn double,
               l7d_avg_website_clicks_revn double,
               L7d_website_clicks_revn double,
               L7d_website_clicks_revn_prior double,
               platform_messenger_revn double,
               platform_messenger_revn_prior double,
               ly_qtd_platform_messenger_revn double,
               lq_qtd_platform_messenger_revn double,
               l7d_avg_platform_messenger_revn double,
               L7d_platform_messenger_revn double,
               L7d_platform_messenger_revn_prior double,
               facebook_revn double,
               facebook_revn_prior double,
               ly_qtd_facebook_revn double,
               lq_qtd_facebook_revn double,
               l7d_avg_facebook_revn double,
               L7d_facebook_revn double,
               L7d_facebook_revn_prior double,
               sponsored_messages_revn double,
               sponsored_messages_revn_prior double,
               ly_qtd_sponsored_messages_revn double,
               lq_qtd_sponsored_messages_revn double,
               l7d_avg_sponsored_messages_revn double,
               L7d_sponsored_messages_revn double,
               L7d_sponsored_messages_revn_prior double,
               mobile_feed_revn double,
               mobile_feed_revn_prior double,
               ly_qtd_mobile_feed_revn double,
               lq_qtd_mobile_feed_revn double,
               l7d_avg_mobile_feed_revn double,
               L7d_mobile_feed_revn double,
               L7d_mobile_feed_revn_prior double,
               dr_revn double,
               dr_revn_prior double,
               ly_qtd_dr_revn double,
               lq_qtd_dr_revn double,
               l7d_avg_dr_revn double,
               L7d_dr_revn double,
               L7d_dr_revn_prior double,
               brand_revn double,
               brand_revn_prior double,
               ly_qtd_brand_revn double,
               lq_qtd_brand_revn double,
               l7d_avg_brand_revn double,
               L7d_brand_revn double,
               L7d_brand_revn_prior double,
               wa_messages_spend double,
               wa_messages_spend_prior double,
               ly_qtd_wa_messages_spend double,
               lq_qtd_wa_messages_spend double,
               l7d_avg_wa_messages_spend double,
               L7d_wa_messages_spend double,
               L7d_wa_messages_spend_prior double,
               wa_message_volume double,
               wa_message_volume_prior double,
               ly_qtd_wa_message_volume double,
               lq_qtd_wa_message_volume double,
               l7d_avg_wa_message_volume double,
               L7d_wa_message_volume double,
               L7d_wa_message_volume_prior double,
               l12_reporting_territory VARCHAR,
               l10_reporting_territory VARCHAR,
               l8_reporting_territory  VARCHAR,
               l6_reporting_territory  VARCHAR,
               l4_reporting_territory  VARCHAR,
               l2_reporting_territory  VARCHAR,
               segmentation VARCHAR,
               search_ads_revn DOUBLE COMMENT 'Recognized Search Ads Revenue',
               search_ads_revn_prior DOUBLE COMMENT 'Search Ads revenue from previous Quarter',
               ly_qtd_search_ads_revn DOUBLE COMMENT 'Search Ads Revenue from QTD last year',
               lq_qtd_search_ads_revn DOUBLE COMMENT 'Search Ads Revenue from QTD last quarter',
               l7d_avg_search_ads_revn DOUBLE COMMENT 'last 7 days average Search Ads Revenue',
               L7d_search_ads_revn double,
               L7d_search_ads_revn_prior double,
               ig_feed_revn DOUBLE COMMENT 'Recognized Instagram Feed Revenue',
               ig_feed_revn_prior DOUBLE COMMENT 'Instagram Feed revenue from previous Quarter',
               ly_qtd_ig_feed_revn DOUBLE COMMENT 'Instagram Feed Revenue from QTD last year',
               lq_qtd_ig_feed_revn DOUBLE COMMENT 'Instagram Feed Revenue from QTD last quarter',
               l7d_avg_ig_feed_revn DOUBLE COMMENT 'last 7 days average Instagram Feed Revenue',
               L7d_ig_feed_revn double,
               L7d_ig_feed_revn_prior double,
               marketplace_revn DOUBLE COMMENT 'Recognized MarketPlace Revenue',
               marketplace_revn_prior DOUBLE COMMENT 'Marketplace revenue from previous Quarter',
               ly_qtd_marketplace_revn DOUBLE COMMENT 'Marketplace Revenue from QTD last year',
               lq_qtd_marketplace_revn DOUBLE COMMENT 'Marketplace Revenue from QTD last quarter',
               l7d_avg_marketplace_revn DOUBLE COMMENT 'last 7 days average Marketplace Revenue',
               L7d_marketplace_revn double,
               L7d_marketplace_revn_prior double,
               branded_content_revn DOUBLE COMMENT 'Recognized Branded Content Revenue',
               branded_content_revn_prior DOUBLE COMMENT 'Branded Content revenue from previous Quarter',
               ly_qtd_branded_content_revn DOUBLE COMMENT 'Branded Content Revenue from QTD last year',
               lq_qtd_branded_content_revn DOUBLE COMMENT 'Branded Content Revenue from QTD last quarter',
               l7d_avg_branded_content_revn DOUBLE COMMENT 'last 7 days average Branded Content Revenue',
               messenger_stories_revn DOUBLE COMMENT 'Recognized Messenger Stories Revenue',
               messenger_stories_revn_prior DOUBLE COMMENT 'Messenger Stories revenue from previous Quarter',
               ly_qtd_messenger_stories_revn DOUBLE COMMENT 'Messenger Stories Revenue from QTD last year',
               lq_qtd_messenger_stories_revn DOUBLE COMMENT 'Messenger Stories Revenue from QTD last quarter',
               l7d_avg_messenger_stories_revn DOUBLE COMMENT 'last 7 days average Messenger Stories Revenue',
               L7d_messenger_stories_revn double,
               L7d_messenger_stories_revn_prior double,
               groups_revn DOUBLE COMMENT ' Recognized groups revenue ',
               groups_revn_prior DOUBLE COMMENT 'Groups revenue from previous Quarter',
               ly_qtd_groups_revn DOUBLE COMMENT 'Groups Revenue from QTD last year',
               lq_qtd_groups_revn DOUBLE COMMENT 'Groups Revenue from QTD last quarter',
               l7d_avg_groups_revn DOUBLE COMMENT 'last 7 days average Groups Revenue',
               L7d_groups_revn double,
               L7d_groups_revn_prior double,
               fb_stories_revn DOUBLE COMMENT ' Recognized Delivered FB Stories revenue ',
               fb_stories_revn_prior DOUBLE COMMENT 'Delivered FB Stories revenue from previous Quarter',
               ly_qtd_fb_stories_revn DOUBLE COMMENT 'Delivered FB Stories Revenue from QTD last year',
               lq_qtd_fb_stories_revn DOUBLE COMMENT 'Delivered FB Stories Revenue from QTD last quarter',
               l7d_avg_fb_stories_revn DOUBLE COMMENT 'last 7 days average Delivered FB Stories Revenue',
               L7d_fb_stories_revn double,
               L7d_fb_stories_revn_prior double,
               l12_agency_territory VARCHAR,
               l10_agency_territory VARCHAR,
               l8_agency_territory VARCHAR,
               l6_agency_territory VARCHAR,
               l4_agency_territory VARCHAR,
               l2_agency_territory VARCHAR,
               l12_manager_agency_territory VARCHAR,
               l10_manager_agency_territory VARCHAR,
               l8_manager_agency_territory VARCHAR,
               l6_manager_agency_territory VARCHAR,
               csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
               csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
               asm_username varchar COMMENT 'UnixName for ASM',
               asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
               rp_username varchar COMMENT 'UnixName for reseller partner',
               rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
               sales_adv_country_group VARCHAR, --to be removed
               sales_adv_subregion VARCHAR,
               sales_adv_region VARCHAR,
               market VARCHAR,
               advertiser_country VARCHAR,
               program_optimal_target double,
               program_liquidity_target double,
               l6_optimal_target double,
               l6_liquidity_target double,
               resilient_revn DOUBLE COMMENT ' Recognized Delivered FB Stories revenue ',
               resilient_revn_prior DOUBLE COMMENT 'Delivered FB Stories revenue from previous Quarter',
               ly_qtd_resilient_revn DOUBLE COMMENT 'Delivered FB Stories Revenue from QTD last year',
               lq_qtd_resilient_revn DOUBLE COMMENT 'Delivered FB Stories Revenue from QTD last quarter',
               l7d_avg_resilient_revn DOUBLE COMMENT 'last 7 days average Delivered FB Stories Revenue',
               L7d_resilient_revn double,
               L7d_resilient_prior double,
               ts VARCHAR,
               revenue_segment VARCHAR,
               L14d_revenue DOUBLE,
               L14d_revenue_prior DOUBLE,
               L14d_avg_revenue DOUBLE,
               L14d_avg_revenue_prior DOUBLE,
               ds varchar
           )
                WITH (
                            partitioned_by = ARRAY['ds'],
                            retention_days = <RETENTION:90>,
                            uii=false
                           )"""
        self.select = """
            SELECT
                 ultimate_parent_fbid
                ,ultimate_parent_sfid
                ,ultimate_parent_name
                ,program
                ,advertiser_coverage_model_daa
                ,advertiser_program_daa
                ,advertiser_vertical
                ,specialty
                ,legacy_advertiser_sub_vertical
                ,legacy_advertiser_vertical
                ,gms_optimal_target
                ,gms_liquidity_target
                ,gso_optimal_target
                ,gso_liquidity_target
                ,smb_optimal_target
                ,smb_liquidity_target
                ,l4_optimal_target
                ,l4_liquidity_target
                ,l6_optimal_target
                ,l6_liquidity_target
                ,l8_optimal_target
                ,l8_liquidity_target
                ,l10_optimal_target
                ,l10_liquidity_target
                ,l12_optimal_target
                ,l12_liquidity_target
                ,cp_username
                ,cp_manager_username
                ,am_username
                ,am_manager_username
                ,pm_username
                ,pm_manager_username
                ,ap_username
                ,ap_manager_username
                ,l12_reporting_territory
                ,l10_reporting_territory
                ,l8_reporting_territory
                ,l6_reporting_territory
                ,l4_reporting_territory
                ,l2_reporting_territory
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,l12_agency_territory
                ,l10_agency_territory
                ,l8_agency_territory
                ,l6_agency_territory
                ,l4_agency_territory
                ,l2_agency_territory
                ,l12_manager_agency_territory
                ,l10_manager_agency_territory
                ,l8_manager_agency_territory
                ,l6_manager_agency_territory
                ,cast(asofdate as varchar) asofdate
                ,segmentation
                ,csm_username
                ,csm_manager_username
                ,asm_username
                ,asm_manager_username
                ,rp_username
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,advertiser_country
                ,planning_agency_name
                ,planning_agency_fbid
                ,planning_agency_sfid
                ,planning_agency_ult_fbid
                ,planning_agency_ult_sfid
                ,planning_agency_ult_name
               ,program_optimal_target
               ,program_liquidity_target
               ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
               ,revenue_segment

                --canvas_ads_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then canvas_ads_revn else null end) canvas_ads_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then canvas_ads_revn else null end) canvas_ads_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then canvas_ads_revn else null end) ly_qtd_canvas_ads_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then canvas_ads_revn else null end) lq_qtd_canvas_ads_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then canvas_ads_revn else null end) L7d_avg_canvas_ads_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then canvas_ads_revn else null end) L7d_canvas_ads_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then canvas_ads_revn else null end) L7d_canvas_ads_revn_prior

                --fb_feed_opt_in_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then fb_feed_opt_in_revn else null end) fb_feed_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then fb_feed_opt_in_revn else null end) fb_feed_opt_in_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then fb_feed_opt_in_revn else null end) ly_qtd_fb_feed_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then fb_feed_opt_in_revn else null end) lq_qtd_fb_feed_opt_in_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then fb_feed_opt_in_revn else null end) L7d_avg_fb_feed_opt_in_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then fb_feed_opt_in_revn else null end) L7d_fb_feed_opt_in_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then fb_feed_opt_in_revn else null end) L7d_fb_feed_opt_in_revn_prior

                --app_install_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then app_install_revn else null end) app_install_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then app_install_revn else null end) app_install_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then app_install_revn else null end) ly_qtd_app_install_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then app_install_revn else null end) lq_qtd_app_install_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then app_install_revn else null end) L7d_avg_app_install_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then app_install_revn else null end) L7d_app_install_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then app_install_revn else null end) L7d_app_install_revn_prior

                --dynamic_ads_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then dynamic_ads_revn else null end) dynamic_ads_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then dynamic_ads_revn else null end) dynamic_ads_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then dynamic_ads_revn else null end) ly_qtd_dynamic_ads_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then dynamic_ads_revn else null end) lq_qtd_dynamic_ads_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then dynamic_ads_revn else null end) L7d_avg_dynamic_ads_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then dynamic_ads_revn else null end) L7d_dynamic_ads_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then dynamic_ads_revn else null end) L7d_dynamic_ads_revn_prior

                --carousel_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then carousel_revn else null end) carousel_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then carousel_revn else null end) carousel_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then carousel_revn else null end) ly_qtd_carousel_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then carousel_revn else null end) lq_qtd_carousel_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then carousel_revn else null end) L7d_avg_carousel_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then carousel_revn else null end) L7d_carousel_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then carousel_revn else null end) L7d_carousel_revn_prior

                --cpas_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then cpas_revn else null end) cpas_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then cpas_revn else null end) cpas_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then cpas_revn else null end) ly_qtd_cpas_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then cpas_revn else null end) lq_qtd_cpas_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then cpas_revn else null end) L7d_avg_cpas_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then cpas_revn else null end) L7d_cpas_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then cpas_revn else null end) L7d_cpas_revn_prior

                --playable_ads_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then playable_ads_revn else null end) playable_ads_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then playable_ads_revn else null end) playable_ads_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then playable_ads_revn else null end) ly_qtd_playable_ads_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then playable_ads_revn else null end) lq_qtd_playable_ads_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then playable_ads_revn else null end) L7d_avg_playable_ads_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then playable_ads_revn else null end) L7d_playable_ads_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then playable_ads_revn else null end) L7d_playable_ads_revn_prior

                --app_event_optimisation_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then app_event_optimisation_revn else null end) app_event_optimisation_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then app_event_optimisation_revn else null end) app_event_optimisation_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then app_event_optimisation_revn else null end) ly_qtd_app_event_optimisation_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then app_event_optimisation_revn else null end) lq_qtd_app_event_optimisation_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then app_event_optimisation_revn else null end) L7d_avg_app_event_optimisation_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then app_event_optimisation_revn else null end) L7d_app_event_optimisation_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then app_event_optimisation_revn else null end) L7d_app_event_optimisation_revn_prior

                --reach_and_frequency_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then reach_and_frequency_revn else null end) reach_and_frequency_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then reach_and_frequency_revn else null end) reach_and_frequency_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then reach_and_frequency_revn else null end) ly_qtd_reach_and_frequency_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then reach_and_frequency_revn else null end) lq_qtd_reach_and_frequency_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then reach_and_frequency_revn else null end) L7d_avg_reach_and_frequency_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then reach_and_frequency_revn else null end) L7d_reach_and_frequency_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then reach_and_frequency_revn else null end) L7d_reach_and_frequency_revn_prior

                --sdk_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then sdk_revn else null end) sdk_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then sdk_revn else null end) sdk_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then sdk_revn else null end) ly_qtd_sdk_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then sdk_revn else null end) lq_qtd_sdk_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then sdk_revn else null end) L7d_avg_sdk_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then sdk_revn else null end) L7d_sdk_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                         cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then sdk_revn else null end) L7d_sdk_revn_prior

                --page_likes_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then page_likes_revn else null end) page_likes_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then page_likes_revn else null end) page_likes_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then page_likes_revn else null end) ly_qtd_page_likes_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then page_likes_revn else null end) lq_qtd_page_likes_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then page_likes_revn else null end) L7d_avg_page_likes_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then page_likes_revn else null end) L7d_page_likes_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                             cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                        then page_likes_revn else null end) L7d_page_likes_revn_prior

                --vertical_video_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then vertical_video_revn else null end) vertical_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then vertical_video_revn else null end) vertical_video_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then vertical_video_revn else null end) ly_qtd_vertical_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then vertical_video_revn else null end) lq_qtd_vertical_video_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then vertical_video_revn else null end) L7d_avg_vertical_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then vertical_video_revn else null end) L7d_vertical_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then vertical_video_revn else null end) L7d_vertical_video_revn_prior

                --slideshow_video_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then slideshow_video_revn else null end) slideshow_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then slideshow_video_revn else null end) slideshow_video_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then slideshow_video_revn else null end) ly_qtd_slideshow_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then slideshow_video_revn else null end) lq_qtd_slideshow_video_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then slideshow_video_revn else null end) L7d_avg_slideshow_video_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                          then slideshow_video_revn else null end) L7d_slideshow_video_revn
                ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                               cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                          then slideshow_video_revn else null end) L7d_slideshow_video_revn_prior

                --click_to_messenger_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then click_to_messenger_revn else null end) click_to_messenger_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then click_to_messenger_revn else null end) click_to_messenger_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then click_to_messenger_revn else null end) ly_qtd_click_to_messenger_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then click_to_messenger_revn else null end) lq_qtd_click_to_messenger_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then click_to_messenger_revn else null end) L7d_avg_click_to_messenger_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then click_to_messenger_revn else null end) L7d_click_to_messenger_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then click_to_messenger_revn else null end) L7d_click_to_messenger_revn_prior

                --short_form_video_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then short_form_video_revn else null end) short_form_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then short_form_video_revn else null end) short_form_video_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then short_form_video_revn else null end) ly_qtd_short_form_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then short_form_video_revn else null end) lq_qtd_short_form_video_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then short_form_video_revn else null end) L7d_avg_short_form_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then short_form_video_revn else null end) L7d_short_form_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then short_form_video_revn else null end) L7d_short_form_video_revn_prior

                --video_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then video_revn else null end) video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then video_revn else null end) video_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then video_revn else null end) ly_qtd_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then video_revn else null end) lq_qtd_video_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then video_revn else null end) L7d_avg_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then video_revn else null end) L7d_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then video_revn else null end) L7d_video_revn_prior

                --reach_optimized_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then reach_optimized_revn else null end) reach_optimized_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then reach_optimized_revn else null end) reach_optimized_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then reach_optimized_revn else null end) ly_qtd_reach_optimized_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then reach_optimized_revn else null end) lq_qtd_reach_optimized_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then reach_optimized_revn else null end) L7d_avg_reach_optimized_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then reach_optimized_revn else null end) L7d_reach_optimized_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then reach_optimized_revn else null end) L7d_reach_optimized_revn_prior

                --video_views_optimized_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then video_views_optimized_revn else null end) video_views_optimized_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then video_views_optimized_revn else null end) video_views_optimized_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then video_views_optimized_revn else null end) ly_qtd_video_views_optimized_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then video_views_optimized_revn else null end) lq_qtd_video_views_optimized_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then video_views_optimized_revn else null end) L7d_avg_video_views_optimized_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then video_views_optimized_revn else null end) L7d_video_views_optimized_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then video_views_optimized_revn else null end) L7d_video_views_optimized_revn_prior

                --videoviews_mfc_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then videoviews_mfc_revn else null end) videoviews_mfc_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then videoviews_mfc_revn else null end) videoviews_mfc_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then videoviews_mfc_revn else null end) ly_qtd_videoviews_mfc_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then videoviews_mfc_revn else null end) lq_qtd_videoviews_mfc_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then videoviews_mfc_revn else null end) L7d_avg_videoviews_mfc_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then videoviews_mfc_revn else null end) L7d_videoviews_mfc_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then videoviews_mfc_revn else null end) L7d_videoviews_mfc_revn_prior

                --instagram_opt_in_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then instagram_opt_in_revn else null end) instagram_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then instagram_opt_in_revn else null end) instagram_opt_in_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then instagram_opt_in_revn else null end) ly_qtd_instagram_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then instagram_opt_in_revn else null end) lq_qtd_instagram_opt_in_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then instagram_opt_in_revn else null end) L7d_avg_instagram_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then instagram_opt_in_revn else null end) L7d_instagram_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then instagram_opt_in_revn else null end) L7d_instagram_opt_in_revn_prior

                --instagram_stories_opt_in_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then instagram_stories_opt_in_revn else null end) instagram_stories_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then instagram_stories_opt_in_revn else null end) instagram_stories_opt_in_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then instagram_stories_opt_in_revn else null end) ly_qtd_instagram_stories_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then instagram_stories_opt_in_revn else null end) lq_qtd_instagram_stories_opt_in_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then instagram_stories_opt_in_revn else null end) L7d_avg_instagram_stories_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then instagram_stories_opt_in_revn else null end) L7d_instagram_stories_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then instagram_stories_opt_in_revn else null end) L7d_instagram_stories_opt_in_revn_prior

                --audience_network_opt_in_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then audience_network_opt_in_revn else null end) audience_network_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then audience_network_opt_in_revn else null end) audience_network_opt_in_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then audience_network_opt_in_revn else null end) ly_qtd_audience_network_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then audience_network_opt_in_revn else null end) lq_qtd_audience_network_opt_in_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then audience_network_opt_in_revn else null end) L7d_avg_audience_network_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then audience_network_opt_in_revn else null end) L7d_audience_network_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then audience_network_opt_in_revn else null end) L7d_audience_network_opt_in_revn_prior

                --messenger_opt_in_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then messenger_opt_in_revn else null end) messenger_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then messenger_opt_in_revn else null end) messenger_opt_in_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then messenger_opt_in_revn else null end) ly_qtd_messenger_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then messenger_opt_in_revn else null end) lq_qtd_messenger_opt_in_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then messenger_opt_in_revn else null end) L7d_avg_messenger_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then messenger_opt_in_revn else null end) L7d_messenger_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then messenger_opt_in_revn else null end) L7d_messenger_opt_in_revn_prior

                --home_instream_opt_in_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then home_instream_opt_in_revn else null end) home_instream_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then home_instream_opt_in_revn else null end) home_instream_opt_in_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then home_instream_opt_in_revn else null end) ly_qtd_home_instream_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then home_instream_opt_in_revn else null end) lq_qtd_home_instream_opt_in_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then home_instream_opt_in_revn else null end) L7d_avg_home_instream_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then home_instream_opt_in_revn else null end) L7d_home_instream_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then home_instream_opt_in_revn else null end) L7d_home_instream_opt_in_revn_prior

                --fb_pixel_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then fb_pixel_revn else null end) fb_pixel_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then fb_pixel_revn else null end) fb_pixel_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then fb_pixel_revn else null end) ly_qtd_fb_pixel_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then fb_pixel_revn else null end) lq_qtd_fb_pixel_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then fb_pixel_revn else null end) L7d_avg_fb_pixel_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then fb_pixel_revn else null end) L7d_fb_pixel_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then fb_pixel_revn else null end) L7d_fb_pixel_revn_prior

                --lead_ads_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then lead_ads_revn else null end) lead_ads_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then lead_ads_revn else null end) lead_ads_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then lead_ads_revn else null end) ly_qtd_lead_ads_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then lead_ads_revn else null end) lq_qtd_lead_ads_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then lead_ads_revn else null end) L7d_avg_lead_ads_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then lead_ads_revn else null end) L7d_lead_ads_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then lead_ads_revn else null end) L7d_lead_ads_revn_prior

                --mobile_first_video_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then mobile_first_video_revn else null end) mobile_first_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then mobile_first_video_revn else null end) mobile_first_video_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then mobile_first_video_revn else null end) ly_qtd_mobile_first_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then mobile_first_video_revn else null end) lq_qtd_mobile_first_video_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then mobile_first_video_revn else null end) L7d_avg_mobile_first_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then mobile_first_video_revn else null end) L7d_mobile_first_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then mobile_first_video_revn else null end) L7d_mobile_first_video_revn_prior

                --collection_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then collection_revn else null end) collection_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then collection_revn else null end) collection_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then collection_revn else null end) ly_qtd_collection_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then collection_revn else null end) lq_qtd_collection_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then collection_revn else null end) L7d_avg_collection_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then collection_revn else null end) L7d_collection_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then collection_revn else null end) L7d_collection_revn_prior

                --instream_video_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then instream_video_revn else null end) instream_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then instream_video_revn else null end) instream_video_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then instream_video_revn else null end) ly_qtd_instream_video_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then instream_video_revn else null end) lq_qtd_instream_video_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then instream_video_revn else null end) L7d_avg_instream_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then instream_video_revn else null end) L7d_instream_video_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then instream_video_revn else null end) L7d_instream_video_revn_prior

                --messenger_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then messenger_revn else null end) messenger_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then messenger_revn else null end) messenger_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then messenger_revn else null end) ly_qtd_messenger_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then messenger_revn else null end) lq_qtd_messenger_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then messenger_revn else null end) L7d_avg_messenger_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then messenger_revn else null end) L7d_messenger_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then messenger_revn else null end) L7d_messenger_revn_prior

                --instagram_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then instagram_revn else null end) instagram_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then instagram_revn else null end) instagram_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then instagram_revn else null end) ly_qtd_instagram_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then instagram_revn else null end) lq_qtd_instagram_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then instagram_revn else null end) L7d_avg_instagram_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then instagram_revn else null end) L7d_instagram_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then instagram_revn else null end) L7d_instagram_revn_prior

                --fb_stories_opt_in_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then fb_stories_opt_in_revn else null end) fb_stories_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then fb_stories_opt_in_revn else null end) fb_stories_opt_in_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then fb_stories_opt_in_revn else null end) ly_qtd_fb_stories_opt_in_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then fb_stories_opt_in_revn else null end) lq_qtd_fb_stories_opt_in_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then fb_stories_opt_in_revn else null end) L7d_avg_fb_stories_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then fb_stories_opt_in_revn else null end) L7d_fb_stories_opt_in_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then fb_stories_opt_in_revn else null end) L7d_fb_stories_opt_in_revn_prior

                --click_to_whatsapp_revn
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then click_to_whatsapp_revn else null end) click_to_whatsapp_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then click_to_whatsapp_revn else null end) click_to_whatsapp_revn_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then click_to_whatsapp_revn else null end) ly_qtd_click_to_whatsapp_revn
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then click_to_whatsapp_revn else null end) lq_qtd_click_to_whatsapp_revn
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then click_to_whatsapp_revn else null end) L7d_avg_click_to_whatsapp_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then click_to_whatsapp_revn else null end) L7d_click_to_whatsapp_revn
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then click_to_whatsapp_revn else null end) L7d_click_to_whatsapp_revn_prior

                --revenue
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then revenue else null end) revenue
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then revenue else null end) revenue_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then revenue else null end) ly_qtd_revenue
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then revenue else null end) lq_qtd_revenue
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then revenue else null end) L7d_avg_revenue
                ,Sum(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY)
                and cast(date_id as date) <= (asofdate)
                then revenue else null end) L28d_revenue
                ,Sum(case when cast(date_id as date) >= (asofdate - INTERVAL '34' DAY)
                and cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                then revenue else null end) L28d_revenue_prior
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then revenue else null end) L7d_revenue
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then revenue else null end) L7d_revenue_prior

                --optimal
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then optimal else null end) optimal
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then optimal else null end) optimal_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then optimal else null end) ly_qtd_optimal
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then optimal else null end) lq_qtd_optimal
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then optimal else null end) L7d_avg_optimal
                ,Sum(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY)
                and cast(date_id as date) <= (asofdate)
                then optimal else null end) L28d_optimal
                ,Sum(case when cast(date_id as date) >= (asofdate - INTERVAL '34' DAY)
                and cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                then optimal else null end) L28d_optimal_prior
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                        then optimal else null end) L7d_optimal
                        ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                            then optimal else null end) L7d_optimal_prior

                --liquidity
                ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                >= '<quarter_id>' then liquidity else null end) liquidity
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                then liquidity else null end) liquidity_prior
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                then liquidity else null end) ly_qtd_liquidity
                ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                then liquidity else null end) lq_qtd_liquidity
                ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                then liquidity else null end) L7d_avg_liquidity
                    ,Sum(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY)
                                        and cast(date_id as date) <= (asofdate)
                    then liquidity else null end) L28d_liquidity
                    ,Sum(case when cast(date_id as date) >= (asofdate - INTERVAL '34' DAY)
                    and cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    then liquidity else null end) L28d_liquidity_prior
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then liquidity else null end) L7d_liquidity
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then liquidity else null end) L7d_liquidity_prior

                --ig_stories_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then ig_stories_revn else null end) ig_stories_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then ig_stories_revn else null end) ig_stories_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then ig_stories_revn else null end) ly_qtd_ig_stories_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then ig_stories_revn else null end) lq_qtd_ig_stories_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then ig_stories_revn else null end) L7d_avg_ig_stories_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then ig_stories_revn else null end) L7d_ig_stories_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then ig_stories_revn else null end) L7d_ig_stories_revn_prior

                --messenger_ads_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then messenger_ads_revn else null end) messenger_ads_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then messenger_ads_revn else null end) messenger_ads_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then messenger_ads_revn else null end) ly_qtd_messenger_ads_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then messenger_ads_revn else null end) lq_qtd_messenger_ads_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then messenger_ads_revn else null end) L7d_avg_messenger_ads_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then messenger_ads_revn else null end) L7d_messenger_ads_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then messenger_ads_revn else null end) L7d_messenger_ads_revn_prior

                --audience_network_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then audience_network_revn else null end) audience_network_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then audience_network_revn else null end) audience_network_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then audience_network_revn else null end) ly_qtd_audience_network_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then audience_network_revn else null end) lq_qtd_audience_network_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then audience_network_revn else null end) L7d_avg_audience_network_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then audience_network_revn else null end) L7d_audience_network_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                             cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                        then audience_network_revn else null end) L7d_audience_network_revn_prior

                --web_conversion_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then web_conversion_revn else null end) web_conversion_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then web_conversion_revn else null end) web_conversion_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then web_conversion_revn else null end) ly_qtd_web_conversion_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then web_conversion_revn else null end) lq_qtd_web_conversion_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then web_conversion_revn else null end) L7d_avg_web_conversion_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then web_conversion_revn else null end) L7d_web_conversion_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then web_conversion_revn else null end) L7d_web_conversion_revn_prior

                --website_clicks_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then website_clicks_revn else null end) website_clicks_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then website_clicks_revn else null end) website_clicks_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then website_clicks_revn else null end) ly_qtd_website_clicks_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then website_clicks_revn else null end) lq_qtd_website_clicks_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then website_clicks_revn else null end) L7d_avg_website_clicks_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then website_clicks_revn else null end) L7d_website_clicks_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then website_clicks_revn else null end) L7d_website_clicks_revn_prior

                --platform_messenger_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then platform_messenger_revn else null end) platform_messenger_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then platform_messenger_revn else null end) platform_messenger_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then platform_messenger_revn else null end) ly_qtd_platform_messenger_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then platform_messenger_revn else null end) lq_qtd_platform_messenger_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then platform_messenger_revn else null end) L7d_avg_platform_messenger_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then platform_messenger_revn else null end) L7d_platform_messenger_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then platform_messenger_revn else null end) L7d_platform_messenger_revn_prior

                --facebook_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then facebook_revn else null end) facebook_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then facebook_revn else null end) facebook_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then facebook_revn else null end) ly_qtd_facebook_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then facebook_revn else null end) lq_qtd_facebook_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then facebook_revn else null end) L7d_avg_facebook_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then facebook_revn else null end) L7d_facebook_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then facebook_revn else null end) L7d_facebook_revn_prior

                --sponsored_messages_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then sponsored_messages_revn else null end) sponsored_messages_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then sponsored_messages_revn else null end) sponsored_messages_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then sponsored_messages_revn else null end) ly_qtd_sponsored_messages_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then sponsored_messages_revn else null end) lq_qtd_sponsored_messages_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then sponsored_messages_revn else null end) L7d_avg_sponsored_messages_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then sponsored_messages_revn else null end) L7d_sponsored_messages_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then sponsored_messages_revn else null end) L7d_sponsored_messages_revn_prior

                --mobile_feed_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then mobile_feed_revn else null end) mobile_feed_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then mobile_feed_revn else null end) mobile_feed_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then mobile_feed_revn else null end) ly_qtd_mobile_feed_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then mobile_feed_revn else null end) lq_qtd_mobile_feed_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then mobile_feed_revn else null end) L7d_avg_mobile_feed_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then mobile_feed_revn else null end) L7d_mobile_feed_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then mobile_feed_revn else null end) L7d_mobile_feed_revn_prior

                --dr_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then dr_revn else null end) dr_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then dr_revn else null end) dr_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then dr_revn else null end) ly_qtd_dr_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then dr_revn else null end) lq_qtd_dr_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then dr_revn else null end) L7d_avg_dr_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then dr_revn else null end) L7d_dr_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then dr_revn else null end) L7d_dr_revn_prior

                --brand_revn
                    ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                    >= '<quarter_id>' then brand_revn else null end) brand_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                    and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                    then brand_revn else null end) brand_revn_prior
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                    then brand_revn else null end) ly_qtd_brand_revn
                    ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                    and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                    then brand_revn else null end) lq_qtd_brand_revn
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then brand_revn else null end) L7d_avg_brand_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                            then brand_revn else null end) L7d_brand_revn
                            ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                     cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                                then brand_revn else null end) L7d_brand_revn_prior

                --whatsapp_notifications_revn
                   ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                   >= '<quarter_id>' then wa_messages_spend else null end) wa_messages_spend
                   ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                   and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                   then wa_messages_spend else null end) wa_messages_spend_prior
                   ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                   then wa_messages_spend else null end) ly_qtd_wa_messages_spend
                   ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                   and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                   then wa_messages_spend else null end) lq_qtd_wa_messages_spend
                   ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                   then wa_messages_spend else null end) L7d_avg_wa_messages_spend
                           ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                           then wa_messages_spend else null end) L7d_wa_messages_spend
                           ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                               then wa_messages_spend else null end) L7d_wa_messages_spend_prior

                --whatsapp_notifications_volume
                   ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                   >= '<quarter_id>' then wa_message_volume else null end) wa_message_volume
                   ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                   and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                   then wa_message_volume else null end) wa_message_volume_prior
                   ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                   then wa_message_volume else null end) ly_qtd_wa_message_volume
                   ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                   and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                   then wa_message_volume else null end) lq_qtd_wa_message_volume
                   ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                   then wa_message_volume else null end) L7d_avg_wa_message_volume
                           ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                           then wa_message_volume else null end) L7d_wa_message_volume
                           ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                               then wa_message_volume else null end) L7d_wa_message_volume_prior


                 --search_ads_revn
                   ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                   >= '<quarter_id>' then search_ads_revn else null end) search_ads_revn
                   ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                   and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                   then search_ads_revn else null end) search_ads_revn_prior
                   ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                   then search_ads_revn else null end) ly_qtd_search_ads_revn
                   ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                   and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                   then search_ads_revn else null end) lq_qtd_search_ads_revn
                   ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                   then search_ads_revn else null end) L7d_avg_search_ads_revn
                           ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                           then search_ads_revn else null end) L7d_search_ads_revn
                           ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                    cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                               then search_ads_revn else null end) L7d_search_ads_revn_prior

                 --ig_feed_revn
                  ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                  >= '<quarter_id>' then ig_feed_revn else null end) ig_feed_revn
                  ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                  and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                  then ig_feed_revn else null end) ig_feed_revn_prior
                  ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                  then ig_feed_revn else null end) ly_qtd_ig_feed_revn
                  ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                  and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                  then ig_feed_revn else null end) lq_qtd_ig_feed_revn
                  ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                  then ig_feed_revn else null end) L7d_avg_ig_feed_revn
                          ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                          then ig_feed_revn else null end) L7d_ig_feed_revn
                          ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                   cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                              then ig_feed_revn else null end) L7d_ig_feed_revn_prior

                 --marketplace_revn
                 ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                 >= '<quarter_id>' then marketplace_revn else null end) marketplace_revn
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                 and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                 then marketplace_revn else null end) marketplace_revn_prior
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                 then marketplace_revn else null end) ly_qtd_marketplace_revn
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                 and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                 then marketplace_revn else null end) lq_qtd_marketplace_revn
                 ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                 then marketplace_revn else null end) L7d_avg_marketplace_revn
                         ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                         then marketplace_revn else null end) L7d_marketplace_revn
                         ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                  cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                             then marketplace_revn else null end) L7d_marketplace_revn_prior

                 --branded_content_revn
                 ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                 >= '<quarter_id>' then branded_content_revn else null end) branded_content_revn
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                 and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                 then branded_content_revn else null end) branded_content_revn_prior
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                 then branded_content_revn else null end) ly_qtd_branded_content_revn
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                 and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                 then branded_content_revn else null end) lq_qtd_branded_content_revn
                 ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                 then branded_content_revn else null end) L7d_avg_branded_content_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then branded_content_revn else null end) L7d_branded_content_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                             cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                        then branded_content_revn else null end) L7d_branded_content_revn_prior

                 --messenger_stories_revn
                 ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                 >= '<quarter_id>' then messenger_stories_revn else null end) messenger_stories_revn
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                 and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                 then messenger_stories_revn else null end) messenger_stories_revn_prior
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                 then messenger_stories_revn else null end) ly_qtd_messenger_stories_revn
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                 and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                 then messenger_stories_revn else null end) lq_qtd_messenger_stories_revn
                 ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                 then messenger_stories_revn else null end) L7d_avg_messenger_stories_revn
                         ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                         then messenger_stories_revn else null end) L7d_messenger_stories_revn
                         ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                                  cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                             then messenger_stories_revn else null end) L7d_messenger_stories_revn_prior

                 --groups_revn
                 ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                 >= '<quarter_id>' then groups_revn else null end) groups_revn
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                 and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                 then groups_revn else null end) groups_revn_prior
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                 then groups_revn else null end) ly_qtd_groups_revn
                 ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                 and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                 then groups_revn else null end) lq_qtd_groups_revn
                 ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                 then groups_revn else null end) L7d_avg_groups_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then groups_revn else null end) L7d_groups_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                             cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                        then groups_revn else null end) L7d_groups_revn_prior

                  --fb_stories_revn
                  ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                  >= '<quarter_id>' then fb_stories_revn else null end) fb_stories_revn
                  ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                  and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                  then fb_stories_revn else null end) fb_stories_revn_prior
                  ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                  then fb_stories_revn else null end) ly_qtd_fb_stories_revn
                  ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                  and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                  then fb_stories_revn else null end) lq_qtd_fb_stories_revn
                  ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                  then fb_stories_revn else null end) L7d_avg_fb_stories_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then fb_stories_revn else null end) L7d_fb_stories_revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                             cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                        then fb_stories_revn else null end) L7d_fb_stories_revn_prior

                   --resilient_revn
                  ,Sum(case when cast(date_trunc('quarter',cast(date_id as date)) as varchar)
                  >= '<quarter_id>' then resilient_revn else null end) resilient_revn

                  ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                  and cast(date_trunc('quarter',cast(date_id as date)) as varchar) >= '<quarter_id>'
                  then resilient_revn else null end) resilient_revn_prior

                  ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '12' MONTH)
                  then resilient_revn else null end) ly_qtd_resilient_revn

                  ,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '3' MONTH)
                  and date_trunc('quarter',cast(date_id as date)) = (cast('<quarter_id>' as date) - INTERVAL '3' MONTH)
                  then resilient_revn else null end) lq_qtd_resilient_revn

                  ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                  then resilient_revn else null end) L7d_avg_resilient_revn

                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '6' DAY)
                    then resilient_revn else null end) L7d_resilient_revn

                    --- 14 day revn
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY) and
                             cast(date_id as date) <= (asofdate - INTERVAL '7' DAY)
                        then resilient_revn else null end) l7d_resilient_prior
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY)
                        then revenue else null end) L14d_revenue
                    ,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
                            then revenue else null end) L14d_revenue_prior
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '13' DAY)
                    then revenue else null end) L14d_avg_revenue
                    ,AVG(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
                                 cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
                            then revenue else null end) L14d_avg_revenue_prior


              from <TABLE:bpo_gms_solutions_snapshot> prod

              LEFT JOIN (SELECT date_id sales_date
                                      ,id_d_customer_account
                                      ,id_dh_territory
                                      ,SUM(paid_msgs_spend_split_usd) wa_messages_spend
                                      ,SUM(paid_msgs_delivered_split) wa_message_volume
                               FROM gms_whatsapp_notifications


                               WHERE ds= '<LATEST_DS:gms_whatsapp_notifications>'
                               GROUP BY 1,2,3
                              ) whatsapp on whatsapp.sales_date = prod.date_id
              AND whatsapp.id_d_customer_account = prod.id_d_customer_account_adv
              and whatsapp.id_dh_territory = prod.id_dh_territory

              cross join (Select max(cast(date_id as date)) as asofdate from
              <TABLE:bpo_gms_solutions_snapshot> where ds = '<DATEID>' AND  optimal is not null)

              where ds = '<DATEID>'

              group by
               ultimate_parent_fbid
              ,ultimate_parent_sfid
              ,ultimate_parent_name
              ,revenue_segment
              ,program
              ,advertiser_coverage_model_daa
              ,advertiser_program_daa
              ,advertiser_vertical
              ,specialty
              ,legacy_advertiser_sub_vertical
              ,legacy_advertiser_vertical
              ,gms_optimal_target
              ,gms_liquidity_target
              ,gso_optimal_target
              ,gso_liquidity_target
              ,smb_optimal_target
              ,smb_liquidity_target
              ,l4_optimal_target
              ,l4_liquidity_target
              ,l6_optimal_target
              ,l6_liquidity_target
              ,l8_optimal_target
              ,l8_liquidity_target
              ,l10_optimal_target
              ,l10_liquidity_target
              ,l12_optimal_target
              ,l12_liquidity_target
              ,cp_username
              ,cp_manager_username
              ,am_username
              ,am_manager_username
              ,pm_username
              ,pm_manager_username
              ,ap_username
              ,ap_manager_username
              ,l12_reporting_territory
              ,l10_reporting_territory
              ,l8_reporting_territory
              ,l6_reporting_territory
              ,l4_reporting_territory
              ,l2_reporting_territory
              ,l12_reporting_terr_mgr
              ,l10_reporting_terr_mgr
              ,l8_reporting_terr_mgr
              ,l6_reporting_terr_mgr
              ,l12_agency_territory
              ,l10_agency_territory
              ,l8_agency_territory
              ,l6_agency_territory
              ,l4_agency_territory
              ,l2_agency_territory
              ,l12_manager_agency_territory
              ,l10_manager_agency_territory
              ,l8_manager_agency_territory
              ,l6_manager_agency_territory
              ,cast(asofdate as varchar)
              ,segmentation
              ,csm_username
              ,csm_manager_username
              ,asm_username
              ,asm_manager_username
              ,rp_username
              ,rp_manager_username
              ,sales_adv_country_group
              ,sales_adv_subregion
              ,sales_adv_region
              ,market
              ,advertiser_country
              ,planning_agency_name
              ,planning_agency_fbid
              ,planning_agency_sfid
              ,planning_agency_ult_fbid
              ,planning_agency_ult_sfid
              ,planning_agency_ult_name
              ,program_optimal_target
              ,program_liquidity_target
              ,CAST(CURRENT_TIMESTAMP AS VARCHAR)

              """
        self.delete = """
            DELETE FROM <TABLE:bpo_gms_solutions_fast>
            where ds <> '<DATEID>'
            """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select

    def get_delete(self):
        """returns a delete statement that will remove all partitions
        from a table that are not '<DATEID>'"""
        return self.delete


class BpoGmsQuotaAndForecastSnapshot:
    """@docstring class to store DDL AND DML
    for bpo_gms_quota_and_forecast_snapshot
    """

    def __init__(self):
        self.name = "<TABLE:bpo_gms_quota_and_forecast_snapshot>"
        self.create = """
            CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_quota_and_forecast_snapshot> (
            date_id varchar,
            id_dh_territory bigint,
            advertiser_name varchar,
            advertiser_country varchar,
            advertiser_vertical varchar,
            advertiser_sub_vertical varchar,
            gvm_vertical_name varchar,
            gvm_sub_vertical_name varchar,
            specialty VARCHAR,
            legacy_advertiser_sub_vertical VARCHAR,
            legacy_advertiser_vertical VARCHAR,
            ultimate_parent_vertical_name_v2 VARCHAR, -- added on 2020-07-14
            region varchar,
            sub_region varchar,
            advertiser_fbid varchar,
            advertiser_sfid varchar,
            ultimate_parent_fbid varchar,
            ultimate_parent_sfid varchar,
            ultimate_parent_name varchar,
            planning_agency_name varchar,
            planning_agency_fbid varchar,
            planning_agency_sfid varchar,
            planning_agency_ult_fbid varchar,
            planning_agency_ult_sfid varchar,
            planning_agency_ult_name varchar,
            split double,
            id_d_employee bigint,
            client_partner varchar,
            cp_username varchar,
            cp_start_date varchar,
            cp_manager varchar,
            cp_manager_username varchar,
            account_manager varchar,
            am_username varchar,
            am_start_date varchar,
            am_manager varchar,
            am_manager_username varchar,
            partner_manager varchar,
            pm_username varchar,
            pm_start_date varchar,
            pm_manager varchar,
            pm_manager_username varchar,
            agency_partner varchar,
            ap_username varchar,
            ap_start_date varchar,
            ap_manager varchar,
            ap_manager_username varchar,
            reseller_fbid varchar,
            reseller_sfid varchar,
            reseller_name varchar,
            program varchar,
            id_d_customer_account_adv bigint,
            advertiser_coverage_model_daa varchar,
            advertiser_program_daa varchar,
            agency_coverage_model_daa varchar,
            is_gat boolean,
            is_gcm boolean,
            is_magic93 boolean,
            l12_advertiser_territory varchar,
            l10_advertiser_territory varchar,
            l8_advertiser_territory varchar,
            l6_advertiser_territory varchar,
            l4_advertiser_territory varchar,
            l2_advertiser_territory varchar,
            l12_usern_advertiser_territory varchar,
            l10_usern_advertiser_territory varchar,
            l12_manager_advertiser_territory varchar,
            l10_manager_advertiser_territory varchar,
            l8_manager_advertiser_territory varchar,
            l12_agency_territory varchar,
            l10_agency_territory varchar,
            l8_agency_territory varchar,
            l6_agency_territory varchar,
            l4_agency_territory varchar,
            l2_agency_territory varchar,
            l12_usern_agency_territory varchar,
            l10_usern_agency_territory varchar,
            l12_manager_agency_territory varchar,
            l10_manager_agency_territory varchar,
            l8_manager_agency_territory varchar,
            ad_account_l4_fbid BIGINT,
            ad_account_l8_fbid BIGINT,
            ad_account_l10_fbid BIGINT,
            ad_account_l12_fbid BIGINT,
            cq_revenue DOUBLE,
            cq_optimal DOUBLE,
            cq_liquidity DOUBLE,
            pq_revenue DOUBLE,
            pq_optimal DOUBLE,
            pq_liquidity DOUBLE,
            ly_revenue DOUBLE,
            l2y_revenue DOUBLE,
            ly_optimal DOUBLE,
            ly_liquidity DOUBLE,
            lyq_revenue DOUBLE,
            l2yq_revenue DOUBLE,
            lyq_optimal DOUBLE,
            lyq_liquidity DOUBLE,
            advertiser_quota DOUBLE,
            agency_quota DOUBLE,
            sales_forecast DOUBLE,
            sales_forecast_prior DOUBLE,
            smb_quota DOUBLE,
            smb_optimal_quota DOUBLE,
            smb_liquidity_quota DOUBLE,
            rep_fbid_am VARCHAR,
            rep_fbid_pm VARCHAR,
            rep_fbid_cp VARCHAR,
            rep_fbid_rp VARCHAR,
            rep_fbid_sp VARCHAR,
            rep_fbid_ap VARCHAR,
            rep_fbid_me VARCHAR,
            total_interactions_am DOUBLE,
            creative_am DOUBLE,
            measurement_am DOUBLE,
            solutions_engineering_am DOUBLE,
            agency_opportunity_am DOUBLE,
            agency_planning_call_am DOUBLE,
            agency_planning_check_in_am DOUBLE,
            face_to_face_am DOUBLE,
            dpa_am DOUBLE,
            daba_am DOUBLE,
            total_interactions_pm DOUBLE,
            creative_pm DOUBLE,
            measurement_pm DOUBLE,
            solutions_engineering_pm DOUBLE,
            agency_opportunity_pm DOUBLE,
            agency_planning_call_pm DOUBLE,
            agency_planning_check_in_pm DOUBLE,
            face_to_face_pm DOUBLE,
            dpa_pm DOUBLE,
            daba_pm DOUBLE,
            l12_reporting_territory VARCHAR,
            l10_reporting_territory VARCHAR,
            l8_reporting_territory  VARCHAR,
            l6_reporting_territory  VARCHAR,
            l4_reporting_territory  VARCHAR,
            l2_reporting_territory  VARCHAR,
            l12_reporting_terr_mgr VARCHAR,
            l10_reporting_terr_mgr VARCHAR,
            l8_reporting_terr_mgr VARCHAR,
            l6_reporting_terr_mgr VARCHAR,
            l6_manager_agency_territory VARCHAR,
            segmentation VARCHAR,
            rep_fbid_csm VARCHAR COMMENT 'FBID for Client Solutions Manager https://fburl.com/wut/cad62z1j',
            client_solutions_manager VARCHAR COMMENT 'full name for Client Solutions Manager',
            csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
            csm_start_date varchar COMMENT 'Start Date of CSM for a given ad account',
            csm_manager varchar COMMENT 'Full name for a CSMs manager',
            csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
            rep_fbid_asm VARCHAR COMMENT 'FBID for Agency Solutions Manager',
            agency_solutions_manager VARCHAR COMMENT 'Full name for ASM' ,
            asm_username varchar COMMENT 'UnixName for ASM',
            asm_start_date varchar COMMENT 'start date of an asm for a given ad account ',
            asm_manager varchar COMMENT 'Full name for an  ASMs manager',
            asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
            reseller_partner VARCHAR COMMENT 'Full name for reseller partner',
            rp_username varchar COMMENT 'UnixName for reseller partner',
            rp_start_date varchar COMMENT 'start date for a reseller partner for a given ad account',
            rp_manager varchar COMMENT 'Full name for an RPs manager',
            rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
            sales_adv_country_group VARCHAR,
            sales_adv_subregion VARCHAR,
            sales_adv_region VARCHAR,
            market VARCHAR,
            l12_reseller_territory VARCHAR,
            l10_reseller_territory VARCHAR,
            l8_reseller_territory  VARCHAR,
            l6_reseller_territory  VARCHAR,
            l4_reseller_territory  VARCHAR,
            l2_reseller_territory  VARCHAR,
            l12_reseller_terr_mgr VARCHAR,
            l10_reseller_terr_mgr VARCHAR,
            l8_reseller_terr_mgr  VARCHAR,
            l6_reseller_terr_mgr  VARCHAR,
            reseller_quota DOUBLE,
            country_agc VARCHAR,
            market_agc VARCHAR,
            region_agc VARCHAR,
            sub_region_agc VARCHAR,
            business_type_adv VARCHAR,
            business_type_agc VARCHAR,
            planning_agency_operating_co VARCHAR,
            program_agency VARCHAR,
            china_export_advertiser VARCHAR,
            export_advertiser_country VARCHAR,
            billing_country_adv VARCHAR,
            billing_region_adv VARCHAR,
            billing_country_agc VARCHAR,
            billing_region_agc VARCHAR,
            hq_country_adv VARCHAR,
            hq_region_adv VARCHAR,
            hq_country_agc VARCHAR,
            hq_region_agc VARCHAR,
            optimal_quota DOUBLE,
            agc_optimal_quota DOUBLE,
            ts VARCHAR,
            L7d_revenue DOUBLE,
            L7d_revenue_prior DOUBLE,
            L7d_avg_revenue DOUBLE,
            L7d_avg_revenue_prior DOUBLE,
            revenue_segment VARCHAR,
            dr_resilient_cq DOUBLE,
            dr_resilient_pq DOUBLE,
            dr_resilient_ly DOUBLE,
            dr_resilient_lyq DOUBLE,
            dr_resilience_goal DOUBLE,
            cq_product_resilient_rec_rev DOUBLE,
            cq_ebr_usd_rec_rev DOUBLE,
            cq_capi_ebr_revenue DOUBLE,
            pq_product_resilient_rec_rev DOUBLE,
            pq_ebr_usd_rec_rev DOUBLE,
            pq_capi_ebr_revenue DOUBLE,
            ly_product_resilient_rec_rev DOUBLE,
            ly_ebr_usd_rec_rev DOUBLE,
            ly_capi_ebr_revenue DOUBLE,
            lyq_product_resilient_rec_rev DOUBLE,
            lyq_ebr_usd_rec_rev DOUBLE,
            lyq_capi_ebr_revenue DOUBLE,
            subsegment VARCHAR,
            L14d_revenue DOUBLE,
            L14d_revenue_prior DOUBLE,
            L14d_avg_revenue DOUBLE,
            L14d_avg_revenue_prior DOUBLE,
            sales_forecast_prior_2w DOUBLE,
            ds varchar
           )
            WITH (
                    partitioned_by = ARRAY['ds'],
                    retention_days = <RETENTION:90>,
                    uii=false
                   ) """
        self.select = """
                 WITH CI AS (
                     SELECT <COLUMNS:<TABLE:bpo_gms_dashboard_stg_ci>>
                     FROM <TABLE:bpo_gms_dashboard_stg_ci>
                     WHERE ds ='<DATEID>'
           )
           SELECT
                 date_id
                ,id_dh_territory
                ,advertiser_name
                ,advertiser_country
                ,advertiser_vertical
                ,advertiser_sub_vertical
                ,gvm_vertical_name
                ,gvm_sub_vertical_name
                ,specialty
                ,legacy_advertiser_sub_vertical
                ,legacy_advertiser_vertical
                ,ultimate_parent_vertical_name_v2
                ,region
                ,sub_region
                ,advertiser_fbid
                ,advertiser_sfid
                ,ultimate_parent_fbid
                ,ultimate_parent_sfid
                ,ultimate_parent_name
                ,planning_agency_name
                ,planning_agency_fbid
                ,planning_agency_sfid
                ,planning_agency_ult_fbid
                ,planning_agency_ult_sfid
                ,planning_agency_ult_name
                ,split
                ,id_d_employee
                ,client_partner
                ,cp_username
                ,cp_start_date
                ,cp_manager
                ,cp_manager_username
                ,account_manager
                ,am_username
                ,am_start_date
                ,am_manager
                ,am_manager_username
                ,partner_manager
                ,pm_username
                ,pm_start_date
                ,pm_manager
                ,pm_manager_username
                ,agency_partner
                ,ap_username
                ,ap_start_date
                ,ap_manager
                ,ap_manager_username
                ,reseller_fbid
                ,reseller_sfid
                ,reseller_name
                ,program
                ,id_d_customer_account_adv
                ,advertiser_coverage_model_daa
                ,advertiser_program_daa
                ,agency_coverage_model_daa
                ,is_gat
                ,is_gcm
                ,is_magic93
                ,l12_advertiser_territory
                ,l10_advertiser_territory
                ,l8_advertiser_territory
                ,l6_advertiser_territory
                ,l4_advertiser_territory
                ,l2_advertiser_territory
                ,l12_usern_advertiser_territory
                ,l10_usern_advertiser_territory
                ,l12_manager_advertiser_territory
                ,l10_manager_advertiser_territory
                ,l8_manager_advertiser_territory
                ,l12_agency_territory
                ,l10_agency_territory
                ,l8_agency_territory
                ,l6_agency_territory
                ,l4_agency_territory
                ,l2_agency_territory
                ,l12_usern_agency_territory
                ,l10_usern_agency_territory
                ,l12_manager_agency_territory
                ,l10_manager_agency_territory
                ,l8_manager_agency_territory
                ,ad_account_l4_fbid
                ,ad_account_l8_fbid
                ,ad_account_l10_fbid
                ,ad_account_l12_fbid
                ,cq_revenue
                ,cq_optimal
                ,cq_liquidity
                ,pq_revenue
                ,pq_optimal
                ,pq_liquidity
                ,ly_revenue
                ,l2y_revenue
                ,ly_optimal
                ,ly_liquidity
                ,lyq_revenue
                ,l2yq_revenue
                ,lyq_optimal
                ,lyq_liquidity
                ,advertiser_quota
                ,agency_quota
                ,sales_forecast
                ,sales_forecast_prior
                ,smb_quota
                ,smb_optimal_quota
                ,smb_liquidity_quota
                ,total_interactions_am
                ,creative_am
                ,measurement_am
                ,solutions_engineering_am
                ,agency_opportunity_am
                ,agency_planning_call_am
                ,agency_planning_check_in_am
                ,face_to_face_am
                ,dpa_am
                ,daba_am
                ,total_interactions_pm
                ,creative_pm
                ,measurement_pm
                ,solutions_engineering_pm
                ,agency_opportunity_pm
                ,agency_planning_call_pm
                ,agency_planning_check_in_pm
                ,face_to_face_pm
                ,dpa_pm
                ,daba_pm
                ,rep_fbid_am
                ,rep_fbid_pm
                ,rep_fbid_cp
                ,rep_fbid_rp
                ,rep_fbid_sp
                ,rep_fbid_ap
                ,rep_fbid_me
                ,l12_reporting_territory
                ,l10_reporting_territory
                ,l8_reporting_territory
                ,l6_reporting_territory
                ,l4_reporting_territory
                ,l2_reporting_territory
                ,segmentation
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,l6_manager_agency_territory
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,l12_reseller_territory
                ,l10_reseller_territory
                ,l8_reseller_territory
                ,l6_reseller_territory
                ,l4_reseller_territory
                ,l2_reseller_territory
                ,l12_reseller_terr_mgr
                ,l10_reseller_terr_mgr
                ,l8_reseller_terr_mgr
                ,l6_reseller_terr_mgr
                ,reseller_quota
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,CASE
                     WHEN lower(revenue_segment) like '%outsourced%'
                     THEN COALESCE(sbg_optimal_goal,optimal_quota)
                     WHEN lower(revenue_segment) like '%sbg%'
                     THEN sbg_optimal_goal
                     ELSE optimal_quota
                END optimal_quota
                ,agc_optimal_quota
                ,ts
                ,L7d_revenue
                ,L7d_revenue_prior
                ,L7d_avg_revenue
                ,L7d_avg_revenue_prior
                ,revenue_segment
                ,NULL dr_resilient_cq
                ,NULL dr_resilient_pq
                ,NULL dr_resilient_ly
                ,NULL dr_resilient_lyq
                ,dr_resilience_goal
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
                ,subsegment
                ,L14d_revenue
                ,L14d_revenue_prior
                ,L14d_avg_revenue
                ,L14d_avg_revenue_prior
                ,sales_forecast_prior_2w
        FROM (
            Select
                date_id
                ,id_dh_territory
                ,advertiser_name
                ,advertiser_country
                ,advertiser_vertical
                ,advertiser_sub_vertical
                ,gvm_vertical_name
                ,gvm_sub_vertical_name
                ,specialty
                ,null as legacy_advertiser_sub_vertical
                ,null as legacy_advertiser_vertical
                ,ultimate_parent_vertical_name_v2
                ,region
                ,sub_region
                ,advertiser_fbid
                ,advertiser_sfid
                ,ultimate_parent_fbid
                ,ultimate_parent_sfid
                ,ultimate_parent_name
                ,planning_agency_name
                ,planning_agency_fbid
                ,planning_agency_sfid
                ,planning_agency_ult_fbid
                ,planning_agency_ult_sfid
                ,planning_agency_ult_name
                ,split
                ,id_d_employee
                ,client_partner
                ,cp_username
                ,cp_start_date
                ,cp_manager
                ,cp_manager_username
                ,account_manager
                ,am_username
                ,am_start_date
                ,am_manager
                ,am_manager_username
                ,partner_manager
                ,pm_username
                ,pm_start_date
                ,pm_manager
                ,pm_manager_username
                ,agency_partner
                ,ap_username
                ,ap_start_date
                ,ap_manager
                ,ap_manager_username
                ,reseller_fbid
                ,reseller_sfid
                ,reseller_name
                ,program
                ,id_d_customer_account_adv
                ,advertiser_coverage_model_daa
                ,advertiser_program_daa
                ,agency_coverage_model_daa
                ,is_gat
                ,is_gcm
                ,is_magic93
                ,l12_advertiser_territory
                ,l10_advertiser_territory
                ,l8_advertiser_territory
                ,l6_advertiser_territory
                ,l4_advertiser_territory
                ,l2_advertiser_territory
                ,l12_usern_advertiser_territory
                ,l10_usern_advertiser_territory
                ,l12_manager_advertiser_territory
                ,l10_manager_advertiser_territory
                ,l8_manager_advertiser_territory
                ,l12_agency_territory
                ,l10_agency_territory
                ,l8_agency_territory
                ,l6_agency_territory
                ,l4_agency_territory
                ,l2_agency_territory
                ,l12_usern_agency_territory
                ,l10_usern_agency_territory
                ,l12_manager_agency_territory
                ,l10_manager_agency_territory
                ,l8_manager_agency_territory
                ,ad_account_l4_fbid
                ,ad_account_l8_fbid
                ,ad_account_l10_fbid
                ,ad_account_l12_fbid
                ,cq_revenue
                ,cq_optimal
                ,cq_liquidity
                ,pq_revenue
                ,pq_optimal
                ,pq_liquidity
                ,ly_revenue
                ,l2y_revenue
                ,ly_optimal
                ,ly_liquidity
                ,lyq_revenue
                ,l2yq_revenue
                ,lyq_optimal
                ,lyq_liquidity
                ,advertiser_quota
                ,agency_quota
                ,sales_forecast
                ,sales_forecast_prior
                ,sbg_quota smb_quota
                ,optimal_quota smb_optimal_quota
                ,liquidity_quota smb_liquidity_quota
                ,ci_am.total_interactions total_interactions_am
                ,ci_am.creative creative_am
                ,ci_am.measurement measurement_am
                ,ci_am.solutions_engineering solutions_engineering_am
                ,ci_am.agency_opportunity agency_opportunity_am
                ,ci_am.agency_planning_call agency_planning_call_am
                ,ci_am.agency_planning_check_in agency_planning_check_in_am
                ,ci_am.face_to_face face_to_face_am
                ,ci_am.dpa dpa_am
                ,ci_am.daba daba_am
                ,ci_pm.total_interactions total_interactions_pm
                ,ci_pm.creative creative_pm
                ,ci_pm.measurement measurement_pm
                ,ci_pm.solutions_engineering solutions_engineering_pm
                ,ci_pm.agency_opportunity agency_opportunity_pm
                ,ci_pm.agency_planning_call agency_planning_call_pm
                ,ci_pm.agency_planning_check_in agency_planning_check_in_pm
                ,ci_pm.face_to_face face_to_face_pm
                ,ci_pm.dpa dpa_pm
                ,ci_pm.daba daba_pm
                ,rep_fbid_am
                ,rep_fbid_pm
                ,rep_fbid_cp
                ,rep_fbid_rp
                ,rep_fbid_sp
                ,rep_fbid_ap
                ,rep_fbid_me
                ,l12_reporting_territory
                ,l10_reporting_territory
                ,l8_reporting_territory
                ,l6_reporting_territory
                ,l4_reporting_territory
                ,l2_reporting_territory
                ,segmentation
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,l6_manager_agency_territory
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,l12_reseller_territory
                ,l10_reseller_territory
                ,l8_reseller_territory
                ,l6_reseller_territory
                ,l4_reseller_territory
                ,l2_reseller_territory
                ,l12_reseller_terr_mgr
                ,l10_reseller_terr_mgr
                ,l8_reseller_terr_mgr
                ,l6_reseller_terr_mgr
                ,reseller_quota
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,CASE WHEN rep_fbid_cp IS NOT NULL then inm_optimal_goal else optimal_quota end AS optimal_quota
                ,CASE WHEN rep_fbid_ap IS NOT NULL THEN agc_optimal_goal
                    WHEN planning_agency_fbid IS NOT NULL THEN optimal_quota
                    ELSE NULL END as agc_optimal_quota

                ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
                ,L7d_revenue
                ,L7d_revenue_prior
                ,L7d_avg_revenue
                ,L7d_avg_revenue_prior
                ,revenue_segment
                ,NULL dr_resilient_cq
                ,NULL dr_resilient_pq
                ,NULL dr_resilient_ly
                ,NULL dr_resilient_lyq
                ,dr_resilience_goal
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
                ,subsegment
                ,CASE WHEN am_lwi_ind = 'LWI'
                      THEN lwi_goal * advertiser_quota
                      WHEN am_lwi_ind = 'AM'
                      THEN am_goal * advertiser_quota
                ELSE NULL
                END sbg_optimal_goal
                ,L14d_revenue
                ,L14d_revenue_prior
                ,L14d_avg_revenue
                ,L14d_avg_revenue_prior
                ,sales_forecast_prior_2w
            from <TABLE:bpo_coverage_asis_stg_4> rev
            left join ci ci_am on rev.date_id = ci_am.interaction_date
            and CAST(rev.advertiser_fbid as BIGINT) =  ci_am.client_id
            and CAST(rev.rep_fbid_am AS BIGINT) = ci_am.account_manager_fbid
            left join ci ci_pm on rev.date_id = ci_pm.interaction_date
            and CAST(planning_agency_fbid as bigint)= ci_pm.client_id
            and CAST(rev.rep_fbid_pm as BIGINT) = CAST(ci_pm.account_manager_fbid AS BIGINT)

            where ds = '<DATEID>'
        )


            """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select


class BpoGmsQuotaAndForecast:
    """@docstring"""

    def __init__(self):
        self.name = "<TABLE:bpo_gms_quota_and_forecast>"
        self.create = """
            CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_quota_and_forecast> (
            date_id varchar,
            id_dh_territory bigint,
            advertiser_name varchar,
            advertiser_country varchar,
            advertiser_vertical varchar,
            advertiser_sub_vertical varchar,
            gvm_vertical_name varchar,
            gvm_sub_vertical_name varchar,
            region varchar,
            sub_region varchar,
            advertiser_fbid varchar,
            advertiser_sfid varchar,
            ultimate_parent_fbid varchar,
            ultimate_parent_sfid varchar,
            ultimate_parent_name varchar,
            planning_agency_name varchar,
            planning_agency_fbid varchar,
            planning_agency_sfid varchar,
            planning_agency_ult_fbid varchar,
            planning_agency_ult_sfid varchar,
            planning_agency_ult_name varchar,
            split double,
            id_d_employee bigint,
            client_partner varchar,
            cp_username varchar,
            cp_start_date varchar,
            cp_manager varchar,
            cp_manager_username varchar,
            account_manager varchar,
            am_username varchar,
            am_start_date varchar,
            am_manager varchar,
            am_manager_username varchar,
            partner_manager varchar,
            pm_username varchar,
            pm_start_date varchar,
            pm_manager varchar,
            pm_manager_username varchar,
            agency_partner varchar,
            ap_username varchar,
            ap_start_date varchar,
            ap_manager varchar,
            ap_manager_username varchar,
            reseller_fbid varchar,
            reseller_sfid varchar,
            reseller_name varchar,
            program varchar,
            id_d_customer_account_adv bigint,
            advertiser_coverage_model_daa varchar,
            advertiser_program_daa varchar,
            agency_coverage_model_daa varchar,
            is_gat boolean,
            is_gcm boolean,
            is_magic93 boolean,
            l12_advertiser_territory varchar,
            l10_advertiser_territory varchar,
            l8_advertiser_territory varchar,
            l6_advertiser_territory varchar,
            l4_advertiser_territory varchar,
            l2_advertiser_territory varchar,
            l12_usern_advertiser_territory varchar,
            l10_usern_advertiser_territory varchar,
            l12_manager_advertiser_territory varchar,
            l10_manager_advertiser_territory varchar,
            l8_manager_advertiser_territory varchar,
            l12_agency_territory varchar,
            l10_agency_territory varchar,
            l8_agency_territory varchar,
            l6_agency_territory varchar,
            l4_agency_territory varchar,
            l2_agency_territory varchar,
            l12_usern_agency_territory varchar,
            l10_usern_agency_territory varchar,
            l12_manager_agency_territory varchar,
            l10_manager_agency_territory varchar,
            l8_manager_agency_territory varchar,
            ad_account_l4_fbid BIGINT,
            ad_account_l8_fbid BIGINT,
            ad_account_l10_fbid BIGINT,
            ad_account_l12_fbid BIGINT,
            cq_revenue DOUBLE,
            cq_optimal DOUBLE,
            cq_liquidity DOUBLE,
            pq_revenue DOUBLE,
            pq_optimal DOUBLE,
            pq_liquidity DOUBLE,
            ly_revenue DOUBLE,
            l2y_revenue DOUBLE,
            ly_optimal DOUBLE,
            ly_liquidity DOUBLE,
            lyq_revenue DOUBLE,
            l2yq_revenue DOUBLE,
            lyq_optimal DOUBLE,
            lyq_liquidity DOUBLE,
            advertiser_quota DOUBLE,
            agency_quota DOUBLE,
            sales_forecast DOUBLE,
            sales_forecast_prior DOUBLE,
            smb_quota DOUBLE,
            smb_optimal_quota DOUBLE,
            smb_liquidity_quota DOUBLE,
            rep_fbid_am VARCHAR,
            rep_fbid_pm VARCHAR,
            rep_fbid_cp VARCHAR,
            rep_fbid_rp VARCHAR,
            rep_fbid_sp VARCHAR,
            rep_fbid_ap VARCHAR,
            rep_fbid_me VARCHAR,
            total_interactions_am DOUBLE,
            creative_am DOUBLE,
            measurement_am DOUBLE,
            solutions_engineering_am DOUBLE,
            agency_opportunity_am DOUBLE,
            agency_planning_call_am DOUBLE,
            agency_planning_check_in_am DOUBLE,
            face_to_face_am DOUBLE,
            dpa_am DOUBLE,
            daba_am DOUBLE,
            total_interactions_pm DOUBLE,
            creative_pm DOUBLE,
            measurement_pm DOUBLE,
            solutions_engineering_pm DOUBLE,
            agency_opportunity_pm DOUBLE,
            agency_planning_call_pm DOUBLE,
            agency_planning_check_in_pm DOUBLE,
            face_to_face_pm DOUBLE,
            dpa_pm DOUBLE,
            daba_pm DOUBLE,
            l12_reporting_territory VARCHAR,
            l10_reporting_territory VARCHAR,
            l8_reporting_territory  VARCHAR,
            l6_reporting_territory  VARCHAR,
            l4_reporting_territory  VARCHAR,
            l2_reporting_territory  VARCHAR,
            l12_reporting_terr_mgr VARCHAR,
            l10_reporting_terr_mgr VARCHAR,
            l8_reporting_terr_mgr VARCHAR,
            l6_reporting_terr_mgr VARCHAR,
            l6_manager_agency_territory VARCHAR,
            segmentation VARCHAR,
            rep_fbid_csm VARCHAR COMMENT 'FBID for Client Solutions Manager https://fburl.com/wut/cad62z1j',
            client_solutions_manager VARCHAR COMMENT 'full name for Client Solutions Manager',
            csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
            csm_start_date varchar COMMENT 'Start Date of CSM for a given ad account',
            csm_manager varchar COMMENT 'Full name for a CSMs manager',
            csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
            rep_fbid_asm VARCHAR COMMENT 'FBID for Agency Solutions Manager',
            agency_solutions_manager VARCHAR COMMENT 'Full name for ASM' ,
            asm_username varchar COMMENT 'UnixName for ASM',
            asm_start_date varchar COMMENT 'start date of an asm for a given ad account ',
            asm_manager varchar COMMENT 'Full name for an  ASMs manager',
            asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
            reseller_partner VARCHAR COMMENT 'Full name for reseller partner',
            rp_username varchar COMMENT 'UnixName for reseller partner',
            rp_start_date varchar COMMENT 'start date for a reseller partner for a given ad account',
            rp_manager varchar COMMENT 'Full name for an RPs manager',
            rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
            sales_adv_country_group VARCHAR,
            sales_adv_subregion VARCHAR,
            sales_adv_region VARCHAR,
            market VARCHAR,
            l12_reseller_territory VARCHAR,
            l10_reseller_territory VARCHAR,
            l8_reseller_territory  VARCHAR,
            l6_reseller_territory  VARCHAR,
            l4_reseller_territory  VARCHAR,
            l2_reseller_territory  VARCHAR,
            l12_reseller_terr_mgr VARCHAR,
            l10_reseller_terr_mgr VARCHAR,
            l8_reseller_terr_mgr  VARCHAR,
            l6_reseller_terr_mgr  VARCHAR,
            reseller_quota DOUBLE,
            country_agc VARCHAR,
            market_agc VARCHAR,
            region_agc VARCHAR,
            sub_region_agc VARCHAR,
            business_type_adv VARCHAR,
            business_type_agc VARCHAR,
            planning_agency_operating_co VARCHAR,
            program_agency VARCHAR,
            china_export_advertiser VARCHAR,
            export_advertiser_country VARCHAR,
            billing_country_adv VARCHAR,
            billing_region_adv VARCHAR,
            billing_country_agc VARCHAR,
            billing_region_agc VARCHAR,
            hq_country_adv VARCHAR,
            hq_region_adv VARCHAR,
            hq_country_agc VARCHAR,
            hq_region_agc VARCHAR,
            optimal_quota DOUBLE,
            agc_optimal_quota DOUBLE,
            ts VARCHAR,
            L7d_revenue DOUBLE,
            L7d_revenue_prior DOUBLE,
            L7d_avg_revenue DOUBLE,
            L7d_avg_revenue_prior DOUBLE,
            legacy_advertiser_vertical VARCHAR,
            legacy_advertiser_sub_vertical VARCHAR,
            ultimate_parent_vertical_name_v2 VARCHAR, -- added on 2020-07-14
            specialty VARCHAR,
            revenue_segment VARCHAR,
            dr_resilient_cq DOUBLE,
            dr_resilient_pq DOUBLE,
            dr_resilient_ly DOUBLE,
            dr_resilient_lyq DOUBLE,
            dr_resilience_goal DOUBLE,
            cq_product_resilient_rec_rev DOUBLE,
            cq_ebr_usd_rec_rev DOUBLE,
            cq_capi_ebr_revenue DOUBLE,
            pq_product_resilient_rec_rev DOUBLE,
            pq_ebr_usd_rec_rev DOUBLE,
            pq_capi_ebr_revenue DOUBLE,
            ly_product_resilient_rec_rev DOUBLE,
            ly_ebr_usd_rec_rev DOUBLE,
            ly_capi_ebr_revenue DOUBLE,
            lyq_product_resilient_rec_rev DOUBLE,
            lyq_ebr_usd_rec_rev DOUBLE,
            lyq_capi_ebr_revenue DOUBLE,
            subsegment VARCHAR,
            L14d_revenue DOUBLE,
            L14d_revenue_prior DOUBLE,
            L14d_avg_revenue DOUBLE,
            L14d_avg_revenue_prior DOUBLE,
            sales_forecast_prior_2w DOUBLE,
            ds varchar
           )
            WITH (
                        partitioned_by = ARRAY['ds'],
                        retention_days = <RETENTION:90>,
                        uii=false
                       )"""
        self.select = """
            SELECT
                date_id
                ,id_dh_territory
                ,advertiser_name
                ,advertiser_country
                ,advertiser_vertical
                ,advertiser_sub_vertical
                ,gvm_vertical_name
                ,gvm_sub_vertical_name
                ,region
                ,sub_region
                ,advertiser_fbid
                ,advertiser_sfid
                ,ultimate_parent_fbid
                ,ultimate_parent_sfid
                ,ultimate_parent_name
                ,planning_agency_name
                ,planning_agency_fbid
                ,planning_agency_sfid
                ,planning_agency_ult_fbid
                ,planning_agency_ult_sfid
                ,planning_agency_ult_name
                ,split
                ,id_d_employee
                ,client_partner
                ,cp_username
                ,cp_start_date
                ,cp_manager
                ,cp_manager_username
                ,account_manager
                ,am_username
                ,am_start_date
                ,am_manager
                ,am_manager_username
                ,partner_manager
                ,pm_username
                ,pm_start_date
                ,pm_manager
                ,pm_manager_username
                ,agency_partner
                ,ap_username
                ,ap_start_date
                ,ap_manager
                ,ap_manager_username
                ,reseller_fbid
                ,reseller_sfid
                ,reseller_name
                ,program
                ,id_d_customer_account_adv
                ,advertiser_coverage_model_daa
                ,advertiser_program_daa
                ,agency_coverage_model_daa
                ,is_gat
                ,is_gcm
                ,is_magic93
                ,l12_advertiser_territory
                ,l10_advertiser_territory
                ,l8_advertiser_territory
                ,l6_advertiser_territory
                ,l4_advertiser_territory
                ,l2_advertiser_territory
                ,l12_usern_advertiser_territory
                ,l10_usern_advertiser_territory
                ,l12_manager_advertiser_territory
                ,l10_manager_advertiser_territory
                ,l8_manager_advertiser_territory
                ,l12_agency_territory
                ,l10_agency_territory
                ,l8_agency_territory
                ,l6_agency_territory
                ,l4_agency_territory
                ,l2_agency_territory
                ,l12_usern_agency_territory
                ,l10_usern_agency_territory
                ,l12_manager_agency_territory
                ,l10_manager_agency_territory
                ,l8_manager_agency_territory
                ,ad_account_l4_fbid
                ,ad_account_l8_fbid
                ,ad_account_l10_fbid
                ,ad_account_l12_fbid
                ,cq_revenue
                ,cq_optimal
                ,cq_liquidity
                ,pq_revenue
                ,pq_optimal
                ,pq_liquidity
                ,ly_revenue
                ,l2y_revenue
                ,ly_optimal
                ,ly_liquidity
                ,lyq_revenue
                ,l2yq_revenue
                ,lyq_optimal
                ,lyq_liquidity
                ,advertiser_quota
                ,agency_quota
                ,sales_forecast
                ,sales_forecast_prior
                ,smb_quota
                ,smb_optimal_quota
                ,smb_liquidity_quota
                ,total_interactions_am
                ,creative_am
                ,measurement_am
                ,solutions_engineering_am
                ,agency_opportunity_am
                ,agency_planning_call_am
                ,agency_planning_check_in_am
                ,face_to_face_am
                ,dpa_am
                ,daba_am
                ,total_interactions_pm
                ,creative_pm
                ,measurement_pm
                ,solutions_engineering_pm
                ,agency_opportunity_pm
                ,agency_planning_call_pm
                ,agency_planning_check_in_pm
                ,face_to_face_pm
                ,dpa_pm
                ,daba_pm
                ,rep_fbid_am
                ,rep_fbid_pm
                ,rep_fbid_cp
                ,rep_fbid_rp
                ,rep_fbid_sp
                ,rep_fbid_ap
                ,rep_fbid_me
                ,l12_reporting_territory
                ,l10_reporting_territory
                ,l8_reporting_territory
                ,l6_reporting_territory
                ,l4_reporting_territory
                ,l2_reporting_territory
                ,segmentation
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,l6_manager_agency_territory
                ,rep_fbid_csm
                ,client_solutions_manager
                ,csm_username
                ,csm_start_date
                ,csm_manager
                ,csm_manager_username
                ,rep_fbid_asm
                ,agency_solutions_manager
                ,asm_username
                ,asm_start_date
                ,asm_manager
                ,asm_manager_username
                ,reseller_partner
                ,rp_username
                ,rp_start_date
                ,rp_manager
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,l12_reseller_territory
                ,l10_reseller_territory
                ,l8_reseller_territory
                ,l6_reseller_territory
                ,l4_reseller_territory
                ,l2_reseller_territory
                ,l12_reseller_terr_mgr
                ,l10_reseller_terr_mgr
                ,l8_reseller_terr_mgr
                ,l6_reseller_terr_mgr
                ,reseller_quota
                ,country_agc
                ,market_agc
                ,region_agc
                ,sub_region_agc
                ,business_type_adv
                ,business_type_agc
                ,planning_agency_operating_co
                ,program_agency
                ,china_export_advertiser
                ,export_advertiser_country
                ,billing_country_adv
                ,billing_region_adv
                ,billing_country_agc
                ,billing_region_agc
                ,hq_country_adv
                ,hq_region_adv
                ,hq_country_agc
                ,hq_region_agc
                ,optimal_quota
                ,agc_optimal_quota
                ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
                ,L7d_revenue
                ,L7d_revenue_prior
                ,L7d_avg_revenue
                ,L7d_avg_revenue_prior
                ,NULL as legacy_advertiser_vertical
                ,NULL as legacy_advertiser_sub_vertical
                ,specialty
                ,ultimate_parent_vertical_name_v2
                ,revenue_segment
                ,dr_resilient_cq
                ,dr_resilient_pq
                ,dr_resilient_ly
                ,dr_resilient_lyq
                ,dr_resilience_goal
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
                ,L14d_revenue
                ,L14d_revenue_prior
                ,L14d_avg_revenue
                ,L14d_avg_revenue_prior
                ,subsegment
                ,sales_forecast_prior_2w
            from <TABLE:bpo_gms_quota_and_forecast_snapshot>

            where ds = '<DATEID>' """
        self.delete = """
            DELETE FROM <TABLE:bpo_gms_quota_and_forecast>
            where ds <> '<DATEID>'"""

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select

    def get_delete(self):
        """returns a delete statement that will remove all partitions
        from a table that are not '<DATEID>'"""
        return self.delete


class BpoGmsQuotaAndForecastFast:
    """@docstring"""

    def __init__(self):
        self.name = "<TABLE:bpo_gms_quota_and_forecast_fast>"
        self.create=Table(
            cols=[
                Column('ultimate_parent_fbid', 'varchar', 'ultimate_parent_fbid', HiveAnon.NONE),
                Column('ultimate_parent_sfid', 'varchar', 'ultimate_parent_sfid', HiveAnon.NONE),
                Column('ultimate_parent_name', 'varchar', 'ultimate_parent_name', HiveAnon.NONE),
                Column('advertiser_name', 'varchar', 'advertiser_name', HiveAnon.NONE),
                Column('advertiser_fbid', 'varchar', 'advertiser_fbid', HiveAnon.NONE),
                Column('advertiser_sfid', 'varchar', 'advertiser_sfid', HiveAnon.NONE),
                Column('program', 'varchar', 'program', HiveAnon.NONE),
                Column('advertiser_coverage_model_daa', 'varchar', 'advertiser_coverage_model_daa', HiveAnon.NONE),
                Column('advertiser_program_daa', 'varchar', 'advertiser_program_daa', HiveAnon.NONE),
                Column('advertiser_vertical', 'varchar', 'advertiser_vertical', HiveAnon.NONE),
                Column('segmentation', 'varchar', 'segmentation', HiveAnon.NONE),
                Column('cp_username', 'varchar', 'cp_username', HiveAnon.NONE),
                Column('cp_manager_username', 'varchar', 'cp_manager_username', HiveAnon.NONE),
                Column('am_username', 'varchar', 'am_username', HiveAnon.NONE),
                Column('am_manager_username', 'varchar', 'am_manager_username', HiveAnon.NONE),
                Column('pm_username', 'varchar', 'pm_username', HiveAnon.NONE),
                Column('pm_manager_username', 'varchar', 'pm_manager_username', HiveAnon.NONE),
                Column('ap_username', 'varchar', 'ap_username', HiveAnon.NONE),
                Column('ap_manager_username', 'varchar', 'ap_manager_username', HiveAnon.NONE),
                Column('asofdate', 'varchar', 'asofdate', HiveAnon.NONE),
                Column('quarter_id', 'varchar', 'quarter_id', HiveAnon.NONE),
                Column('next_quarter_id', 'varchar', 'next_quarter_id', HiveAnon.NONE),
                Column('days_left_in_quarter', 'bigint', 'days_left_in_quarter', HiveAnon.NONE),
                Column('days_left_in_quarter_prior', 'bigint', 'days_left_in_quarter_prior', HiveAnon.NONE),
                Column('days_total_in_quarter', 'bigint', 'days_total_in_quarter', HiveAnon.NONE),
                Column('days_closed_in_quarter', 'bigint', 'days_closed_in_quarter', HiveAnon.NONE),
                Column('run_rate_forecast', 'double', 'run_rate_forecast', HiveAnon.NONE),
                Column('run_rate_forecast_prior', 'double', 'run_rate_forecast_prior', HiveAnon.NONE),
                Column('advertiser_quota', 'double', 'advertiser_quota', HiveAnon.NONE),
                Column('sales_forecast', 'double', 'sales_forecast', HiveAnon.NONE),
                Column('sales_forecast_prior', 'double', 'sales_forecast_prior', HiveAnon.NONE),
                Column('cq_revenue', 'double', 'cq_revenue', HiveAnon.NONE),
                Column('pq_revenue', 'double', 'pq_revenue', HiveAnon.NONE),
                Column('cq_revenue_qtd_prior', 'double', 'cq_revenue_qtd_prior', HiveAnon.NONE),
                Column('lyq_revenue', 'double', 'lyq_revenue', HiveAnon.NONE),
                Column('l2yq_revenue', 'double', 'l2yq_revenue', HiveAnon.NONE),
                Column('lyq_revenue_qtd', 'double', 'lyq_revenue_qtd', HiveAnon.NONE),
                Column('l2yq_revenue_qtd', 'double', 'l2yq_revenue_qtd', HiveAnon.NONE),
                Column('lyq_revenue_qtd_prior', 'double', 'lyq_revenue_qtd_prior', HiveAnon.NONE),
                Column('l2yq_revenue_qtd_prior', 'double', 'l2yq_revenue_qtd_prior', HiveAnon.NONE),
                Column('pq_revenue_qtd', 'double', 'pq_revenue_qtd', HiveAnon.NONE),
                Column('l7d_revenue', 'double', 'l7d_revenue', HiveAnon.NONE),
                Column('l7d_revenue_prior', 'double', 'l7d_revenue_prior', HiveAnon.NONE),
                Column('l7d_avg_revenue', 'double', 'l7d_avg_revenue', HiveAnon.NONE),
                Column('l7d_avg_revenue_prior', 'double', 'l7d_avg_revenue_prior', HiveAnon.NONE),
                Column('l14d_avg_revenue', 'double', 'l14d_avg_revenue', HiveAnon.NONE),
                Column('l28d_avg_revenue', 'double', 'l28d_avg_revenue', HiveAnon.NONE),
                Column('l28d_avg_revenue_prior', 'double', 'l28d_avg_revenue_prior', HiveAnon.NONE),
                Column('l12_reporting_territory', 'varchar', 'l12_reporting_territory', HiveAnon.NONE),
                Column('l10_reporting_territory', 'varchar', 'l10_reporting_territory', HiveAnon.NONE),
                Column('l8_reporting_territory', 'varchar', 'l8_reporting_territory', HiveAnon.NONE),
                Column('l6_reporting_territory', 'varchar', 'l6_reporting_territory', HiveAnon.NONE),
                Column('l4_reporting_territory', 'varchar', 'l4_reporting_territory', HiveAnon.NONE),
                Column('l2_reporting_territory', 'varchar', 'l2_reporting_territory', HiveAnon.NONE),
                Column('l12_reporting_terr_mgr', 'varchar', 'l12_reporting_terr_mgr', HiveAnon.NONE),
                Column('l10_reporting_terr_mgr', 'varchar', 'l10_reporting_terr_mgr', HiveAnon.NONE),
                Column('l8_reporting_terr_mgr', 'varchar', 'l8_reporting_terr_mgr', HiveAnon.NONE),
                Column('l6_reporting_terr_mgr', 'varchar', 'l6_reporting_terr_mgr', HiveAnon.NONE),
                Column('l12_agency_territory', 'varchar', 'l12_agency_territory', HiveAnon.NONE),
                Column('l10_agency_territory', 'varchar', 'l10_agency_territory', HiveAnon.NONE),
                Column('l8_agency_territory', 'varchar', 'l8_agency_territory', HiveAnon.NONE),
                Column('l6_agency_territory', 'varchar', 'l6_agency_territory', HiveAnon.NONE),
                Column('l4_agency_territory', 'varchar', 'l4_agency_territory', HiveAnon.NONE),
                Column('l2_agency_territory', 'varchar', 'l2_agency_territory', HiveAnon.NONE),
                Column('agency_quota', 'DOUBLE', 'agency_quota', HiveAnon.NONE),
                Column('l12_manager_agency_territory', 'varchar', 'l12_manager_agency_territory', HiveAnon.NONE),
                Column('l10_manager_agency_territory', 'varchar', 'l10_manager_agency_territory', HiveAnon.NONE),
                Column('L8_manager_agency_territory', 'varchar', 'L8_manager_agency_territory', HiveAnon.NONE),
                Column('l6_manager_agency_territory', 'varchar', 'l6_manager_agency_territory', HiveAnon.NONE),
                Column('csm_username', 'varchar', 'UnixName for Client Solutions Manager', HiveAnon.NONE),
                Column('csm_manager_username', 'varchar', 'UnixName for a CSMs manager', HiveAnon.NONE),
                Column('asm_username', 'varchar', 'UnixName for ASM', HiveAnon.NONE),
                Column('asm_manager_username', 'varchar', 'UnixName for a ASMs manager', HiveAnon.NONE),
                Column('rp_username', 'varchar', 'UnixName for reseller partner', HiveAnon.NONE),
                Column('rp_manager_username', 'varchar', 'UnixName of an RPs manager', HiveAnon.NONE),
                Column('sales_adv_country_group', 'varchar', 'sales_adv_country_group', HiveAnon.NONE),
                Column('sales_adv_subregion', 'varchar', 'sales_adv_subregion', HiveAnon.NONE),
                Column('sales_adv_region', 'varchar', 'sales_adv_region', HiveAnon.NONE),
                Column('market', 'varchar', 'market', HiveAnon.NONE),
                Column('advertiser_country', 'varchar', 'advertiser_country', HiveAnon.NONE),
                Column('planning_agency_name', 'varchar', 'planning_agency_name', HiveAnon.NONE),
                Column('planning_agency_fbid', 'varchar', 'planning_agency_fbid', HiveAnon.NONE),
                Column('planning_agency_sfid', 'varchar', 'planning_agency_sfid', HiveAnon.NONE),
                Column('planning_agency_ult_fbid', 'varchar', 'planning_agency_ult_fbid', HiveAnon.NONE),
                Column('planning_agency_ult_sfid', 'varchar', 'planning_agency_ult_sfid', HiveAnon.NONE),
                Column('planning_agency_ult_name', 'varchar', 'planning_agency_ult_name', HiveAnon.NONE),
                Column('l12_reseller_territory', 'varchar', 'l12_reseller_territory', HiveAnon.NONE),
                Column('l10_reseller_territory', 'varchar', 'l10_reseller_territory', HiveAnon.NONE),
                Column('l8_reseller_territory', 'varchar', 'l8_reseller_territory', HiveAnon.NONE),
                Column('l6_reseller_territory', 'varchar', 'l6_reseller_territory', HiveAnon.NONE),
                Column('l4_reseller_territory', 'varchar', 'l4_reseller_territory', HiveAnon.NONE),
                Column('l2_reseller_territory', 'varchar', 'l2_reseller_territory', HiveAnon.NONE),
                Column('l12_reseller_terr_mgr', 'varchar', 'l12_reseller_terr_mgr', HiveAnon.NONE),
                Column('l10_reseller_terr_mgr', 'varchar', 'l10_reseller_terr_mgr', HiveAnon.NONE),
                Column('l8_reseller_terr_mgr', 'varchar', 'l8_reseller_terr_mgr', HiveAnon.NONE),
                Column('l6_reseller_terr_mgr', 'varchar', 'l6_reseller_terr_mgr', HiveAnon.NONE),
                Column('reseller_quota', 'DOUBLE', 'reseller_quota', HiveAnon.NONE),
                Column('l7d_avg_ly_revenue', 'DOUBLE', 'l7d_avg_ly_revenue', HiveAnon.NONE),
                Column('l7d_avg_ly_revenue_prior', 'DOUBLE', 'l7d_avg_ly_revenue_prior', HiveAnon.NONE),
                Column('l7d_ly_revenue', 'DOUBLE', 'l7d_ly_revenue', HiveAnon.NONE),
                Column('l7d_ly_revenue_prior', 'DOUBLE', 'l7d_ly_revenue_prior', HiveAnon.NONE),
                Column('program_agency', 'varchar', 'program_agency', HiveAnon.NONE),
                Column('country_agc', 'varchar', 'country_agc', HiveAnon.NONE),
                Column('market_agc', 'varchar', 'market_agc', HiveAnon.NONE),
                Column('region_agc', 'varchar', 'region_agc', HiveAnon.NONE),
                Column('sub_region_agc', 'varchar', 'sub_region_agc', HiveAnon.NONE),
                Column('cq_optimal', 'double', 'cq_optimal', HiveAnon.NONE),
                Column('pq_optimal', 'double', 'pq_optimal', HiveAnon.NONE),
                Column('cq_optimal_qtd_prior', 'double', 'cq_optimal_qtd_prior', HiveAnon.NONE),
                Column('lyq_optimal', 'double', 'lyq_optimal', HiveAnon.NONE),
                Column('lyq_optimal_qtd', 'double', 'lyq_optimal_qtd', HiveAnon.NONE),
                Column('lyq_optimal_qtd_prior', 'double', 'lyq_optimal_qtd_prior', HiveAnon.NONE),
                Column('pq_optimal_qtd', 'double', 'pq_optimal_qtd', HiveAnon.NONE),
                Column('l7d_optimal', 'double', 'l7d_optimal', HiveAnon.NONE),
                Column('l7d_optimal_prior', 'double', 'l7d_optimal_prior', HiveAnon.NONE),
                Column('l7d_avg_optimal', 'double', 'l7d_avg_optimal', HiveAnon.NONE),
                Column('l7d_avg_optimal_prior', 'double', 'l7d_avg_optimal_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_optimal', 'double', 'l7d_avg_ly_optimal', HiveAnon.NONE),
                Column('l7d_avg_ly_optimal_prior', 'double', 'l7d_avg_ly_optimal_prior', HiveAnon.NONE),
                Column('l7d_ly_optimal', 'double', 'l7d_ly_optimal', HiveAnon.NONE),
                Column('l7d_ly_optimal_prior', 'double', 'l7d_ly_optimal_prior', HiveAnon.NONE),
                Column('optimal_quota', 'double', 'optimal_quota', HiveAnon.NONE),
                Column('agc_optimal_quota', 'double', 'agc_optimal_quota', HiveAnon.NONE),
                Column('reseller_fbid', 'varchar', 'reseller_fbid', HiveAnon.NONE),
                Column('reseller_name', 'varchar', 'reseller_name', HiveAnon.NONE),
                Column('legacy_advertiser_vertical', 'varchar', 'legacy_advertiser_vertical', HiveAnon.NONE),
                Column('legacy_advertiser_sub_vertical', 'varchar', 'legacy_advertiser_sub_vertical', HiveAnon.NONE),
                Column('ultimate_parent_vertical_name_v2', 'varchar', 'ultimate_parent_vertical_name_v2', HiveAnon.NONE),
                Column('specialty', 'varchar', 'specialty', HiveAnon.NONE),
                Column('advertiser_sub_vertical', 'varchar', 'advertiser_sub_vertical', HiveAnon.NONE),
                Column('ts', 'varchar', 'ts', HiveAnon.NONE),
                Column('revenue_segment', 'varchar', 'revenue_segment', HiveAnon.NONE),
                Column('dr_resilience_goal', 'double', 'dr_resilience_goal', HiveAnon.NONE),
                Column('dr_resilient_cq', 'double', 'dr_resilient_cq', HiveAnon.NONE),
                Column('dr_resilient_pq', 'double', 'dr_resilient_pq', HiveAnon.NONE),
                Column('dr_resilient_ly', 'double', 'dr_resilient_ly', HiveAnon.NONE),
                Column('dr_resilient_lyq', 'double', 'dr_resilient_lyq', HiveAnon.NONE),
                Column('cq_dr_resilient_qtd_prior', 'double', 'cq_dr_resilient_qtd_prior', HiveAnon.NONE),
                Column('lyq_dr_resilient', 'double', 'lyq_dr_resilient', HiveAnon.NONE),
                Column('lyq_dr_resilient_qtd', 'double', 'lyq_dr_resilient_qtd', HiveAnon.NONE),
                Column('lyq_dr_resilient_qtd_prior', 'double', 'lyq_dr_resilient_qtd_prior', HiveAnon.NONE),
                Column('pq_dr_resilient_qtd', 'double', 'pq_dr_resilient_qtd', HiveAnon.NONE),
                Column('l7d_dr_resilient', 'double', 'l7d_dr_resilient', HiveAnon.NONE),
                Column('l7d_dr_resilient_prior', 'double', 'l7d_dr_resilient_prior', HiveAnon.NONE),
                Column('l7d_avg_dr_resilient', 'double', 'l7d_avg_dr_resilient', HiveAnon.NONE),
                Column('l7d_avg_dr_resilient_prior', 'double', 'l7d_avg_dr_resilient_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_dr_resilient', 'double', 'l7d_avg_ly_dr_resilient', HiveAnon.NONE),
                Column('l7d_avg_ly_dr_resilient_prior', 'double', 'l7d_avg_ly_dr_resilient_prior', HiveAnon.NONE),
                Column('l7d_ly_dr_resilient', 'double', 'l7d_ly_dr_resilient', HiveAnon.NONE),
                Column('l7d_ly_dr_resilient_prior', 'double', 'l7d_ly_dr_resilient_prior', HiveAnon.NONE),
                Column('dr_resilient_quota', 'double', 'dr_resilient_quota', HiveAnon.NONE),
                Column('cq_product_resilient_rec_rev', 'double', 'cq_product_resilient_rec_rev', HiveAnon.NONE),
                Column('cq_ebr_usd_rec_rev', 'double', 'cq_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('cq_capi_ebr_revenue', 'double', 'cq_capi_ebr_revenue', HiveAnon.NONE),
                Column('pq_product_resilient_rec_rev', 'double', 'pq_product_resilient_rec_rev', HiveAnon.NONE),
                Column('pq_ebr_usd_rec_rev', 'double', 'pq_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('pq_capi_ebr_revenue', 'double', 'pq_capi_ebr_revenue', HiveAnon.NONE),
                Column('ly_product_resilient_rec_rev', 'double', 'ly_product_resilient_rec_rev', HiveAnon.NONE),
                Column('ly_ebr_usd_rec_rev', 'double', 'ly_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('ly_capi_ebr_revenue', 'double', 'ly_capi_ebr_revenue', HiveAnon.NONE),
                Column('lyq_product_resilient_rec_rev', 'double', 'lyq_product_resilient_rec_rev', HiveAnon.NONE),
                Column('lyq_ebr_usd_rec_rev', 'double', 'lyq_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('lyq_capi_ebr_revenue', 'double', 'lyq_capi_ebr_revenue', HiveAnon.NONE),
                Column('cq_product_resilient_rec_rev_qtd_prior', 'double', 'cq_product_resilient_rec_rev_qtd_prior', HiveAnon.NONE),
                Column('cq_ebr_usd_rec_rev_qtd_prior', 'double', 'cq_ebr_usd_rec_rev_qtd_prior', HiveAnon.NONE),
                Column('cq_capi_ebr_revenue_qtd_prior', 'double', 'cq_capi_ebr_revenue_qtd_prior', HiveAnon.NONE),
                Column('lyq_product_resilient_rec_rev_qtd', 'double', 'lyq_product_resilient_rec_rev_qtd', HiveAnon.NONE),
                Column('lyq_ebr_usd_rec_rev_qtd', 'double', 'lyq_ebr_usd_rec_rev_qtd', HiveAnon.NONE),
                Column('lyq_capi_ebr_revenue_qtd', 'double', 'lyq_capi_ebr_revenue_qtd', HiveAnon.NONE),
                Column('l7d_product_resilient_rec_rev', 'double', 'l7d_product_resilient_rec_rev', HiveAnon.NONE),
                Column('l7d_ebr_usd_rec_rev', 'double', 'l7d_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('l7d_capi_ebr_revenue', 'double', 'l7d_capi_ebr_revenue', HiveAnon.NONE),
                Column('l7d_product_resilient_rec_rev_prior', 'double', 'l7d_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_ebr_usd_rec_rev_prior', 'double', 'l7d_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_capi_ebr_revenue_prior', 'double', 'l7d_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l7d_avg_product_resilient_rec_rev', 'double', 'l7d_avg_product_resilient_rec_rev', HiveAnon.NONE),
                Column('l7d_avg_ebr_usd_rec_rev', 'double', 'l7d_avg_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('l7d_avg_capi_ebr_revenue', 'double', 'l7d_avg_capi_ebr_revenue', HiveAnon.NONE),
                Column('l7d_avg_product_resilient_rec_rev_prior', 'double', 'l7d_avg_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_avg_ebr_usd_rec_rev_prior', 'double', 'l7d_avg_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_avg_capi_ebr_revenue_prior', 'double', 'l7d_avg_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_product_resilient_rec_rev', 'double', 'l7d_avg_ly_product_resilient_rec_rev', HiveAnon.NONE),
                Column('l7d_avg_ly_ebr_usd_rec_rev', 'double', 'l7d_avg_ly_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('l7d_avg_ly_capi_ebr_revenue', 'double', 'l7d_avg_ly_capi_ebr_revenue', HiveAnon.NONE),
                Column('l7d_avg_ly_product_resilient_rec_rev_prior', 'double', 'l7d_avg_ly_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_ebr_usd_rec_rev_prior', 'double', 'l7d_avg_ly_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_capi_ebr_revenue_prior', 'double', 'l7d_avg_ly_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l7d_ly_product_resilient_rec_rev', 'double', 'l7d_ly_product_resilient_rec_rev', HiveAnon.NONE),
                Column('l7d_ly_ebr_usd_rec_rev', 'double', 'l7d_ly_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('l7d_ly_capi_ebr_revenue', 'double', 'l7d_ly_capi_ebr_revenue', HiveAnon.NONE),
                Column('l7d_ly_product_resilient_rec_rev_prior', 'double', 'l7d_ly_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_ly_ebr_usd_rec_rev_prior', 'double', 'l7d_ly_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_ly_capi_ebr_revenue_prior', 'double', 'l7d_ly_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('subsegment', 'varchar', 'subsegment', HiveAnon.NONE),
                Column('L14d_revenue', 'double', 'L14d_revenue', HiveAnon.NONE),
                Column('L14d_revenue_prior', 'double', 'L14d_revenue_prior', HiveAnon.NONE),
                Column('sales_forecast_prior_2w', 'double', 'sales_forecast_prior_2w', HiveAnon.NONE),
                Column('days_left_in_quarter_2w_prior', 'double', 'days_left_in_quarter_2w_prior', HiveAnon.NONE),
                Column('run_rate_forecast_2w_prior', 'double', 'run_rate_forecast_2w_prior', HiveAnon.NONE),
                Column('cq_revenue_qtd_2w_prior', 'double', 'cq_revenue_qtd_2w_prior', HiveAnon.NONE),
                Column('lyq_revenue_qtd_2w_prior', 'double', 'lyq_revenue_qtd_2w_prior', HiveAnon.NONE),
                Column('l2yq_revenue_qtd_2w_prior', 'double', 'l2yq_revenue_qtd_2w_prior', HiveAnon.NONE),
                Column('l14d_avg_revenue_prior', 'double', 'l14d_avg_revenue_prior', HiveAnon.NONE),
                Column('cq_optimal_qtd_2w_prior', 'double', 'cq_optimal_qtd_2w_prior', HiveAnon.NONE),
                Column('lyq_optimal_qtd_2w_prior', 'double', 'lyq_optimal_qtd_2w_prior', HiveAnon.NONE),
                Column('l14d_optimal_prior', 'double', 'l14d_optimal_prior', HiveAnon.NONE),
                Column('l14d_avg_optimal_prior', 'double', 'l14d_avg_optimal_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_optimal_prior', 'double', 'l14d_avg_ly_optimal_prior', HiveAnon.NONE),
                Column('l14d_ly_optimal_prior', 'double', 'l14d_ly_optimal_prior', HiveAnon.NONE),
                Column('cq_dr_resilient_qtd_2w_prior', 'double', 'cq_dr_resilient_qtd_2w_prior', HiveAnon.NONE),
                Column('lyq_dr_resilient_qtd_2w_prior', 'double', 'lyq_dr_resilient_qtd_2w_prior', HiveAnon.NONE),
                Column('l14d_dr_resilient_prior', 'double', 'l14d_dr_resilient_prior', HiveAnon.NONE),
                Column('l14d_avg_dr_resilient_prior', 'double', 'l14d_avg_dr_resilient_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_dr_resilient_prior', 'double', 'l14d_avg_ly_dr_resilient_prior', HiveAnon.NONE),
                Column('l14d_ly_dr_resilient_prior', 'double', 'l14d_ly_dr_resilient_prior', HiveAnon.NONE),
                Column('cq_product_resilient_rec_rev_qtd_2w_prior', 'double', 'cq_product_resilient_rec_rev_qtd_2w_prior', HiveAnon.NONE),
                Column('cq_ebr_usd_rec_rev_qtd_2w_prior', 'double', 'cq_ebr_usd_rec_rev_qtd_2w_prior', HiveAnon.NONE),
                Column('cq_capi_ebr_revenue_qtd_2w_prior', 'double', 'cq_capi_ebr_revenue_qtd_2w_prior', HiveAnon.NONE),
                Column('l14d_product_resilient_rec_rev_prior', 'double', 'l14d_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_ebr_usd_rec_rev_prior', 'double', 'l14d_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_capi_ebr_revenue_prior', 'double', 'l14d_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l14d_avg_product_resilient_rec_rev_prior', 'double', 'l14d_avg_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_avg_ebr_usd_rec_rev_prior', 'double', 'l14d_avg_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_avg_capi_ebr_revenue_prior', 'double', 'l14d_avg_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_product_resilient_rec_rev_prior', 'double', 'l14d_avg_ly_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_ebr_usd_rec_rev_prior', 'double', 'l14d_avg_ly_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_capi_ebr_revenue_prior', 'double', 'l14d_avg_ly_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l14d_ly_product_resilient_rec_rev_prior', 'double', 'l14d_ly_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_ly_ebr_usd_rec_rev_prior', 'double', 'l14d_ly_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_ly_capi_ebr_revenue_prior', 'double', 'l14d_ly_capi_ebr_revenue_prior', HiveAnon.NONE),
            ],
            partitions=[Column('ds', 'VARCHAR', 'datestamp')],
            retention=90,
        )
        self.select = """
WITH quarter_date as (Select
cast('<quarter_id>' as date) as quarter_id,
(cast('<quarter_id>' as date) + INTERVAL '3' MONTH) next_quarter_id,
max(cast(date_id as date)) asofdate
from
<TABLE:bpo_gms_quota_and_forecast_snapshot>
where cq_revenue is not null and ds = '<DATEID>'
group by 1,2)

,quarter_dates as (
Select
quarter_id,
next_quarter_id,
asofdate,
DATE_DIFF('day',(asofdate),next_quarter_id- INTERVAL '1' DAY) days_left_in_quarter,
DATE_DIFF('day',(asofdate - INTERVAL '7' DAY),next_quarter_id- INTERVAL '1' DAY) days_left_in_quarter_prior,
DATE_DIFF('day',(asofdate - INTERVAL '14' DAY),next_quarter_id- INTERVAL '1' DAY) days_left_in_quarter_2w_prior,
DATE_DIFF('day', quarter_id, next_quarter_id) days_total_in_quarter,
DATE_DIFF('day', quarter_id, asofdate + INTERVAL '1' DAY) days_closed_in_quarter
from
quarter_date)

Select
 ultimate_parent_fbid
,ultimate_parent_sfid
,ultimate_parent_name
,advertiser_name
,advertiser_fbid
,advertiser_sfid
,program
,advertiser_coverage_model_daa
,advertiser_program_daa
,advertiser_vertical
,cp_username
,cp_manager_username
,am_username
,am_manager_username
,pm_username
,pm_manager_username
,ap_username
,ap_manager_username
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
,l12_reporting_terr_mgr
,l10_reporting_terr_mgr
,l8_reporting_terr_mgr
,l6_reporting_terr_mgr
,l12_agency_territory
,l10_agency_territory
,l8_agency_territory
,l6_agency_territory
,l4_agency_territory
,l2_agency_territory
,l12_manager_agency_territory
,l10_manager_agency_territory
,l8_manager_agency_territory
,l6_manager_agency_territory
,segmentation
,csm_username
,csm_manager_username
,asm_username
,asm_manager_username
,rp_username
,rp_manager_username
,sales_adv_country_group
,sales_adv_subregion
,sales_adv_region
,market
,advertiser_country
,planning_agency_name
,planning_agency_fbid
,planning_agency_sfid
,planning_agency_ult_fbid
,planning_agency_ult_sfid
,planning_agency_ult_name
,l12_reseller_territory
,l10_reseller_territory
,l8_reseller_territory
,l6_reseller_territory
,l4_reseller_territory
,l2_reseller_territory
,l12_reseller_terr_mgr
,l10_reseller_terr_mgr
,l8_reseller_terr_mgr
,l6_reseller_terr_mgr
,program_agency
,country_agc
,market_agc
,region_agc
,sub_region_agc
,reseller_fbid
,reseller_name
,NULL as legacy_advertiser_vertical
,NULL as legacy_advertiser_sub_vertical
,ultimate_parent_vertical_name_v2
,specialty
,advertiser_sub_vertical
,revenue_segment
,subsegment
,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
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
then cq_revenue else null end) L7d_revenue_prior,
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
,SUM(sales_forecast_prior_2w) sales_forecast_prior_2w,
days_left_in_quarter_2w_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_revenue else 0 end) + (SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY)
and cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_revenue else 0 end)/7 * days_left_in_quarter_prior) run_rate_forecast_2w_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_revenue else 0 end) cq_revenue_qtd_2w_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_revenue else 0 end) lyq_revenue_qtd_2w_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then l2y_revenue else 0 end) l2yq_revenue_qtd_2w_prior
,SUM(case when days_closed_in_quarter >= 28 then (case when date_id >= '<DATEID-27>'
and date_id <= '<DATEID-14>' then cq_revenue else null end) / 14 else
(case when date_id >= '<DATEID-27>' and date_id <= '<DATEID-14>'
then cq_revenue else null end) / (days_closed_in_quarter-14) end) L14d_avg_revenue_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_optimal else 0 end) cq_optimal_qtd_2w_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_optimal else 0 end) lyq_optimal_qtd_2w_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_optimal else null end) L14d_optimal_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_optimal else null end)/14 L14d_avg_optimal_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_optimal else null end)/14 L14d_avg_ly_optimal_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_optimal else null end) L14d_ly_optimal_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then dr_resilient_cq else 0 end) cq_dr_resilient_qtd_2w_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then dr_resilient_ly else 0 end) lyq_dr_resilient_qtd_2w_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then dr_resilient_cq else null end) L14d_dr_resilient_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then dr_resilient_cq else null end)/14 L14d_avg_dr_resilient_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then dr_resilient_ly else null end)/14 L14d_avg_ly_dr_resilient_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then dr_resilient_ly else null end) L14d_ly_dr_resilient_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_product_resilient_rec_rev else 0 end) cq_product_resilient_rec_rev_qtd_2w_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_ebr_usd_rec_rev else 0 end) cq_ebr_usd_rec_rev_qtd_2w_prior
,Sum(case when cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_capi_ebr_revenue else 0 end) cq_capi_ebr_revenue_qtd_2w_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_product_resilient_rec_rev else null end) L14d_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_ebr_usd_rec_rev else null end) L14d_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_capi_ebr_revenue else null end) L14d_capi_ebr_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_product_resilient_rec_rev else null end)/14 l14d_avg_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_ebr_usd_rec_rev else null end)/14 l14d_avg_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
        cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then cq_capi_ebr_revenue else null end)/14 l14d_avg_capi_ebr_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_product_resilient_rec_rev else null end)/14 L14d_avg_ly_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_ebr_usd_rec_rev else null end)/14 L14d_avg_ly_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_capi_ebr_revenue else null end)/14 L14d_avg_ly_capi_ebr_revenue_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_product_resilient_rec_rev else null end) L14d_ly_product_resilient_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_ebr_usd_rec_rev else null end) L14d_ly_ebr_usd_rec_rev_prior
,SUM(case when cast(date_id as date) >= (asofdate - INTERVAL '27' DAY) and
    cast(date_id as date) <= (asofdate - INTERVAL '14' DAY)
then ly_capi_ebr_revenue else null end) L14d_ly_capi_ebr_revenue_prior
         from <TABLE:bpo_gms_quota_and_forecast_snapshot>

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
            ,program
            ,advertiser_coverage_model_daa
            ,advertiser_program_daa
            ,advertiser_vertical
            ,advertiser_sub_vertical
            ,specialty
            ,legacy_advertiser_sub_vertical
            ,legacy_advertiser_vertical
            ,ultimate_parent_vertical_name_v2
            ,cp_username
            ,cp_manager_username
            ,am_username
            ,am_manager_username
            ,pm_username
            ,pm_manager_username
            ,ap_username
            ,ap_manager_username
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
            ,l12_reporting_terr_mgr
            ,l10_reporting_terr_mgr
            ,l8_reporting_terr_mgr
            ,l6_reporting_terr_mgr
            ,l12_agency_territory
            ,l10_agency_territory
            ,l8_agency_territory
            ,l6_agency_territory
            ,l4_agency_territory
            ,l2_agency_territory
            ,l12_manager_agency_territory
            ,l10_manager_agency_territory
            ,l8_manager_agency_territory
            ,l6_manager_agency_territory
            ,segmentation
            ,csm_username
            ,csm_manager_username
            ,asm_username
            ,asm_manager_username
            ,rp_username
            ,rp_manager_username
            ,sales_adv_country_group
            ,sales_adv_subregion
            ,sales_adv_region
            ,market
            ,advertiser_country
            ,planning_agency_name
            ,planning_agency_fbid
            ,planning_agency_sfid
            ,planning_agency_ult_fbid
            ,planning_agency_ult_sfid
            ,planning_agency_ult_name
            ,l12_reseller_territory
            ,l10_reseller_territory
            ,l8_reseller_territory
            ,l6_reseller_territory
            ,l4_reseller_territory
            ,l2_reseller_territory
            ,l12_reseller_terr_mgr
            ,l10_reseller_terr_mgr
            ,l8_reseller_terr_mgr
            ,l6_reseller_terr_mgr
            ,program_agency
            ,country_agc
            ,market_agc
            ,region_agc
            ,sub_region_agc
            ,reseller_fbid
            ,reseller_name
            ,CAST(CURRENT_TIMESTAMP AS VARCHAR)
            ,subsegment
            ,days_left_in_quarter_2w_prior
             """
        self.delete = """
            DELETE FROM <TABLE:bpo_gms_quota_and_forecast_fast>

            where DS <> '<DATEID>'


            """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select

    def get_delete(self):
        """returns a delete statement that will remove all partitions
        from a table that are not '<DATEID>'"""
        return self.delete


class BpoGmsQuotaAndForecastUberFast:
    """@docstring"""

    def __init__(self):
        self.name = "<TABLE:bpo_gms_quota_and_forecast_uber_fast>"
        self.create=Table(
            cols=[
                Column('program', 'varchar', 'program', HiveAnon.NONE),
                Column('revenue_segment', 'varchar', 'revenue_segment', HiveAnon.NONE),
                Column('advertiser_vertical', 'varchar', 'advertiser_vertical', HiveAnon.NONE),
                Column('advertiser_sub_vertical', 'varchar', 'advertiser_sub_vertical', HiveAnon.NONE),
                Column('segmentation', 'varchar', 'segmentation', HiveAnon.NONE),
                Column('cp_username', 'varchar', 'cp_username', HiveAnon.NONE),
                Column('cp_manager_username', 'varchar', 'cp_manager_username', HiveAnon.NONE),
                Column('am_username', 'varchar', 'am_username', HiveAnon.NONE),
                Column('am_manager_username', 'varchar', 'am_manager_username', HiveAnon.NONE),
                Column('pm_username', 'varchar', 'pm_username', HiveAnon.NONE),
                Column('pm_manager_username', 'varchar', 'pm_manager_username', HiveAnon.NONE),
                Column('ap_username', 'varchar', 'ap_username', HiveAnon.NONE),
                Column('ap_manager_username', 'varchar', 'ap_manager_username', HiveAnon.NONE),
                Column('asofdate', 'varchar', 'asofdate', HiveAnon.NONE),
                Column('quarter_id', 'varchar', 'quarter_id', HiveAnon.NONE),
                Column('next_quarter_id', 'varchar', 'next_quarter_id', HiveAnon.NONE),
                Column('days_left_in_quarter', 'bigint', 'days_left_in_quarter', HiveAnon.NONE),
                Column('days_left_in_quarter_prior', 'bigint', 'days_left_in_quarter_prior', HiveAnon.NONE),
                Column('days_total_in_quarter', 'bigint', 'days_total_in_quarter', HiveAnon.NONE),
                Column('days_closed_in_quarter', 'bigint', 'days_closed_in_quarter', HiveAnon.NONE),
                Column('run_rate_forecast', 'double', 'run_rate_forecast', HiveAnon.NONE),
                Column('run_rate_forecast_prior', 'double', 'run_rate_forecast_prior', HiveAnon.NONE),
                Column('advertiser_quota', 'double', 'advertiser_quota', HiveAnon.NONE),
                Column('sales_forecast', 'double', 'sales_forecast', HiveAnon.NONE),
                Column('sales_forecast_prior', 'double', 'sales_forecast_prior', HiveAnon.NONE),
                Column('cq_revenue', 'double', 'cq_revenue', HiveAnon.NONE),
                Column('pq_revenue', 'double', 'pq_revenue', HiveAnon.NONE),
                Column('cq_revenue_qtd_prior', 'double', 'cq_revenue_qtd_prior', HiveAnon.NONE),
                Column('lyq_revenue', 'double', 'lyq_revenue', HiveAnon.NONE),
                Column('l2yq_revenue', 'double', 'l2yq_revenue', HiveAnon.NONE),
                Column('lyq_revenue_qtd', 'double', 'lyq_revenue_qtd', HiveAnon.NONE),
                Column('l2yq_revenue_qtd', 'double', 'l2yq_revenue_qtd', HiveAnon.NONE),
                Column('lyq_revenue_qtd_prior', 'double', 'lyq_revenue_qtd_prior', HiveAnon.NONE),
                Column('l2yq_revenue_qtd_prior', 'double', 'l2yq_revenue_qtd_prior', HiveAnon.NONE),
                Column('pq_revenue_qtd', 'double', 'pq_revenue_qtd', HiveAnon.NONE),
                Column('l7d_revenue', 'double', 'l7d_revenue', HiveAnon.NONE),
                Column('l7d_revenue_prior', 'double', 'l7d_revenue_prior', HiveAnon.NONE),
                Column('l7d_avg_revenue', 'double', 'l7d_avg_revenue', HiveAnon.NONE),
                Column('l7d_avg_revenue_prior', 'double', 'l7d_avg_revenue_prior', HiveAnon.NONE),
                Column('l12_reporting_territory', 'varchar', 'l12_reporting_territory', HiveAnon.NONE),
                Column('l10_reporting_territory', 'varchar', 'l10_reporting_territory', HiveAnon.NONE),
                Column('l8_reporting_territory', 'varchar', 'l8_reporting_territory', HiveAnon.NONE),
                Column('l6_reporting_territory', 'varchar', 'l6_reporting_territory', HiveAnon.NONE),
                Column('l4_reporting_territory', 'varchar', 'l4_reporting_territory', HiveAnon.NONE),
                Column('l2_reporting_territory', 'varchar', 'l2_reporting_territory', HiveAnon.NONE),
                Column('l12_reporting_terr_mgr', 'varchar', 'l12_reporting_terr_mgr', HiveAnon.NONE),
                Column('l10_reporting_terr_mgr', 'varchar', 'l10_reporting_terr_mgr', HiveAnon.NONE),
                Column('l8_reporting_terr_mgr', 'varchar', 'l8_reporting_terr_mgr', HiveAnon.NONE),
                Column('l6_reporting_terr_mgr', 'varchar', 'l6_reporting_terr_mgr', HiveAnon.NONE),
                Column('l12_agency_territory', 'varchar', 'l12_agency_territory', HiveAnon.NONE),
                Column('l10_agency_territory', 'varchar', 'l10_agency_territory', HiveAnon.NONE),
                Column('l8_agency_territory', 'varchar', 'l8_agency_territory', HiveAnon.NONE),
                Column('l6_agency_territory', 'varchar', 'l6_agency_territory', HiveAnon.NONE),
                Column('l4_agency_territory', 'varchar', 'l4_agency_territory', HiveAnon.NONE),
                Column('l2_agency_territory', 'varchar', 'l2_agency_territory', HiveAnon.NONE),
                Column('agency_quota', 'DOUBLE', 'agency_quota', HiveAnon.NONE),
                Column('l12_manager_agency_territory', 'varchar', 'l12_manager_agency_territory', HiveAnon.NONE),
                Column('l10_manager_agency_territory', 'varchar', 'l10_manager_agency_territory', HiveAnon.NONE),
                Column('L8_manager_agency_territory', 'varchar', 'L8_manager_agency_territory', HiveAnon.NONE),
                Column('l6_manager_agency_territory', 'varchar', 'l6_manager_agency_territory', HiveAnon.NONE),
                Column('csm_username', 'varchar', 'UnixName for Client Solutions Manager', HiveAnon.NONE),
                Column('csm_manager_username', 'varchar', 'UnixName for a CSMs manager', HiveAnon.NONE),
                Column('asm_username', 'varchar', 'UnixName for ASM', HiveAnon.NONE),
                Column('asm_manager_username', 'varchar', 'UnixName for a ASMs manager', HiveAnon.NONE),
                Column('rp_username', 'varchar', 'UnixName for reseller partner', HiveAnon.NONE),
                Column('rp_manager_username', 'varchar', 'UnixName of an RPs manager', HiveAnon.NONE),
                Column('sales_adv_country_group', 'varchar', 'sales_adv_country_group', HiveAnon.NONE),
                Column('sales_adv_subregion', 'varchar', 'sales_adv_subregion', HiveAnon.NONE),
                Column('sales_adv_region', 'varchar', 'sales_adv_region', HiveAnon.NONE),
                Column('market', 'varchar', 'market', HiveAnon.NONE),
                Column('advertiser_country', 'varchar', 'advertiser_country', HiveAnon.NONE),
                Column('l12_reseller_territory', 'varchar', 'l12_reseller_territory', HiveAnon.NONE),
                Column('l10_reseller_territory', 'varchar', 'l10_reseller_territory', HiveAnon.NONE),
                Column('l8_reseller_territory', 'varchar', 'l8_reseller_territory', HiveAnon.NONE),
                Column('l6_reseller_territory', 'varchar', 'l6_reseller_territory', HiveAnon.NONE),
                Column('l4_reseller_territory', 'varchar', 'l4_reseller_territory', HiveAnon.NONE),
                Column('l2_reseller_territory', 'varchar', 'l2_reseller_territory', HiveAnon.NONE),
                Column('l12_reseller_terr_mgr', 'varchar', 'l12_reseller_terr_mgr', HiveAnon.NONE),
                Column('l10_reseller_terr_mgr', 'varchar', 'l10_reseller_terr_mgr', HiveAnon.NONE),
                Column('l8_reseller_terr_mgr', 'varchar', 'l8_reseller_terr_mgr', HiveAnon.NONE),
                Column('l6_reseller_terr_mgr', 'varchar', 'l6_reseller_terr_mgr', HiveAnon.NONE),
                Column('reseller_quota', 'double', 'reseller_quota', HiveAnon.NONE),
                Column('cq_optimal', 'double', 'cq_optimal', HiveAnon.NONE),
                Column('pq_optimal', 'double', 'pq_optimal', HiveAnon.NONE),
                Column('cq_optimal_qtd_prior', 'double', 'cq_optimal_qtd_prior', HiveAnon.NONE),
                Column('lyq_optimal', 'double', 'lyq_optimal', HiveAnon.NONE),
                Column('lyq_optimal_qtd', 'double', 'lyq_optimal_qtd', HiveAnon.NONE),
                Column('lyq_optimal_qtd_prior', 'double', 'lyq_optimal_qtd_prior', HiveAnon.NONE),
                Column('pq_optimal_qtd', 'double', 'pq_optimal_qtd', HiveAnon.NONE),
                Column('l7d_optimal', 'double', 'l7d_optimal', HiveAnon.NONE),
                Column('l7d_optimal_prior', 'double', 'l7d_optimal_prior', HiveAnon.NONE),
                Column('l7d_avg_optimal', 'double', 'l7d_avg_optimal', HiveAnon.NONE),
                Column('l7d_avg_optimal_prior', 'double', 'l7d_avg_optimal_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_optimal', 'double', 'l7d_avg_ly_optimal', HiveAnon.NONE),
                Column('l7d_avg_ly_optimal_prior', 'double', 'l7d_avg_ly_optimal_prior', HiveAnon.NONE),
                Column('l7d_ly_optimal', 'double', 'l7d_ly_optimal', HiveAnon.NONE),
                Column('l7d_ly_optimal_prior', 'double', 'l7d_ly_optimal_prior', HiveAnon.NONE),
                Column('optimal_quota', 'double', 'optimal_quota', HiveAnon.NONE),
                Column('agc_optimal_quota', 'double', 'agc_optimal_quota', HiveAnon.NONE),
                Column('legacy_advertiser_vertical', 'varchar', 'legacy_advertiser_vertical', HiveAnon.NONE),
                Column('legacy_advertiser_sub_vertical', 'varchar', 'legacy_advertiser_sub_vertical', HiveAnon.NONE),
                Column('ultimate_parent_vertical_name_v2', 'varchar', 'ultimate_parent_vertical_name_v2', HiveAnon.NONE),
                Column('specialty', 'varchar', 'specialty', HiveAnon.NONE),
                Column('dr_resilience_goal', 'double', 'dr_resilience_goal', HiveAnon.NONE),
                Column('dr_resilient_cq', 'double', 'dr_resilient_cq', HiveAnon.NONE),
                Column('dr_resilient_pq', 'double', 'dr_resilient_pq', HiveAnon.NONE),
                Column('dr_resilient_ly', 'double', 'dr_resilient_ly', HiveAnon.NONE),
                Column('dr_resilient_lyq', 'double', 'dr_resilient_lyq', HiveAnon.NONE),
                Column('cq_dr_resilient_qtd_prior', 'double', 'cq_dr_resilient_qtd_prior', HiveAnon.NONE),
                Column('lyq_dr_resilient', 'double', 'lyq_dr_resilient', HiveAnon.NONE),
                Column('lyq_dr_resilient_qtd', 'double', 'lyq_dr_resilient_qtd', HiveAnon.NONE),
                Column('lyq_dr_resilient_qtd_prior', 'double', 'lyq_dr_resilient_qtd_prior', HiveAnon.NONE),
                Column('pq_dr_resilient_qtd', 'double', 'pq_dr_resilient_qtd', HiveAnon.NONE),
                Column('l7d_dr_resilient', 'double', 'l7d_dr_resilient', HiveAnon.NONE),
                Column('l7d_dr_resilient_prior', 'double', 'l7d_dr_resilient_prior', HiveAnon.NONE),
                Column('l7d_avg_dr_resilient', 'double', 'l7d_avg_dr_resilient', HiveAnon.NONE),
                Column('l7d_avg_dr_resilient_prior', 'double', 'l7d_avg_dr_resilient_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_dr_resilient', 'double', 'l7d_avg_ly_dr_resilient', HiveAnon.NONE),
                Column('l7d_avg_ly_dr_resilient_prior', 'double', 'l7d_avg_ly_dr_resilient_prior', HiveAnon.NONE),
                Column('l7d_ly_dr_resilient', 'double', 'l7d_ly_dr_resilient', HiveAnon.NONE),
                Column('l7d_ly_dr_resilient_prior', 'double', 'l7d_ly_dr_resilient_prior', HiveAnon.NONE),
                Column('dr_resilient_quota', 'double', 'dr_resilient_quota', HiveAnon.NONE),
                Column('cq_product_resilient_rec_rev', 'double', 'cq_product_resilient_rec_rev', HiveAnon.NONE),
                Column('cq_ebr_usd_rec_rev', 'double', 'cq_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('cq_capi_ebr_revenue', 'double', 'cq_capi_ebr_revenue', HiveAnon.NONE),
                Column('pq_product_resilient_rec_rev', 'double', 'pq_product_resilient_rec_rev', HiveAnon.NONE),
                Column('pq_ebr_usd_rec_rev', 'double', 'pq_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('pq_capi_ebr_revenue', 'double', 'pq_capi_ebr_revenue', HiveAnon.NONE),
                Column('ly_product_resilient_rec_rev', 'double', 'ly_product_resilient_rec_rev', HiveAnon.NONE),
                Column('ly_ebr_usd_rec_rev', 'double', 'ly_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('ly_capi_ebr_revenue', 'double', 'ly_capi_ebr_revenue', HiveAnon.NONE),
                Column('lyq_product_resilient_rec_rev', 'double', 'lyq_product_resilient_rec_rev', HiveAnon.NONE),
                Column('lyq_ebr_usd_rec_rev', 'double', 'lyq_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('lyq_capi_ebr_revenue', 'double', 'lyq_capi_ebr_revenue', HiveAnon.NONE),
                Column('cq_product_resilient_rec_rev_qtd_prior', 'double', 'cq_product_resilient_rec_rev_qtd_prior', HiveAnon.NONE),
                Column('cq_ebr_usd_rec_rev_qtd_prior', 'double', 'cq_ebr_usd_rec_rev_qtd_prior', HiveAnon.NONE),
                Column('cq_capi_ebr_revenue_qtd_prior', 'double', 'cq_capi_ebr_revenue_qtd_prior', HiveAnon.NONE),
                Column('lyq_product_resilient_rec_rev_qtd', 'double', 'lyq_product_resilient_rec_rev_qtd', HiveAnon.NONE),
                Column('lyq_ebr_usd_rec_rev_qtd', 'double', 'lyq_ebr_usd_rec_rev_qtd', HiveAnon.NONE),
                Column('lyq_capi_ebr_revenue_qtd', 'double', 'lyq_capi_ebr_revenue_qtd', HiveAnon.NONE),
                Column('l7d_product_resilient_rec_rev', 'double', 'l7d_product_resilient_rec_rev', HiveAnon.NONE),
                Column('l7d_ebr_usd_rec_rev', 'double', 'l7d_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('l7d_capi_ebr_revenue', 'double', 'l7d_capi_ebr_revenue', HiveAnon.NONE),
                Column('l7d_product_resilient_rec_rev_prior', 'double', 'l7d_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_ebr_usd_rec_rev_prior', 'double', 'l7d_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_capi_ebr_revenue_prior', 'double', 'l7d_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l7d_avg_product_resilient_rec_rev', 'double', 'l7d_avg_product_resilient_rec_rev', HiveAnon.NONE),
                Column('l7d_avg_ebr_usd_rec_rev', 'double', 'l7d_avg_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('l7d_avg_capi_ebr_revenue', 'double', 'l7d_avg_capi_ebr_revenue', HiveAnon.NONE),
                Column('l7d_avg_product_resilient_rec_rev_prior', 'double', 'l7d_avg_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_avg_ebr_usd_rec_rev_prior', 'double', 'l7d_avg_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_avg_capi_ebr_revenue_prior', 'double', 'l7d_avg_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_product_resilient_rec_rev', 'double', 'l7d_avg_ly_product_resilient_rec_rev', HiveAnon.NONE),
                Column('l7d_avg_ly_ebr_usd_rec_rev', 'double', 'l7d_avg_ly_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('l7d_avg_ly_capi_ebr_revenue', 'double', 'l7d_avg_ly_capi_ebr_revenue', HiveAnon.NONE),
                Column('l7d_avg_ly_product_resilient_rec_rev_prior', 'double', 'l7d_avg_ly_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_ebr_usd_rec_rev_prior', 'double', 'l7d_avg_ly_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_avg_ly_capi_ebr_revenue_prior', 'double', 'l7d_avg_ly_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l7d_ly_product_resilient_rec_rev', 'double', 'l7d_ly_product_resilient_rec_rev', HiveAnon.NONE),
                Column('l7d_ly_ebr_usd_rec_rev', 'double', 'l7d_ly_ebr_usd_rec_rev', HiveAnon.NONE),
                Column('l7d_ly_capi_ebr_revenue', 'double', 'l7d_ly_capi_ebr_revenue', HiveAnon.NONE),
                Column('l7d_ly_product_resilient_rec_rev_prior', 'double', 'l7d_ly_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_ly_ebr_usd_rec_rev_prior', 'double', 'l7d_ly_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l7d_ly_capi_ebr_revenue_prior', 'double', 'l7d_ly_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('subsegment', 'varchar', 'subsegment', HiveAnon.NONE),
                Column('ts', 'varchar', 'ts', HiveAnon.NONE),
                Column('L14d_revenue', 'double', 'L14d_revenue', HiveAnon.NONE),
                Column('L14d_revenue_prior', 'double', 'L14d_revenue_prior', HiveAnon.NONE),
                Column('l14d_avg_revenue', 'double', 'l14d_avg_revenue', HiveAnon.NONE),
                Column('l14d_avg_revenue_prior', 'double', 'l14d_avg_revenue_prior', HiveAnon.NONE),
                Column('sales_forecast_prior_2w', 'double', 'sales_forecast_prior_2w', HiveAnon.NONE),
                Column('run_rate_forecast_2w_prior', 'double', 'run_rate_forecast_2w_prior', HiveAnon.NONE),
                Column('cq_revenue_qtd_2w_prior', 'double', 'cq_revenue_qtd_2w_prior', HiveAnon.NONE),
                Column('lyq_revenue_qtd_2w_prior', 'double', 'lyq_revenue_qtd_2w_prior', HiveAnon.NONE),
                Column('l2yq_revenue_qtd_2w_prior', 'double', 'l2yq_revenue_qtd_2w_prior', HiveAnon.NONE),
                Column('cq_optimal_qtd_2w_prior', 'double', 'cq_optimal_qtd_2w_prior', HiveAnon.NONE),
                Column('lyq_optimal_qtd_2w_prior', 'double', 'lyq_optimal_qtd_2w_prior', HiveAnon.NONE),
                Column('l14d_optimal_prior', 'double', 'l14d_optimal_prior', HiveAnon.NONE),
                Column('l14d_avg_optimal_prior', 'double', 'l14d_avg_optimal_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_optimal_prior', 'double', 'l14d_avg_ly_optimal_prior', HiveAnon.NONE),
                Column('l14d_ly_optimal_prior', 'double', 'l14d_ly_optimal_prior', HiveAnon.NONE),
                Column('cq_dr_resilient_qtd_2w_prior', 'double', 'cq_dr_resilient_qtd_2w_prior', HiveAnon.NONE),
                Column('lyq_dr_resilient_qtd_2w_prior', 'double', 'lyq_dr_resilient_qtd_2w_prior', HiveAnon.NONE),
                Column('l14d_dr_resilient_prior', 'double', 'l14d_dr_resilient_prior', HiveAnon.NONE),
                Column('l14d_avg_dr_resilient_prior', 'double', 'l14d_avg_dr_resilient_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_dr_resilient_prior', 'double', 'l14d_avg_ly_dr_resilient_prior', HiveAnon.NONE),
                Column('l14d_ly_dr_resilient_prior', 'double', 'l14d_ly_dr_resilient_prior', HiveAnon.NONE),
                Column('cq_product_resilient_rec_rev_qtd_2w_prior', 'double', 'cq_product_resilient_rec_rev_qtd_2w_prior', HiveAnon.NONE),
                Column('cq_ebr_usd_rec_rev_qtd_2w_prior', 'double', 'cq_ebr_usd_rec_rev_qtd_2w_prior', HiveAnon.NONE),
                Column('cq_capi_ebr_revenue_qtd_2w_prior', 'double', 'cq_capi_ebr_revenue_qtd_2w_prior', HiveAnon.NONE),
                Column('l14d_product_resilient_rec_rev_prior', 'double', 'l14d_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_ebr_usd_rec_rev_prior', 'double', 'l14d_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_capi_ebr_revenue_prior', 'double', 'l14d_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l14d_avg_product_resilient_rec_rev_prior', 'double', 'l14d_avg_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_avg_ebr_usd_rec_rev_prior', 'double', 'l14d_avg_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_avg_capi_ebr_revenue_prior', 'double', 'l14d_avg_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_product_resilient_rec_rev_prior', 'double', 'l14d_avg_ly_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_ebr_usd_rec_rev_prior', 'double', 'l14d_avg_ly_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_avg_ly_capi_ebr_revenue_prior', 'double', 'l14d_avg_ly_capi_ebr_revenue_prior', HiveAnon.NONE),
                Column('l14d_ly_product_resilient_rec_rev_prior', 'double', 'l14d_ly_product_resilient_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_ly_ebr_usd_rec_rev_prior', 'double', 'l14d_ly_ebr_usd_rec_rev_prior', HiveAnon.NONE),
                Column('l14d_ly_capi_ebr_revenue_prior', 'double', 'l14d_ly_capi_ebr_revenue_prior', HiveAnon.NONE),
            ],
            partitions=[Column('ds', 'VARCHAR', 'datestamp')],
            retention=90,
        )
        self.select = """

            SELECT
                 program
                ,revenue_segment
                ,advertiser_vertical
                ,cp_username
                ,cp_manager_username
                ,am_username
                ,am_manager_username
                ,pm_username
                ,pm_manager_username
                ,ap_username
                ,ap_manager_username
                ,asofdate
                ,quarter_id
                ,next_quarter_id
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
                ,l12_reporting_terr_mgr
                ,l10_reporting_terr_mgr
                ,l8_reporting_terr_mgr
                ,l6_reporting_terr_mgr
                ,l12_agency_territory
                ,l10_agency_territory
                ,l8_agency_territory
                ,l6_agency_territory
                ,l4_agency_territory
                ,l2_agency_territory
                ,l12_manager_agency_territory
                ,l10_manager_agency_territory
                ,l8_manager_agency_territory
                ,l6_manager_agency_territory
                ,segmentation
                ,csm_username
                ,csm_manager_username
                ,asm_username
                ,asm_manager_username
                ,rp_username
                ,rp_manager_username
                ,sales_adv_country_group
                ,sales_adv_subregion
                ,sales_adv_region
                ,market
                ,advertiser_country
                ,l12_reseller_territory
                ,l10_reseller_territory
                ,l8_reseller_territory
                ,l6_reseller_territory
                ,l4_reseller_territory
                ,l2_reseller_territory
                ,l12_reseller_terr_mgr
                ,l10_reseller_terr_mgr
                ,l8_reseller_terr_mgr
                ,l6_reseller_terr_mgr
                ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
                ,SUM(run_rate_forecast) run_rate_forecast
                ,SUM(run_rate_forecast_prior) run_rate_forecast_prior
                ,SUM(advertiser_quota) advertiser_quota
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
                ,NULL as legacy_advertiser_vertical
                ,NULL as legacy_advertiser_sub_vertical
                ,ultimate_parent_vertical_name_v2
                ,specialty
                ,advertiser_sub_vertical
                ,subsegment
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
                ,SUM(run_rate_forecast_2w_prior) run_rate_forecast_2w_prior
                ,SUM(cq_revenue_qtd_2w_prior) cq_revenue_qtd_2w_prior
                ,SUM(lyq_revenue_qtd_2w_prior) lyq_revenue_qtd_2w_prior
                ,SUM(l2yq_revenue_qtd_2w_prior) l2yq_revenue_qtd_2w_prior
                ,SUM(cq_optimal_qtd_2w_prior) cq_optimal_qtd_2w_prior
                ,SUM(lyq_optimal_qtd_2w_prior) lyq_optimal_qtd_2w_prior
                ,SUM(l14d_optimal_prior) l14d_optimal_prior
                ,SUM(l14d_avg_optimal_prior) l14d_avg_optimal_prior
                ,SUM(l14d_avg_ly_optimal_prior) l14d_avg_ly_optimal_prior
                ,SUM(l14d_ly_optimal_prior) l14d_ly_optimal_prior
                ,SUM(cq_dr_resilient_qtd_2w_prior) cq_dr_resilient_qtd_2w_prior
                ,SUM(lyq_dr_resilient_qtd_2w_prior) lyq_dr_resilient_qtd_2w_prior
                ,SUM(l14d_dr_resilient_prior) l14d_dr_resilient_prior
                ,SUM(l14d_avg_dr_resilient_prior) l14d_avg_dr_resilient_prior
                ,SUM(l14d_avg_ly_dr_resilient_prior) l14d_avg_ly_dr_resilient_prior
                ,SUM(l14d_ly_dr_resilient_prior) l14d_ly_dr_resilient_prior
                ,SUM(cq_product_resilient_rec_rev_qtd_2w_prior) cq_product_resilient_rec_rev_qtd_2w_prior
                ,SUM(cq_ebr_usd_rec_rev_qtd_2w_prior) cq_ebr_usd_rec_rev_qtd_2w_prior
                ,SUM(cq_capi_ebr_revenue_qtd_2w_prior) cq_capi_ebr_revenue_qtd_2w_prior
                ,SUM(l14d_product_resilient_rec_rev_prior) l14d_product_resilient_rec_rev_prior
                ,SUM(l14d_ebr_usd_rec_rev_prior) l14d_ebr_usd_rec_rev_prior
                ,SUM(l14d_capi_ebr_revenue_prior) l14d_capi_ebr_revenue_prior
                ,SUM(l14d_avg_product_resilient_rec_rev_prior) l14d_avg_product_resilient_rec_rev_prior
                ,SUM(l14d_avg_ebr_usd_rec_rev_prior) l14d_avg_ebr_usd_rec_rev_prior
                ,SUM(l14d_avg_capi_ebr_revenue_prior) l14d_avg_capi_ebr_revenue_prior
                ,SUM(l14d_avg_ly_product_resilient_rec_rev_prior) l14d_avg_ly_product_resilient_rec_rev_prior
                ,SUM(l14d_avg_ly_ebr_usd_rec_rev_prior) l14d_avg_ly_ebr_usd_rec_rev_prior
                ,SUM(l14d_avg_ly_capi_ebr_revenue_prior) l14d_avg_ly_capi_ebr_revenue_prior
                ,SUM(l14d_ly_product_resilient_rec_rev_prior) l14d_ly_product_resilient_rec_rev_prior
                ,SUM(l14d_ly_ebr_usd_rec_rev_prior) l14d_ly_ebr_usd_rec_rev_prior
                ,SUM(l14d_ly_capi_ebr_revenue_prior) l14d_ly_capi_ebr_revenue_prior
                ,ds

            from <TABLE:bpo_gms_quota_and_forecast_fast>

            where ds = '<DATEID>'

            group by
              program
             ,revenue_segment
             ,advertiser_vertical
             ,legacy_advertiser_vertical
             ,legacy_advertiser_sub_vertical
             ,ultimate_parent_vertical_name_v2
             ,specialty
             ,advertiser_sub_vertical
             ,cp_username
             ,cp_manager_username
             ,am_username
             ,am_manager_username
             ,pm_username
             ,pm_manager_username
             ,ap_username
             ,ap_manager_username
             ,asofdate
             ,quarter_id
             ,next_quarter_id
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
             ,l12_reporting_terr_mgr
             ,l10_reporting_terr_mgr
             ,l8_reporting_terr_mgr
             ,l6_reporting_terr_mgr
             ,l12_agency_territory
             ,l10_agency_territory
             ,l8_agency_territory
             ,l6_agency_territory
             ,l4_agency_territory
             ,l2_agency_territory
             ,l12_manager_agency_territory
             ,l10_manager_agency_territory
             ,l8_manager_agency_territory
             ,l6_manager_agency_territory
             ,segmentation
             ,csm_username
             ,csm_manager_username
             ,asm_username
             ,asm_manager_username
             ,rp_username
             ,rp_manager_username
             ,sales_adv_country_group
             ,sales_adv_subregion
             ,sales_adv_region
             ,market
             ,advertiser_country
             ,l12_reseller_territory
             ,l10_reseller_territory
             ,l8_reseller_territory
             ,l6_reseller_territory
             ,l4_reseller_territory
             ,l2_reseller_territory
             ,l12_reseller_terr_mgr
             ,l10_reseller_terr_mgr
             ,l8_reseller_terr_mgr
             ,l6_reseller_terr_mgr
             ,subsegment
             ,ts
             ,ds
              """
        self.delete = """
            DELETE FROM <TABLE:bpo_gms_quota_and_forecast_uber_fast>

            where DS <> '<DATEID>'


            """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select

    def get_delete(self):
        """returns a delete statement that will remove all partitions
        from a table that are not '<DATEID>'"""
        return self.delete


class BpoGmsQuotaAndForecastFastDaily:
    """@docstring"""

    def __init__(self):
        self.name = "<TABLE:bpo_gms_quota_and_forecast_fast_daily>"
        self.create = """
            CREATE TABLE IF NOT EXISTS <TABLE:bpo_gms_quota_and_forecast_fast_daily> (
            date_id varchar,
            program varchar,
            revenue_segment varchar,
            advertiser_coverage_model_daa varchar, --to be removed
            advertiser_program_daa varchar, --to be removed
            l12_reporting_territory VARCHAR,
            l10_reporting_territory VARCHAR,
            l8_reporting_territory  VARCHAR,
            l6_reporting_territory  VARCHAR,
            l4_reporting_territory  VARCHAR,
            l2_reporting_territory  VARCHAR,
            l12_reporting_terr_mgr VARCHAR,
            l10_reporting_terr_mgr VARCHAR,
            l8_reporting_terr_mgr VARCHAR,
            l6_reporting_terr_mgr VARCHAR,
            l12_agency_territory VARCHAR,
            l10_agency_territory VARCHAR,
            l8_agency_territory VARCHAR,
            l6_agency_territory VARCHAR,
            l4_agency_territory VARCHAR,
            l2_agency_territory VARCHAR,
            advertiser_vertical varchar,
            cp_username varchar,
            cp_manager_username varchar,
            am_username varchar,
            am_manager_username varchar,
            pm_username varchar,
            pm_manager_username varchar,
            ap_username varchar,
            ap_manager_username varchar,
            asofdate varchar,
            cq_revenue DOUBLE,
            pq_revenue DOUBLE,
            ly_revenue DOUBLE,
            l2y_revenue DOUBLE,
            lyq_revenue DOUBLE,
            l2yq_revenue DOUBLE,
            advertiser_quota double,
            agency_quota DOUBLE,
            sales_forecast DOUBLE,
            sales_forecast_prior DOUBLE,
            segmentation VARCHAR,
            l12_manager_agency_territory VARCHAR,
            l10_manager_agency_territory VARCHAR,
            l8_manager_agency_territory VARCHAR,
            l6_manager_agency_territory VARCHAR,
            csm_username varchar COMMENT 'UnixName for Client Solutions Manager',
            csm_manager_username varchar COMMENT 'UnixName for a CSMs manager ',
            asm_username varchar COMMENT 'UnixName for ASM',
            asm_manager_username varchar COMMENT 'UnixName for a ASMs manager',
            rp_username varchar COMMENT 'UnixName for reseller partner',
            rp_manager_username varchar COMMENT 'UnixName of an RPs manager',
            sales_adv_country_group VARCHAR, --to be removed
            sales_adv_subregion VARCHAR,
            sales_adv_region VARCHAR,
            market VARCHAR,
            advertiser_country VARCHAR,
            l12_reseller_territory VARCHAR,
            l10_reseller_territory VARCHAR,
            l8_reseller_territory  VARCHAR,
            l6_reseller_territory  VARCHAR,
            l4_reseller_territory  VARCHAR,
            l2_reseller_territory  VARCHAR,
            l12_reseller_terr_mgr VARCHAR,
            l10_reseller_terr_mgr VARCHAR,
            l8_reseller_terr_mgr  VARCHAR,
            l6_reseller_terr_mgr  VARCHAR,
            reseller_quota DOUBLE,
            adv_exclusion INT,
            cq_optimal DOUBLE,
            pq_optimal DOUBLE,
            ly_optimal DOUBLE,
            lyq_optimal DOUBLE,
            optimal_quota double,
            agc_optimal_quota DOUBLE,
            ts VARCHAR,
            L7d_revenue DOUBLE,
            L7d_revenue_prior DOUBLE,
            L7d_avg_revenue DOUBLE,
            L7d_avg_revenue_prior DOUBLE,
            legacy_advertiser_vertical VARCHAR,
            ultimate_parent_vertical_name_v2 VARCHAR, -- added on 2020-07-14
            dr_resilience_goal DOUBLE,
            dr_resilient_cq double,
            dr_resilient_pq double,
            dr_resilient_ly double,
            dr_resilient_lyq double,
            cq_product_resilient_rec_rev DOUBLE,
            cq_ebr_usd_rec_rev DOUBLE,
            cq_capi_ebr_revenue DOUBLE,
            pq_product_resilient_rec_rev DOUBLE,
            pq_ebr_usd_rec_rev DOUBLE,
            pq_capi_ebr_revenue DOUBLE,
            ly_product_resilient_rec_rev DOUBLE,
            ly_ebr_usd_rec_rev DOUBLE,
            ly_capi_ebr_revenue DOUBLE,
            lyq_product_resilient_rec_rev DOUBLE,
            lyq_ebr_usd_rec_rev DOUBLE,
            lyq_capi_ebr_revenue DOUBLE,
            subsegment VARCHAR,
            L14d_revenue DOUBLE,
            L14d_revenue_prior DOUBLE,
            L14d_avg_revenue DOUBLE,
            L14d_avg_revenue_prior DOUBLE,
            sales_forecast_prior_2w DOUBLE,
            ds varchar(10)
           )
            WITH (
                    partitioned_by = ARRAY['ds'],
                    retention_days = <RETENTION:90>,
                    uii=false
                   )"""
        self.select = """
           SELECT
                date_id
               ,program
               ,revenue_segment
               ,advertiser_coverage_model_daa
               ,advertiser_program_daa
               ,l12_reporting_territory
               ,l10_reporting_territory
               ,l8_reporting_territory
               ,l6_reporting_territory
               ,l4_reporting_territory
               ,l2_reporting_territory
               ,l12_reporting_terr_mgr
               ,l10_reporting_terr_mgr
               ,l8_reporting_terr_mgr
               ,l6_reporting_terr_mgr
               ,advertiser_vertical
               ,segmentation
               ,cp_username
               ,cp_manager_username
               ,am_username
               ,am_manager_username
               ,pm_username
               ,pm_manager_username
               ,ap_username
               ,ap_manager_username
               ,cast(asofdate as varchar) asofdate
               ,l12_manager_agency_territory
               ,l10_manager_agency_territory
               ,l8_manager_agency_territory
               ,l6_manager_agency_territory
               ,l12_agency_territory
               ,l10_agency_territory
               ,l8_agency_territory
               ,l6_agency_territory
               ,l4_agency_territory
               ,l2_agency_territory
               ,csm_username
               ,csm_manager_username
               ,asm_username
               ,asm_manager_username
               ,rp_username
               ,rp_manager_username
               ,sales_adv_country_group
               ,sales_adv_subregion
               ,sales_adv_region
               ,market
               ,advertiser_country
               ,l12_reseller_territory
               ,l10_reseller_territory
               ,l8_reseller_territory
               ,l6_reseller_territory
               ,l4_reseller_territory
               ,l2_reseller_territory
               ,l12_reseller_terr_mgr
               ,l10_reseller_terr_mgr
               ,l8_reseller_terr_mgr
               ,l6_reseller_terr_mgr
               ,CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts
               ,SUM(reseller_quota) reseller_quota
               ,SUM(cq_revenue) cq_revenue
               ,SUM(pq_revenue) pq_revenue
               ,SUM(ly_revenue) ly_revenue
               ,SUM(l2y_revenue) l2y_revenue
               ,SUM(lyq_revenue) lyq_revenue
               ,SUM(l2yq_revenue) l2yq_revenue
               ,SUM(smb_optimal_quota) smb_optimal_quota
               ,SUM(smb_liquidity_quota) smb_liquidity_quota
               ,SUM(advertiser_quota) advertiser_quota
               ,SUM(agency_quota) agency_quota
               ,SUM(sales_forecast) sales_forecast
               ,SUM(sales_forecast_prior) sales_forecast_prior
               ,CASE WHEN ultimate_parent_sfid in ('001A0000010ydjWIAQ')
                     THEN 1
                     ELSE 0
                END adv_exclusion
                ,SUM(cq_optimal) cq_optimal
                ,SUM(pq_optimal) pq_optimal
                ,SUM(ly_optimal) ly_optimal
                ,SUM(lyq_optimal) lyq_optimal
                ,SUM(optimal_quota) optimal_quota
                ,SUM(agc_optimal_quota) agc_optimal_quota
                ,SUM(L7d_revenue) L7d_revenue
                ,SUM(L7d_revenue_prior) L7d_revenue_prior
                ,SUM(L7d_avg_revenue) L7d_avg_revenue
                ,SUM(L7d_avg_revenue_prior) L7d_avg_revenue_prior
                ,SUM(dr_resilience_goal) dr_resilience_goal
                ,SUM(dr_resilient_cq) dr_resilient_cq
                ,SUM(dr_resilient_pq) dr_resilient_pq
                ,SUM(dr_resilient_ly) dr_resilient_ly
                ,SUM(dr_resilient_lyq) dr_resilient_lyq
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
                ,SUM(L14d_revenue) L14d_revenue
                ,SUM(L14d_revenue_prior) L14d_revenue_prior
                ,SUM(L14d_avg_revenue) L14d_avg_revenue
                ,SUM(L14d_avg_revenue_prior) L14d_avg_revenue_prior
                ,SUM(sales_forecast_prior_2w) sales_forecast_prior_2w
                ,ultimate_parent_vertical_name_v2
                ,legacy_advertiser_vertical
                ,subsegment

           from <TABLE:bpo_gms_quota_and_forecast_snapshot>

             cross join (Select max(cast(date_id as date)) as asofdate from
             <TABLE:bpo_gms_quota_and_forecast_snapshot> where ds ='<DATEID>'
             AND  cq_revenue is not null)

           where ds = '<DATEID>'



           group by
              date_id
             ,program
             ,revenue_segment
             ,advertiser_coverage_model_daa
             ,advertiser_program_daa
             ,l12_reporting_territory
             ,l10_reporting_territory
             ,l8_reporting_territory
             ,l6_reporting_territory
             ,l4_reporting_territory
             ,l2_reporting_territory
             ,l12_reporting_terr_mgr
             ,l10_reporting_terr_mgr
             ,l8_reporting_terr_mgr
             ,l6_reporting_terr_mgr
             ,advertiser_vertical
             ,legacy_advertiser_vertical
             ,ultimate_parent_vertical_name_v2
             ,segmentation
             ,cp_username
             ,cp_manager_username
             ,am_username
             ,am_manager_username
             ,pm_username
             ,pm_manager_username
             ,ap_username
             ,ap_manager_username
             ,cast(asofdate as varchar)
             ,l12_manager_agency_territory
             ,l10_manager_agency_territory
             ,l8_manager_agency_territory
             ,l6_manager_agency_territory
             ,l12_agency_territory
             ,l10_agency_territory
             ,l8_agency_territory
             ,l6_agency_territory
             ,l4_agency_territory
             ,l2_agency_territory
             ,csm_username
             ,csm_manager_username
             ,asm_username
             ,asm_manager_username
             ,rp_username
             ,rp_manager_username
             ,sales_adv_country_group
             ,sales_adv_subregion
             ,sales_adv_region
             ,market
             ,advertiser_country
             ,l12_reseller_territory
             ,l10_reseller_territory
             ,l8_reseller_territory
             ,l6_reseller_territory
             ,l4_reseller_territory
             ,l2_reseller_territory
             ,l12_reseller_terr_mgr
             ,l10_reseller_terr_mgr
             ,l8_reseller_terr_mgr
             ,l6_reseller_terr_mgr
             ,subsegment
             ,ts
             ,CASE WHEN ultimate_parent_sfid in ('001A0000010ydjWIAQ')
                   THEN 1
                   ELSE 0
              END
             """
        self.delete = """
            DELETE FROM <TABLE:bpo_gms_quota_and_forecast_fast_daily>

            where ds <> '<DATEID>' """

    def get_name(self):
        """returns the table name encased in the <TABLE:> macro"""
        return self.name

    def get_create(self):
        """returns the create statement as a string"""
        return self.create

    def get_select(self):
        """returns the select statement as a string"""
        return self.select

    def get_delete(self):
        """returns a delete statement that will remove all partitions
        from a table that are not '<DATEID>'"""
        return self.delete
