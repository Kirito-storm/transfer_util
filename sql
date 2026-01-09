<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="com.amoros.pumpkin.data.adapter.db.clickhouse.AdsWideStoreMonthlyMapper">

    <select id="dropPartition">
        alter table t_ads_wide_store_monthly ON CLUSTER cluster_single_shard drop partition (#{statisTm})
    </select>
    <select id="selectStoreCount" resultType="java.lang.Long">
        select
            count(store.store_id) AS store_id
        from (select
                  *
              from
                  retail_consultant_db.t_ads_store_monthly_stats
              where tenant_id = 'anta'
                and stats_date =#{curStatisTm}
             ) store join ( select
                                *
                            from retail_consultant_db.t_ads_store_scd
                            where tenant_id = 'anta'
        ) scd on store.tenant_id=scd.tenant_id
            and store.store_id=scd.store_id
    </select>

    <select id="selectStorePage" resultType="java.lang.String">
        select
            store.store_id
        from (select
                  *
              from
                  retail_consultant_db.t_ads_store_monthly_stats
              where tenant_id = 'anta'
                and stats_date =#{curStatisTm}
             ) store join ( select
                                *
                            from retail_consultant_db.t_ads_store_scd
                            where tenant_id = 'anta'
        ) scd on store.tenant_id=scd.tenant_id
            and store.store_id=scd.store_id
        limit #{limit} offset #{offset}
    </select>

    <insert id="insertSelectBatch">
        insert into retail_consultant_db.t_ads_wide_store_monthly
        select cur.cms_code,
               cur.brand_store_code,
               cur.temperature_area,
               cur.store_nature,
               cur.store_type_name,
               cur.shopLevel_adjust,
               cur.org_branch_name,
               cur.stats_date,
               #{endStatisTm},
               #{lastDayFlag},
               cur.name,
               cur.address,
               cur.province_name,
               cur.join_type,
               cur.company_type,
               cur.store_id,
               cur.key_store,
               cur.opening_date,
               cur.store_type,
               cur.expand_type,
               cur.store_image_name,
               cur.rack_type,
               cur.channel_brand_code,
               cur.city_name,
               cur.tenant_id,
               cur.is_retail,
               cur.district_name,
               cur.level,
               cur.org_office_name,
               cur.brand_name,
               cur.store_level_name,
               cur.org_area_name,
               cur.business_status,
               cur.new_good_dynamic_rate,
               cur.vip_repurchase_cnt_similar,
               cur.inv_sale_rate_similar,
               cur.upt_similar,
               cur.one_to_seven_dynamic_rate,
               cur.sales_amt_actual_per_staff_similar,
               cur.aur_similar,
               cur.top50_sku_unbroken_rate_similar,
               cur.one_to_seven_dynamic_rate_similar,
               cur.shoes_clothing_sales_qty,
               cur.top50_sales_inv_match_rate_similar,
               cur.write_off_rate_similar,
               cur.thousand_retail_cnt_pct,
               cur.vip_repurchase_duration_similar,
               cur.sales_amt_list_similar,
               cur.retail_cnt,
               cur.top50_match_cnt,
               cur.sales_amt_actual_in_season,
               cur.inside_outside_ratio,
               cur.plural_retail_cnt_similar,
               cur.singular_tot_amt_actual,
               cur.issue_qty,
               cur.numOfPurMember_sc_label,
               cur.top_sales_upt,
               cur.stay_sku_cnt,
               cur.vip_retail_pct,
               cur.shop_area,
               cur.exec_qty,
               cur.gw_retail_amt_pct_similar,
               cur.couponUsageRate_sc_label,
               cur.plural_tot_amt_list_similar,
               cur.sales_amt_actual_similar,
               cur.numOfPurNewMember_sc_label,
               cur.salesOfInSeason_sc_label,
               cur.consume_member_qty,
               cur.avg_unit_inv_list_similar,
               cur.shoes_clothing_retail_cnt,
               cur.retial_cnt_per_staff_similar,
               cur.sales_amt_actual,
               cur.inv_amt,
               cur.singular_retail_cnt_similar,
               cur.write_off_qty,
               cur.top50_art_no_cnt,
               cur.topFullSizeRate_sc_label,
               cur.plural_tot_amt_actual_similar,
               cur.offline_amt_similar,
               cur.large_screen_sale_amt,
               cur.sale_deep_similar,
               cur.plural_tot_amt_actual,
               cur.inside_outside_ratio_similar,
               cur.yun_store_sale_amt_pct_similar,
               cur.stay_sku_cnt_similar,
               cur.sales_sc_label,
               cur.shoes_clothes_ratio,
               cur.matchDegTopProduct_sc_label,
               cur.new_consume_member_qty,
               cur.atv_sc_label,
               cur.consume_member_qty_similar,
               cur.cr_sc_label,
               cur.numOfPurExistMember_sc_label,
               cur.old_consume_member_qty,
               cur.singular_tot_amt_list,
               cur.singular_tot_amt_list_similar,
               cur.upt_excl_acc_similar,
               cur.top_sales_amt_actual_pct_similar,
               cur.top50_sales_amt_actual,
               cur.dynamic_sale_rate_similar,
               cur.total_sku_cnt,
               cur.new_good_dynamic_rate_similar,
               cur.old_consume_member_qty_similar,
               cur.atv_similar,
               cur.exec_rate_similar,
               cur.shoes_qty_ge2_receipt_ratio_similar,
               cur.have_sale_staff_cnt,
               cur.amt_ge1000_receipt_ratio_similar,
               cur.md_sc_label,
               cur.plural_discount_rate_similar,
               cur.discount_rate_diff_similar,
               cur.salesTnumStaffs_sc_label,
               cur.plural_tot_amt_list,
               cur.top50_is_homo_cnt,
               cur.sales_sku_cnt,
               cur.singular_tot_amt_actual_similar,
               cur.upt_sc_label,
               cur.topSalesPct_sc_label,
               cur.vip_repurchase_duration,
               cur.inv_sale_rate,
               cur.two_pairs_shoes_retail_cnt_pct,
               cur.sales_target,
               cur.large_screen_sale_amt_similar,
               cur.singular_retail_cnt,
               cur.vip_retail_pct_similar,
               cur.shoes_clothes_ratio_similar,
               cur.sales_amt_actual_in_season_pct_similar,
               cur.sales_amt_list_in_season,
               cur.retail_cnt_have_customer,
               cur.customer_cnt,
               cur.new_sku_discount_rate_similar,
               cur.gw_retail_amt_similar,
               cur.top_sales_upt_similar,
               cur.inv_qty,
               cur.vip_repurchase_cnt,
               cur.large_screen_sale_amt_pct_similar,
               cur.skuEfficency_sc_label,
               cur.task_tgt_qty,
               cur.yun_store_sale_amt_similar,
               cur.sales_qty_for_upt,
               cur.plural_retail_cnt_pct_similar,
               cur.deal_rate_similar,
               cur.offline_amt_pct_similar,
               cur.plural_retail_cnt,
               cur.new_consume_member_qty_similar,
               cur.yun_store_sale_amt,
               cur.gw_retail_amt,
               cur.sales_qty,
               cur.singular_discount_rate_similar,
               cur.sales_amt_list,
               cur.discount_rate_similar,
               cur.taskExecuteRate_sc_label,
               cur.total_amount_similar,
               pre_month.new_good_dynamic_rate AS new_good_dynamic_rate_mom,
               pre_month.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate_mom,
               pre_month.shoes_clothing_sales_qty AS shoes_clothing_sales_qty_mom,
               pre_month.thousand_retail_cnt_pct AS thousand_retail_cnt_pct_mom,
               pre_month.retail_cnt AS retail_cnt_mom,
               pre_month.top50_match_cnt AS top50_match_cnt_mom,
               pre_month.sales_amt_actual_in_season AS sales_amt_actual_in_season_mom,
               pre_month.inside_outside_ratio AS inside_outside_ratio_mom,
               pre_month.singular_tot_amt_actual AS singular_tot_amt_actual_mom,
               pre_month.issue_qty AS issue_qty_mom,
               pre_month.numOfPurMember_sc_label AS numOfPurMember_sc_label_mom,
               pre_month.top_sales_upt AS top_sales_upt_mom,
               pre_month.stay_sku_cnt AS stay_sku_cnt_mom,
               pre_month.vip_retail_pct AS vip_retail_pct_mom,
               pre_month.shop_area AS shop_area_mom,
               pre_month.exec_qty AS exec_qty_mom,
               pre_month.couponUsageRate_sc_label AS couponUsageRate_sc_label_mom,
               pre_month.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label_mom,
               pre_month.salesOfInSeason_sc_label AS salesOfInSeason_sc_label_mom,
               pre_month.consume_member_qty AS consume_member_qty_mom,
               pre_month.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt_mom,
               pre_month.sales_amt_actual AS sales_amt_actual_mom,
               pre_month.inv_amt AS inv_amt_mom,
               pre_month.write_off_qty AS write_off_qty_mom,
               pre_month.top50_art_no_cnt AS top50_art_no_cnt_mom,
               pre_month.topFullSizeRate_sc_label AS topFullSizeRate_sc_label_mom,
               pre_month.large_screen_sale_amt AS large_screen_sale_amt_mom,
               pre_month.plural_tot_amt_actual AS plural_tot_amt_actual_mom,
               pre_month.sales_sc_label AS sales_sc_label_mom,
               pre_month.shoes_clothes_ratio AS shoes_clothes_ratio_mom,
               pre_month.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label_mom,
               pre_month.new_consume_member_qty AS new_consume_member_qty_mom,
               pre_month.atv_sc_label AS atv_sc_label_mom,
               pre_month.cr_sc_label AS cr_sc_label_mom,
               pre_month.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label_mom,
               pre_month.old_consume_member_qty AS old_consume_member_qty_mom,
               pre_month.singular_tot_amt_list AS singular_tot_amt_list_mom,
               pre_month.top50_sales_amt_actual AS top50_sales_amt_actual_mom,
               pre_month.total_sku_cnt AS total_sku_cnt_mom,
               pre_month.have_sale_staff_cnt AS have_sale_staff_cnt_mom,
               pre_month.md_sc_label AS md_sc_label_mom,
               pre_month.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label_mom,
               pre_month.plural_tot_amt_list AS plural_tot_amt_list_mom,
               pre_month.top50_is_homo_cnt AS top50_is_homo_cnt_mom,
               pre_month.sales_sku_cnt AS sales_sku_cnt_mom,
               pre_month.upt_sc_label AS upt_sc_label_mom,
               pre_month.topSalesPct_sc_label AS topSalesPct_sc_label_mom,
               pre_month.vip_repurchase_duration AS vip_repurchase_duration_mom,
               pre_month.inv_sale_rate AS inv_sale_rate_mom,
               pre_month.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct_mom,
               pre_month.sales_target AS sales_target_mom,
               pre_month.singular_retail_cnt AS singular_retail_cnt_mom,
               pre_month.sales_amt_list_in_season AS sales_amt_list_in_season_mom,
               pre_month.retail_cnt_have_customer AS retail_cnt_have_customer_mom,
               pre_month.customer_cnt AS customer_cnt_mom,
               pre_month.inv_qty AS inv_qty_mom,
               pre_month.vip_repurchase_cnt AS vip_repurchase_cnt_mom,
               pre_month.skuEfficency_sc_label AS skuEfficency_sc_label_mom,
               pre_month.task_tgt_qty AS task_tgt_qty_mom,
               pre_month.sales_qty_for_upt AS sales_qty_for_upt_mom,
               pre_month.plural_retail_cnt AS plural_retail_cnt_mom,
               pre_month.yun_store_sale_amt AS yun_store_sale_amt_mom,
               pre_month.gw_retail_amt AS gw_retail_amt_mom,
               pre_month.sales_qty AS sales_qty_mom,
               pre_month.sales_amt_list AS sales_amt_list_mom,
               pre_month.taskExecuteRate_sc_label AS taskExecuteRate_sc_label_mom,
               pre_year.new_good_dynamic_rate AS new_good_dynamic_rate_yoy,
               pre_year.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate_yoy,
               pre_year.shoes_clothing_sales_qty AS shoes_clothing_sales_qty_yoy,
               pre_year.thousand_retail_cnt_pct AS thousand_retail_cnt_pct_yoy,
               pre_year.retail_cnt AS retail_cnt_yoy,
               pre_year.top50_match_cnt AS top50_match_cnt_yoy,
               pre_year.sales_amt_actual_in_season AS sales_amt_actual_in_season_yoy,
               pre_year.inside_outside_ratio AS inside_outside_ratio_yoy,
               pre_year.singular_tot_amt_actual AS singular_tot_amt_actual_yoy,
               pre_year.issue_qty AS issue_qty_yoy,
               pre_year.numOfPurMember_sc_label AS numOfPurMember_sc_label_yoy,
               pre_year.top_sales_upt AS top_sales_upt_yoy,
               pre_year.stay_sku_cnt AS stay_sku_cnt_yoy,
               pre_year.vip_retail_pct AS vip_retail_pct_yoy,
               pre_year.shop_area AS shop_area_yoy,
               pre_year.exec_qty AS exec_qty_yoy,
               pre_year.couponUsageRate_sc_label AS couponUsageRate_sc_label_yoy,
               pre_year.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label_yoy,
               pre_year.salesOfInSeason_sc_label AS salesOfInSeason_sc_label_yoy,
               pre_year.consume_member_qty AS consume_member_qty_yoy,
               pre_year.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt_yoy,
               pre_year.sales_amt_actual AS sales_amt_actual_yoy,
               pre_year.inv_amt AS inv_amt_yoy,
               pre_year.write_off_qty AS write_off_qty_yoy,
               pre_year.top50_art_no_cnt AS top50_art_no_cnt_yoy,
               pre_year.topFullSizeRate_sc_label AS topFullSizeRate_sc_label_yoy,
               pre_year.large_screen_sale_amt AS large_screen_sale_amt_yoy,
               pre_year.plural_tot_amt_actual AS plural_tot_amt_actual_yoy,
               pre_year.sales_sc_label AS sales_sc_label_yoy,
               pre_year.shoes_clothes_ratio AS shoes_clothes_ratio_yoy,
               pre_year.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label_yoy,
               pre_year.new_consume_member_qty AS new_consume_member_qty_yoy,
               pre_year.atv_sc_label AS atv_sc_label_yoy,
               pre_year.cr_sc_label AS cr_sc_label_yoy,
               pre_year.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label_yoy,
               pre_year.old_consume_member_qty AS old_consume_member_qty_yoy,
               pre_year.singular_tot_amt_list AS singular_tot_amt_list_yoy,
               pre_year.top50_sales_amt_actual AS top50_sales_amt_actual_yoy,
               pre_year.total_sku_cnt AS total_sku_cnt_yoy,
               pre_year.have_sale_staff_cnt AS have_sale_staff_cnt_yoy,
               pre_year.md_sc_label AS md_sc_label_yoy,
               pre_year.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label_yoy,
               pre_year.plural_tot_amt_list AS plural_tot_amt_list_yoy,
               pre_year.top50_is_homo_cnt AS top50_is_homo_cnt_yoy,
               pre_year.sales_sku_cnt AS sales_sku_cnt_yoy,
               pre_year.upt_sc_label AS upt_sc_label_yoy,
               pre_year.topSalesPct_sc_label AS topSalesPct_sc_label_yoy,
               pre_year.vip_repurchase_duration AS vip_repurchase_duration_yoy,
               pre_year.inv_sale_rate AS inv_sale_rate_yoy,
               pre_year.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct_yoy,
               pre_year.sales_target AS sales_target_yoy,
               pre_year.singular_retail_cnt AS singular_retail_cnt_yoy,
               pre_year.sales_amt_list_in_season AS sales_amt_list_in_season_yoy,
               pre_year.retail_cnt_have_customer AS retail_cnt_have_customer_yoy,
               pre_year.customer_cnt AS customer_cnt_yoy,
               pre_year.inv_qty AS inv_qty_yoy,
               pre_year.vip_repurchase_cnt AS vip_repurchase_cnt_yoy,
               pre_year.skuEfficency_sc_label AS skuEfficency_sc_label_yoy,
               pre_year.task_tgt_qty AS task_tgt_qty_yoy,
               pre_year.sales_qty_for_upt AS sales_qty_for_upt_yoy,
               pre_year.plural_retail_cnt AS plural_retail_cnt_yoy,
               pre_year.yun_store_sale_amt AS yun_store_sale_amt_yoy,
               pre_year.gw_retail_amt AS gw_retail_amt_yoy,
               pre_year.sales_qty AS sales_qty_yoy,
               pre_year.sales_amt_list AS sales_amt_list_yoy,
               pre_year.taskExecuteRate_sc_label AS taskExecuteRate_sc_label_yoy,
               cur.top_sales_amt_actual_pct AS top_sales_amt_actual_pct,
               pre_month.top_sales_amt_actual_pct AS top_sales_amt_actual_pct_mom,
               pre_year.top_sales_amt_actual_pct AS top_sales_amt_actual_pct_yoy,
               cur.top20_sales_match_rate AS top20_sales_match_rate,
               pre_month.top20_sales_match_rate AS top20_sales_match_rate_mom,
               pre_year.top20_sales_match_rate AS top20_sales_match_rate_yoy,
               cur.top20_sales_match_rate_similar AS top20_sales_match_rate_similar,
               cur.new_reg_member_qty AS new_reg_member_qty,
               pre_year.new_reg_member_qty AS new_reg_member_qty_yoy,
               pre_month.new_reg_member_qty AS new_reg_member_qty_mom,
               cur.new_reg_member_qty_similar AS new_reg_member_qty_similar,
               cur.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty,
               pre_year.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty_yoy,
               pre_month.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty_mom,
               cur.new_reg_has_consume_member_qty_similar AS new_reg_has_consume_member_qty_similar,
               cur.sku_unbroken_rate AS sku_unbroken_rate,
               pre_year.sku_unbroken_rate AS sku_unbroken_rate_yoy,
               pre_month.sku_unbroken_rate AS sku_unbroken_rate_mom,
               cur.sku_unbroken_rate_similar AS sku_unbroken_rate_similar,
               cur.new_reg_has_consume_ratio_similar AS new_reg_has_consume_ratio_similar,
               cur.high_val_member_consume_member_qty AS high_val_member_consume_member_qty,
               pre_year.high_val_member_consume_member_qty AS high_val_member_consume_member_qty_yoy,
               pre_month.high_val_member_consume_member_qty AS high_val_member_consume_member_qty_mom,
               cur.high_val_member_consume_member_qty_similar AS high_val_member_consume_member_qty_similar,
               cur.birth_issue_qty AS birth_issue_qty,
               pre_year.birth_issue_qty AS birth_issue_qty_yoy,
               pre_month.birth_issue_qty AS birth_issue_qty_mom,
               cur.birth_issue_qty_similar AS birth_issue_qty_similar,
               cur.birth_write_off_qty AS birth_write_off_qty,
               pre_year.birth_write_off_qty AS birth_write_off_qty_yoy,
               pre_month.birth_write_off_qty AS birth_write_off_qty_mom,
               cur.birth_write_off_qty_similar AS birth_write_off_qty_similar,
               cur.birth_write_off_rate_similar AS birth_write_off_rate_similar,
               cur.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty,
               pre_year.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty_yoy,
               pre_month.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty_mom,
               cur.new_reg_non_consume_member_qty_similar AS new_reg_non_consume_member_qty_similar,
               cur.new_reg_non_consume_ratio_similar as new_reg_non_consume_ratio_similar,
               cur.high_val_member_consume_amt AS high_val_member_consume_amt,
               pre_year.high_val_member_consume_amt AS high_val_member_consume_amt_yoy,
               pre_month.high_val_member_consume_amt AS high_val_member_consume_amt_mom,
               cur.high_val_member_consume_amt_similar AS high_val_member_consume_amt_similar,
               cur.high_val_member_order_qty AS high_val_member_order_qty,
               pre_year.high_val_member_order_qty AS high_val_member_order_qty_yoy,
               pre_month.high_val_member_order_qty AS high_val_member_order_qty_mom,
               cur.high_val_member_order_qty_similar AS high_val_member_order_qty_similar,
               cur.high_val_member_atv_similar AS high_val_member_atv_similar,
               cur.curr_month_new_member_order_qty AS curr_month_new_member_order_qty,
               pre_year.curr_month_new_member_order_qty AS curr_month_new_member_order_qty_yoy,
               pre_month.curr_month_new_member_order_qty AS curr_month_new_member_order_qty_mom,
               cur.curr_month_old_member_order_qty AS curr_month_old_member_order_qty,
               pre_year.curr_month_old_member_order_qty AS curr_month_old_member_order_qty_yoy,
               pre_month.curr_month_old_member_order_qty AS curr_month_old_member_order_qty_mom,
               cur.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty,
               pre_year.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty_yoy,
               pre_month.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty_mom,
               cur.barg_new_cust_reg_pct_similar AS barg_new_cust_reg_pct_similar,
               cur.old_member_consume_amt AS old_member_consume_amt,
               pre_year.old_member_consume_amt AS old_member_consume_amt_yoy,
               pre_month.old_member_consume_amt AS old_member_consume_amt_mom,
               cur.old_member_consume_amt_similar AS old_member_consume_amt_similar,
               cur.old_member_order_qty AS old_member_order_qty,
               pre_year.old_member_order_qty AS old_member_order_qty_yoy,
               pre_month.old_member_order_qty AS old_member_order_qty_mom,
               cur.old_member_order_qty_similar AS old_member_order_qty_similar,
               cur.old_member_atv_similar AS old_member_atv_similar,
               cur.new_member_consume_amt AS new_member_consume_amt,
               pre_year.new_member_consume_amt AS new_member_consume_amt_yoy,
               pre_month.new_member_consume_amt AS new_member_consume_amt_mom,
               cur.new_member_consume_amt_similar AS new_member_consume_amt_similar,
               cur.new_member_order_qty AS new_member_order_qty,
               pre_year.new_member_order_qty AS new_member_order_qty_yoy,
               pre_month.new_member_order_qty AS new_member_order_qty_mom,
               cur.new_member_order_qty_similar AS new_member_order_qty_similar,
               cur.new_member_atv_similar AS new_member_atv_similar,
               cur.member_consume_amt AS member_consume_amt,
               pre_year.member_consume_amt AS member_consume_amt_yoy,
               pre_month.member_consume_amt AS member_consume_amt_mom,
               cur.member_consume_amt_similar AS member_consume_amt_similar,
               cur.high_val_member_consume_amt_pct_similar AS high_val_member_consume_amt_pct_similar,
               cur.old_member_consume_amt_pct_similar AS old_member_consume_amt_pct_similar,
               cur.new_member_consume_amt_pct_similar AS new_member_consume_amt_pct_similar,
               cur.new_consume_member_qty_pct_similar AS new_consume_member_qty_pct_similar,
               cur.old_consume_member_qty_pct_similar AS old_consume_member_qty_pct_similar,
               cur.high_val_member_consume_member_qty_pct_similar AS high_val_member_consume_member_qty_pct_similar,
               cur.numOfPurNewMember_target,
               pre_year.numOfPurNewMember_target AS numOfPurNewMember_target_yoy,
               pre_month.numOfPurNewMember_target AS numOfPurNewMember_target_mom,
               cur.numOfPurMember_target,
               pre_year.numOfPurMember_target AS numOfPurMember_target_yoy,
               pre_month.numOfPurMember_target AS numOfPurMember_target_mom,
               cur.numOfPurExistMember_target,
               pre_year.numOfPurExistMember_target AS numOfPurExistMember_target_yoy,
               pre_month.numOfPurExistMember_target AS numOfPurExistMember_target_mom,
               cur.new_reg_member_qty_target,
               pre_year.new_reg_member_qty_target AS new_reg_member_qty_target_yoy,
               pre_month.new_reg_member_qty_target AS new_reg_member_qty_target_mom,
               cur.new_reg_has_consume_member_rate_target,
               pre_year.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target_yoy,
               pre_month.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target_mom,
               cur.high_val_member_consume_member_qty_target,
               pre_year.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target_yoy,
               pre_month.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target_mom,
               pre_year.new_reg_has_consume_ratio AS new_reg_has_consume_ratio_yoy
        from
            (
                select
                    store.cms_code AS cms_code,
                    scd.brand_store_code AS brand_store_code,
                    scd.temperature_area AS temperature_area,
                    scd.store_nature AS store_nature,
                    scd.store_type_name AS store_type_name,
                    scd.shopLevel_adjust AS shopLevel_adjust,
                    scd.org_branch_name AS org_branch_name,
                    store.stats_date AS stats_date,
                    scd.name AS name,
                    scd.address AS address,
                    scd.province_name AS province_name,
                    scd.join_type AS join_type,
                    scd.company_type AS company_type,
                    store.store_id AS store_id,
                    scd.key_store AS key_store,
                    scd.opening_date AS opening_date,
                    scd.store_type AS store_type,
                    scd.expand_type AS expand_type,
                    scd.store_image_name AS store_image_name,
                    scd.rack_type AS rack_type,
                    scd.channel_brand_code AS channel_brand_code,
                    scd.city_name AS city_name,
                    scd.tenant_id AS tenant_id,
                    scd.is_retail AS is_retail,
                    scd.district_name AS district_name,
                    scd.level AS level,
                    scd.org_office_name AS org_office_name,
                    store.brand_name AS brand_name,
                    scd.store_level_name AS store_level_name,
                    scd.org_area_name AS org_area_name,
                    scd.business_status AS business_status,
                    store.new_good_dynamic_rate AS new_good_dynamic_rate,
                    similar.vip_repurchase_cnt_similar AS vip_repurchase_cnt_similar,
                    similar.inv_sale_rate_similar AS inv_sale_rate_similar,
                    similar.upt_similar AS upt_similar,
                    store.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate,
                    similar.sales_amt_actual_per_staff_similar AS sales_amt_actual_per_staff_similar,
                    similar.aur_similar AS aur_similar,
                    similar.top50_sku_unbroken_rate_similar AS top50_sku_unbroken_rate_similar,
                    similar.one_to_seven_dynamic_rate_similar AS one_to_seven_dynamic_rate_similar,
                    store.shoes_clothing_sales_qty AS shoes_clothing_sales_qty,
                    similar.top50_sales_inv_match_rate_similar AS top50_sales_inv_match_rate_similar,
                    coupon_similar.write_off_rate_similar AS write_off_rate_similar,
                    store.thousand_retail_cnt_pct AS thousand_retail_cnt_pct,
                    similar.vip_repurchase_duration_similar AS vip_repurchase_duration_similar,
                    similar.sales_amt_list_similar AS sales_amt_list_similar,
                    store.retail_cnt AS retail_cnt,
                    store.top50_match_cnt AS top50_match_cnt,
                    store.sales_amt_actual_in_season AS sales_amt_actual_in_season,
                    store.inside_outside_ratio AS inside_outside_ratio,
                    similar.plural_retail_cnt_similar AS plural_retail_cnt_similar,
                    store.singular_tot_amt_actual AS singular_tot_amt_actual,
                    member_coupon.issue_qty AS issue_qty,
                    scorecard.numOfPurMember_sc_label AS numOfPurMember_sc_label,
                    store.top_sales_upt AS top_sales_upt,
                    store.stay_sku_cnt AS stay_sku_cnt,
                    store.vip_retail_pct AS vip_retail_pct,
                    scd.shop_area AS shop_area,
                    task.exec_qty AS exec_qty,
                    similar.gw_retail_amt_pct_similar AS gw_retail_amt_pct_similar,
                    scorecard.couponUsageRate_sc_label AS couponUsageRate_sc_label,
                    similar.plural_tot_amt_list_similar AS plural_tot_amt_list_similar,
                    similar.sales_amt_actual_similar AS sales_amt_actual_similar,
                    scorecard.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label,
                    scorecard.salesOfInSeason_sc_label AS salesOfInSeason_sc_label,
                    member.consume_member_qty AS consume_member_qty,
                    similar.avg_unit_inv_list_similar AS avg_unit_inv_list_similar,
                    store.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt,
                    similar.retial_cnt_per_staff_similar AS retial_cnt_per_staff_similar,
                    store.sales_amt_actual AS sales_amt_actual,
                    store.inv_amt AS inv_amt,
                    similar.singular_retail_cnt_similar AS singular_retail_cnt_similar,
                    member_coupon.write_off_qty AS write_off_qty,
                    store.top50_art_no_cnt AS top50_art_no_cnt,
                    scorecard.topFullSizeRate_sc_label AS topFullSizeRate_sc_label,
                    similar.plural_tot_amt_actual_similar AS plural_tot_amt_actual_similar,
                    similar.offline_amt_similar AS offline_amt_similar,
                    store.large_screen_sale_amt AS large_screen_sale_amt,
                    similar.sale_deep_similar AS sale_deep_similar,
                    store.plural_tot_amt_actual AS plural_tot_amt_actual,
                    similar.inside_outside_ratio_similar AS inside_outside_ratio_similar,
                    similar.yun_store_sale_amt_pct_similar AS yun_store_sale_amt_pct_similar,
                    similar.stay_sku_cnt_similar AS stay_sku_cnt_similar,
                    scorecard.sales_sc_label AS sales_sc_label,
                    store.shoes_clothes_ratio AS shoes_clothes_ratio,
                    scorecard.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label,
                    member.new_consume_member_qty AS new_consume_member_qty,
                    scorecard.atv_sc_label AS atv_sc_label,
                    member_similar.consume_member_qty_similar AS consume_member_qty_similar,
                    scorecard.cr_sc_label AS cr_sc_label,
                    scorecard.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label,
                    member.old_consume_member_qty AS old_consume_member_qty,
                    store.singular_tot_amt_list AS singular_tot_amt_list,
                    similar.singular_tot_amt_list_similar AS singular_tot_amt_list_similar,
                    similar.upt_excl_acc_similar AS upt_excl_acc_similar,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top50_sales_amt_actual AS top50_sales_amt_actual,
                    similar.dynamic_sale_rate_similar AS dynamic_sale_rate_similar,
                    store.total_sku_cnt AS total_sku_cnt,
                    similar.new_good_dynamic_rate_similar AS new_good_dynamic_rate_similar,
                    member_similar.old_consume_member_qty_similar AS old_consume_member_qty_similar,
                    similar.atv_similar AS atv_similar,
                    task_similar.exec_rate_similar AS exec_rate_similar,
                    similar.shoes_qty_ge2_receipt_ratio_similar AS shoes_qty_ge2_receipt_ratio_similar,
                    store.have_sale_staff_cnt AS have_sale_staff_cnt,
                    similar.amt_ge1000_receipt_ratio_similar AS amt_ge1000_receipt_ratio_similar,
                    scorecard.md_sc_label AS md_sc_label,
                    similar.plural_discount_rate_similar AS plural_discount_rate_similar,
                    similar.discount_rate_diff_similar AS discount_rate_diff_similar,
                    scorecard.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label,
                    store.plural_tot_amt_list AS plural_tot_amt_list,
                    store.top50_is_homo_cnt AS top50_is_homo_cnt,
                    store.sales_sku_cnt AS sales_sku_cnt,
                    similar.singular_tot_amt_actual_similar AS singular_tot_amt_actual_similar,
                    scorecard.upt_sc_label AS upt_sc_label,
                    scorecard.topSalesPct_sc_label AS topSalesPct_sc_label,
                    store.vip_repurchase_duration AS vip_repurchase_duration,
                    store.inv_sale_rate AS inv_sale_rate,
                    store.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct,
                    feature_target.sales_target AS sales_target,
                    similar.large_screen_sale_amt_similar AS large_screen_sale_amt_similar,
                    store.singular_retail_cnt AS singular_retail_cnt,
                    similar.vip_retail_pct_similar AS vip_retail_pct_similar,
                    similar.shoes_clothes_ratio_similar AS shoes_clothes_ratio_similar,
                    similar.sales_amt_actual_in_season_pct_similar AS sales_amt_actual_in_season_pct_similar,
                    store.sales_amt_list_in_season AS sales_amt_list_in_season,
                    store.retail_cnt_have_customer AS retail_cnt_have_customer,
                    store.customer_cnt AS customer_cnt,
                    similar.new_sku_discount_rate_similar AS new_sku_discount_rate_similar,
                    similar.gw_retail_amt_similar AS gw_retail_amt_similar,
                    similar.top_sales_upt_similar AS top_sales_upt_similar,
                    store.inv_qty AS inv_qty,
                    store.vip_repurchase_cnt AS vip_repurchase_cnt,
                    similar.large_screen_sale_amt_pct_similar AS large_screen_sale_amt_pct_similar,
                    scorecard.skuEfficency_sc_label AS skuEfficency_sc_label,
                    task.task_tgt_qty AS task_tgt_qty,
                    similar.yun_store_sale_amt_similar AS yun_store_sale_amt_similar,
                    store.sales_qty_for_upt AS sales_qty_for_upt,
                    similar.plural_retail_cnt_pct_similar AS plural_retail_cnt_pct_similar,
                    similar.deal_rate_similar AS deal_rate_similar,
                    similar.offline_amt_pct_similar AS offline_amt_pct_similar,
                    store.plural_retail_cnt AS plural_retail_cnt,
                    member_similar.new_consume_member_qty_similar AS new_consume_member_qty_similar,
                    store.yun_store_sale_amt AS yun_store_sale_amt,
                    store.gw_retail_amt AS gw_retail_amt,
                    store.sales_qty AS sales_qty,
                    similar.singular_discount_rate_similar AS singular_discount_rate_similar,
                    store.sales_amt_list AS sales_amt_list,
                    similar.discount_rate_similar AS discount_rate_similar,
                    scorecard.taskExecuteRate_sc_label AS taskExecuteRate_sc_label,
                    similar.total_amount_similar AS total_amount_similar,
                    store.top_sales_amt_actual_pct AS top_sales_amt_actual_pct,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top20_sales_match_rate AS top20_sales_match_rate,
                    similar.top20_match_rate_similar AS top20_sales_match_rate_similar,
                    member.new_reg_member_qty AS new_reg_member_qty,
                    member_similar.new_reg_member_qty_similar AS new_reg_member_qty_similar,
                    member.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty,
                    member_similar.new_reg_has_consume_member_qty_similar AS new_reg_has_consume_member_qty_similar,
                    store.sku_unbroken_rate AS sku_unbroken_rate,
                    similar.sku_unbroken_rate_similar AS sku_unbroken_rate_similar,
                    member_similar.new_reg_has_consume_member_rate_similar AS new_reg_has_consume_ratio_similar,
                    member.high_val_member_consume_member_qty AS high_val_member_consume_member_qty,
                    member_similar.high_val_member_consume_member_qty_similar AS high_val_member_consume_member_qty_similar,
                    birth_member_coupon.issue_qty AS birth_issue_qty,
                    birth_coupon_similar.issue_qty_similar AS birth_issue_qty_similar,
                    birth_member_coupon.write_off_qty AS birth_write_off_qty,
                    birth_coupon_similar.write_off_qty_similar AS birth_write_off_qty_similar,
                    birth_coupon_similar.write_off_rate_similar AS birth_write_off_rate_similar,
                    member.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty,
                    member_similar.new_reg_non_consume_member_qty_similar AS new_reg_non_consume_member_qty_similar,
                    member_similar.new_reg_non_consume_member_rate_similar as new_reg_non_consume_ratio_similar,
                    member.high_val_member_consume_amt AS high_val_member_consume_amt,
                    member_similar.high_val_member_consume_amt_similar AS high_val_member_consume_amt_similar,
                    member.high_val_member_order_qty AS high_val_member_order_qty,
                    member_similar.high_val_member_order_qty_similar AS high_val_member_order_qty_similar,
                    member_similar.high_val_member_atv_similar AS high_val_member_atv_similar,
                    member.curr_month_new_member_order_qty AS curr_month_new_member_order_qty,
                    member.curr_month_old_member_order_qty AS curr_month_old_member_order_qty,
                    member.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty,
                    member_similar.barg_new_cust_reg_pct_similar AS barg_new_cust_reg_pct_similar,
                    member.old_member_consume_amt AS old_member_consume_amt,
                    member_similar.old_member_consume_amt_similar AS old_member_consume_amt_similar,
                    member.old_member_order_qty AS old_member_order_qty,
                    member_similar.old_member_order_qty_similar AS old_member_order_qty_similar,
                    member_similar.old_member_atv_similar AS old_member_atv_similar,
                    member.new_member_consume_amt AS new_member_consume_amt,
                    member_similar.new_member_consume_amt_similar AS new_member_consume_amt_similar,
                    member.new_member_order_qty AS new_member_order_qty,
                    member_similar.new_member_order_qty_similar AS new_member_order_qty_similar,
                    member_similar.new_member_atv_similar AS new_member_atv_similar,
                    member.member_consume_amt AS member_consume_amt,
                    member_similar.member_consume_amt_similar AS member_consume_amt_similar,
                    member_similar.high_val_member_consume_amt_pct_similar AS high_val_member_consume_amt_pct_similar,
                    member_similar.old_member_consume_amt_pct_similar AS old_member_consume_amt_pct_similar,
                    member_similar.new_member_consume_amt_pct_similar AS new_member_consume_amt_pct_similar,
                    member_similar.new_consume_member_qty_pct_similar AS new_consume_member_qty_pct_similar,
                    member_similar.old_consume_member_qty_pct_similar AS old_consume_member_qty_pct_similar,
                    member_similar.high_val_member_consume_member_qty_pct_similar AS high_val_member_consume_member_qty_pct_similar,
                    feature_target.numOfPurNewMember_target AS numOfPurNewMember_target,
                    feature_target.numOfPurMember_target AS numOfPurMember_target,
                    feature_target.numOfPurExistMember_target AS numOfPurExistMember_target,
                    feature_target.new_reg_member_qty_target AS new_reg_member_qty_target,
                    feature_target.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target,
                    feature_target.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target
        from (select
                          *
                      from
                          retail_consultant_db.t_ads_store_monthly_stats
                      where tenant_id = 'anta'
                        and stats_date = #{curStatisTm}
                      and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                     ) store join ( select
                                        *
                                    from retail_consultant_db.t_ads_store_scd
                                    where tenant_id = 'anta'
                                      and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                ) scd on store.tenant_id=scd.tenant_id
                    and store.store_id=scd.store_id
                             left outer join (select tenant_id, rel_id store_id, stats_date,brand_name,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'sales' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 5)   end)  sales_target,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurNewMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurNewMember_target,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurMember_target,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurExistMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurExistMember_target,
                                                    null as new_reg_member_qty_target,
                                                    null as new_reg_has_consume_member_rate_target,
                                                    null as high_val_member_consume_member_qty_target
                                             from
                                                  retail_consultant_db.t_sc_feature_month_target_info
                                              where tenant_id = 'anta'
                                                and stats_date = #{curStatisTm}
                                                and rel_type='STORE' and del_flag=0
                                                and rel_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by tenant_id,rel_id,stats_date,brand_name
                ) feature_target on store.tenant_id=feature_target.tenant_id
                    and store.store_id=feature_target.store_id
                    and toDate(store.stats_date)=feature_target.stats_date
                    and store.brand_name=feature_target.brand_name
                             left outer join (select
                                                  tenant_id,
                                                  region_id,
                                                  calc_begin_date,
                                                  brand_name,
                                                  max(case when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurExistMember_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  matchDegTopProduct_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurNewMember_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  topFullSizeRate_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  couponUsageRate_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  taskExecuteRate_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesTnumStaffs_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesOfInSeason_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurMember_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  skuEfficency_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'ANOMALY' then '异常' end)  topSalesPct_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  sales_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  atv_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  upt_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  cr_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  md_sc_label
                                              from retail_consultant_db.t_ads_feature_score_card feature_scorecard
                                              where tenant_id = 'anta'
                                                and region_type='STORE'
                                                and cycle = 'MONTH'
                                                and calc_begin_date = #{curStatisTm}
                                                and region_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by tenant_id,region_id,calc_begin_date,brand_name
                ) scorecard on store.tenant_id=scorecard.tenant_id
                    and store.store_id=scorecard.region_id
                    and toDate(store.stats_date)=scorecard.calc_begin_date
                    and store.brand_name=scorecard.brand_name
                             left outer join (select
                                                  member.tenant_id tenant_id,
                                                  member.stats_date stats_date,
                                                  member.store_id store_id,
                                                  member.brand_name brand_name,
                                                  sum(member.consume_member_qty) consume_member_qty,
                                                  sum(member.old_consume_member_qty) old_consume_member_qty,
                                                  sum(member.new_consume_member_qty) new_consume_member_qty,
                                                  sum(member.new_reg_member_qty) as new_reg_member_qty,
                                                  sum(member.new_reg_has_consume_member_qty) new_reg_has_consume_member_qty,
                                                  sum(member.high_val_member_consume_member_qty) high_val_member_consume_member_qty,
                                                  sum(member.new_reg_non_consume_member_qty) new_reg_non_consume_member_qty,
                                                  sum(member.high_val_member_consume_amt) high_val_member_consume_amt,
                                                  sum(member.high_val_member_order_qty) high_val_member_order_qty,
                                                  sum(member.curr_month_new_member_order_qty) curr_month_new_member_order_qty,
                                                  sum(member.curr_month_old_member_order_qty) curr_month_old_member_order_qty,
                                                  sum(member.curr_month_total_barg_order_qty) curr_month_total_barg_order_qty,
                                                  sum(member.old_member_consume_amt) old_member_consume_amt,
                                                  sum(member.old_member_order_qty) old_member_order_qty,
                                                  sum(member.new_member_consume_amt) new_member_consume_amt,
                                                  sum(member.new_member_order_qty) new_member_order_qty,
                                                  sum(member.member_consume_amt) member_consume_amt
                                              from retail_consultant_db.t_ads_crm_mco_store_statis member
                                              where tenant_id = 'anta'
                                                and stats_date = #{curStatisTm}
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by tenant_id,stats_date,store_id,brand_name
                ) member on store.tenant_id=member.tenant_id
                    and store.store_id=member.store_id
                    and store.stats_date=member.stats_date
                    and store.brand_name=member.brand_name
                             left outer join (select
                                                  member_coupon.brand_name brand_name,
                                                  member_coupon.store_id store_id,
                                                  member_coupon.tenant_id tenant_id,
                                                  member_coupon.stats_date stats_date,
                                                  sum(member_coupon.issue_qty) issue_qty,
                                                  sum(member_coupon.write_off_qty) write_off_qty
                                              from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                              where tenant_id = 'anta'
                                                and stats_date = #{curStatisTm}
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by brand_name,store_id,tenant_id,stats_date
                ) member_coupon on store.tenant_id=member_coupon.tenant_id
                    and store.store_id=member_coupon.store_id
                    and store.stats_date=member_coupon.stats_date
                    and store.brand_name=member_coupon.brand_name
                             left outer join (select
                                                  member_coupon.brand_name brand_name,
                                                  member_coupon.store_id store_id,
                                                  member_coupon.tenant_id tenant_id,
                                                  member_coupon.stats_date stats_date,
                                                  sum(member_coupon.issue_qty) issue_qty,
                                                  sum(member_coupon.write_off_qty) write_off_qty
                                              from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                              where tenant_id = 'anta'
                                                and stats_date = #{curStatisTm} and coupon_categ_desc = '生日券'
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by brand_name,store_id,tenant_id,stats_date
                ) birth_member_coupon on store.tenant_id=birth_member_coupon.tenant_id
                    and store.store_id=birth_member_coupon.store_id
                    and store.stats_date=birth_member_coupon.stats_date
                    and store.brand_name=birth_member_coupon.brand_name
                             left outer join (select
                                                  task.stats_date stats_date,
                                                  task.tenant_id tenant_id,
                                                  task.store_id store_id,
                                                  task.brand_name brand_name,
                                                  sum(task.exec_qty) exec_qty,
                                                  sum(
                                                          case
                                                              when task.task_type_desc = 'single_talk' then task.task_tgt_qty
                                                              else 0
                                                              end
                                                  ) task_tgt_qty_single_talk,
                                                  toInt32(
                                                          countDistinctIf(guide_id, task.task_type_desc = 'friends_area')
                                                  ) guide_count,
                                                  date_diff('day', toDate(#{curStatisTm}), toDate(#{endStatisTm})) + 1 days,
                                                  guide_count * days * 3 task_tgt_qty_friends_area,
                                                  task_tgt_qty_single_talk + task_tgt_qty_friends_area task_tgt_qty
                                              from retail_consultant_db.t_ads_crm_mco_guide_task_statis task
                                              where true
                                                and tenant_id = 'anta'
                                                and stats_date = #{curStatisTm}
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by stats_date,tenant_id,store_id,brand_name
                ) task on store.tenant_id=task.tenant_id
                    and store.store_id=task.store_id
                    and store.stats_date=task.stats_date
                    and store.brand_name=task.brand_name
                             left outer join (select *
                                              from
                                                  retail_consultant_db.t_ads_similar_store_monthly_stats
                                              where tenant_id = 'anta'
                                                and stats_date = #{curStatisTm}
                ) similar
                                             on  store.tenant_id = similar.tenant_id
                                                 and  store.store_type = similar.store_type
                                                 and  store.stats_date = similar.stats_date
                                                 and  store.brand_name = similar.brand_name
                             left outer join (select *
                                              from
                                                  retail_consultant_db.dm_crm_mco_store_statis_similar
                                              where statis_tm = #{curStatisTm} and store_type_class != ''
                ) member_similar
                                             on   store.store_type = member_similar.store_type_class
                                                 and  store.stats_date = member_similar.statis_tm
                                                 and  store.brand_name = member_similar.brand_name
                             left outer join (select
                                                  *
                                              from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                              where statis_tm = #{curStatisTm} and store_type_class != ''
                                              and coupon_categ_desc = 'ALL'
                ) coupon_similar on store.store_type = coupon_similar.store_type_class
                    and  store.stats_date = coupon_similar.statis_tm
                    and  store.brand_name = coupon_similar.brand_name
                    left outer join (select
                                        statis_tm,
                                        brand_name,
                                        sum(issue_qty_similar) as issue_qty_similar,
                                        sum(write_off_qty_similar) as write_off_qty_similar,
                                        case when sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) != 0 then sum(dm_crm_mco_coupon_statis_similar.write_off_qty_similar)/sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) end as write_off_rate_similar
        from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                        where statis_tm = #{curStatisTm} and store_type_class != ''
                                        and coupon_categ_desc = '生日券'
                                        group by statis_tm, brand_name
                    ) birth_coupon_similar on store.store_type = coupon_similar.store_type_class
                    and  store.stats_date = birth_coupon_similar.statis_tm
                    and  store.brand_name = birth_coupon_similar.brand_name
                             left outer join (select
                                                  *
                                              from retail_consultant_db.dm_crm_mco_guide_task_statis_similar
                                              where true
                                                and statis_tm = #{curStatisTm} and store_type_class != ''
                                                and task_type_desc = 'ALL' AND task_status = 'ALL'
                ) task_similar
                                             on   store.store_type = task_similar.store_type_class
                                                 and  store.stats_date = task_similar.statis_tm
                                                 and  store.brand_name = task_similar.brand_name

            ) cur left join
            (
                select
                    store.cms_code AS cms_code,
                    scd.brand_store_code AS brand_store_code,
                    scd.temperature_area AS temperature_area,
                    scd.store_nature AS store_nature,
                    scd.store_type_name AS store_type_name,
                    scd.shopLevel_adjust AS shopLevel_adjust,
                    scd.org_branch_name AS org_branch_name,
                    store.stats_date AS stats_date,
                    scd.name AS name,
                    scd.address AS address,
                    scd.province_name AS province_name,
                    scd.join_type AS join_type,
                    scd.company_type AS company_type,
                    store.store_id AS store_id,
                    scd.key_store AS key_store,
                    scd.opening_date AS opening_date,
                    scd.store_type AS store_type,
                    scd.expand_type AS expand_type,
                    scd.store_image_name AS store_image_name,
                    scd.rack_type AS rack_type,
                    scd.channel_brand_code AS channel_brand_code,
                    scd.city_name AS city_name,
                    scd.tenant_id AS tenant_id,
                    scd.is_retail AS is_retail,
                    scd.district_name AS district_name,
                    scd.level AS level,
                    scd.org_office_name AS org_office_name,
                    store.brand_name AS brand_name,
                    scd.store_level_name AS store_level_name,
                    scd.org_area_name AS org_area_name,
                    scd.business_status AS business_status,
                    store.new_good_dynamic_rate AS new_good_dynamic_rate,
                    similar.vip_repurchase_cnt_similar AS vip_repurchase_cnt_similar,
                    similar.inv_sale_rate_similar AS inv_sale_rate_similar,
                    similar.upt_similar AS upt_similar,
                    store.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate,
                    similar.sales_amt_actual_per_staff_similar AS sales_amt_actual_per_staff_similar,
                    similar.aur_similar AS aur_similar,
                    similar.top50_sku_unbroken_rate_similar AS top50_sku_unbroken_rate_similar,
                    similar.one_to_seven_dynamic_rate_similar AS one_to_seven_dynamic_rate_similar,
                    store.shoes_clothing_sales_qty AS shoes_clothing_sales_qty,
                    similar.top50_sales_inv_match_rate_similar AS top50_sales_inv_match_rate_similar,
                    coupon_similar.write_off_rate_similar AS write_off_rate_similar,
                    store.thousand_retail_cnt_pct AS thousand_retail_cnt_pct,
                    similar.vip_repurchase_duration_similar AS vip_repurchase_duration_similar,
                    similar.sales_amt_list_similar AS sales_amt_list_similar,
                    store.retail_cnt AS retail_cnt,
                    store.top50_match_cnt AS top50_match_cnt,
                    store.sales_amt_actual_in_season AS sales_amt_actual_in_season,
                    store.inside_outside_ratio AS inside_outside_ratio,
                    similar.plural_retail_cnt_similar AS plural_retail_cnt_similar,
                    store.singular_tot_amt_actual AS singular_tot_amt_actual,
                    member_coupon.issue_qty AS issue_qty,
                    scorecard.numOfPurMember_sc_label AS numOfPurMember_sc_label,
                    store.top_sales_upt AS top_sales_upt,
                    store.stay_sku_cnt AS stay_sku_cnt,
                    store.vip_retail_pct AS vip_retail_pct,
                    scd.shop_area AS shop_area,
                    task.exec_qty AS exec_qty,
                    similar.gw_retail_amt_pct_similar AS gw_retail_amt_pct_similar,
                    scorecard.couponUsageRate_sc_label AS couponUsageRate_sc_label,
                    similar.plural_tot_amt_list_similar AS plural_tot_amt_list_similar,
                    similar.sales_amt_actual_similar AS sales_amt_actual_similar,
                    scorecard.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label,
                    scorecard.salesOfInSeason_sc_label AS salesOfInSeason_sc_label,
                    member.consume_member_qty AS consume_member_qty,
                    similar.avg_unit_inv_list_similar AS avg_unit_inv_list_similar,
                    store.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt,
                    similar.retial_cnt_per_staff_similar AS retial_cnt_per_staff_similar,
                    store.sales_amt_actual AS sales_amt_actual,
                    store.inv_amt AS inv_amt,
                    similar.singular_retail_cnt_similar AS singular_retail_cnt_similar,
                    member_coupon.write_off_qty AS write_off_qty,
                    store.top50_art_no_cnt AS top50_art_no_cnt,
                    scorecard.topFullSizeRate_sc_label AS topFullSizeRate_sc_label,
                    similar.plural_tot_amt_actual_similar AS plural_tot_amt_actual_similar,
                    similar.offline_amt_similar AS offline_amt_similar,
                    store.large_screen_sale_amt AS large_screen_sale_amt,
                    similar.sale_deep_similar AS sale_deep_similar,
                    store.plural_tot_amt_actual AS plural_tot_amt_actual,
                    similar.inside_outside_ratio_similar AS inside_outside_ratio_similar,
                    similar.yun_store_sale_amt_pct_similar AS yun_store_sale_amt_pct_similar,
                    similar.stay_sku_cnt_similar AS stay_sku_cnt_similar,
                    scorecard.sales_sc_label AS sales_sc_label,
                    store.shoes_clothes_ratio AS shoes_clothes_ratio,
                    scorecard.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label,
                    member.new_consume_member_qty AS new_consume_member_qty,
                    scorecard.atv_sc_label AS atv_sc_label,
                    member_similar.consume_member_qty_similar AS consume_member_qty_similar,
                    scorecard.cr_sc_label AS cr_sc_label,
                    scorecard.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label,
                    member.old_consume_member_qty AS old_consume_member_qty,
                    store.singular_tot_amt_list AS singular_tot_amt_list,
                    similar.singular_tot_amt_list_similar AS singular_tot_amt_list_similar,
                    similar.upt_excl_acc_similar AS upt_excl_acc_similar,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top50_sales_amt_actual AS top50_sales_amt_actual,
                    similar.dynamic_sale_rate_similar AS dynamic_sale_rate_similar,
                    store.total_sku_cnt AS total_sku_cnt,
                    similar.new_good_dynamic_rate_similar AS new_good_dynamic_rate_similar,
                    member_similar.old_consume_member_qty_similar AS old_consume_member_qty_similar,
                    similar.atv_similar AS atv_similar,
                    task_similar.exec_rate_similar AS exec_rate_similar,
                    similar.shoes_qty_ge2_receipt_ratio_similar AS shoes_qty_ge2_receipt_ratio_similar,
                    store.have_sale_staff_cnt AS have_sale_staff_cnt,
                    similar.amt_ge1000_receipt_ratio_similar AS amt_ge1000_receipt_ratio_similar,
                    scorecard.md_sc_label AS md_sc_label,
                    similar.plural_discount_rate_similar AS plural_discount_rate_similar,
                    similar.discount_rate_diff_similar AS discount_rate_diff_similar,
                    scorecard.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label,
                    store.plural_tot_amt_list AS plural_tot_amt_list,
                    store.top50_is_homo_cnt AS top50_is_homo_cnt,
                    store.sales_sku_cnt AS sales_sku_cnt,
                    similar.singular_tot_amt_actual_similar AS singular_tot_amt_actual_similar,
                    scorecard.upt_sc_label AS upt_sc_label,
                    scorecard.topSalesPct_sc_label AS topSalesPct_sc_label,
                    store.vip_repurchase_duration AS vip_repurchase_duration,
                    store.inv_sale_rate AS inv_sale_rate,
                    store.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct,
                    feature_target.sales_target AS sales_target,
                    similar.large_screen_sale_amt_similar AS large_screen_sale_amt_similar,
                    store.singular_retail_cnt AS singular_retail_cnt,
                    similar.vip_retail_pct_similar AS vip_retail_pct_similar,
                    similar.shoes_clothes_ratio_similar AS shoes_clothes_ratio_similar,
                    similar.sales_amt_actual_in_season_pct_similar AS sales_amt_actual_in_season_pct_similar,
                    store.sales_amt_list_in_season AS sales_amt_list_in_season,
                    store.retail_cnt_have_customer AS retail_cnt_have_customer,
                    store.customer_cnt AS customer_cnt,
                    similar.new_sku_discount_rate_similar AS new_sku_discount_rate_similar,
                    similar.gw_retail_amt_similar AS gw_retail_amt_similar,
                    similar.top_sales_upt_similar AS top_sales_upt_similar,
                    store.inv_qty AS inv_qty,
                    store.vip_repurchase_cnt AS vip_repurchase_cnt,
                    similar.large_screen_sale_amt_pct_similar AS large_screen_sale_amt_pct_similar,
                    scorecard.skuEfficency_sc_label AS skuEfficency_sc_label,
                    task.task_tgt_qty AS task_tgt_qty,
                    similar.yun_store_sale_amt_similar AS yun_store_sale_amt_similar,
                    store.sales_qty_for_upt AS sales_qty_for_upt,
                    similar.plural_retail_cnt_pct_similar AS plural_retail_cnt_pct_similar,
                    similar.deal_rate_similar AS deal_rate_similar,
                    similar.offline_amt_pct_similar AS offline_amt_pct_similar,
                    store.plural_retail_cnt AS plural_retail_cnt,
                    member_similar.new_consume_member_qty_similar AS new_consume_member_qty_similar,
                    store.yun_store_sale_amt AS yun_store_sale_amt,
                    store.gw_retail_amt AS gw_retail_amt,
                    store.sales_qty AS sales_qty,
                    similar.singular_discount_rate_similar AS singular_discount_rate_similar,
                    store.sales_amt_list AS sales_amt_list,
                    similar.discount_rate_similar AS discount_rate_similar,
                    scorecard.taskExecuteRate_sc_label AS taskExecuteRate_sc_label,
                    similar.total_amount_similar AS total_amount_similar,
                    store.top_sales_amt_actual_pct AS top_sales_amt_actual_pct,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top20_sales_match_rate AS top20_sales_match_rate,
                    similar.top20_match_rate_similar AS top20_sales_match_rate_similar,
                    member.new_reg_member_qty AS new_reg_member_qty,
                    member_similar.new_reg_member_qty_similar AS new_reg_member_qty_similar,
                    member.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty,
                    member_similar.new_reg_has_consume_member_qty_similar AS new_reg_has_consume_member_qty_similar,
                    store.sku_unbroken_rate AS sku_unbroken_rate,
                    similar.sku_unbroken_rate_similar AS sku_unbroken_rate_similar,
                    member_similar.new_reg_has_consume_member_rate_similar AS new_reg_has_consume_ratio_similar,
                    member.high_val_member_consume_member_qty AS high_val_member_consume_member_qty,
                    member_similar.high_val_member_consume_member_qty_similar AS high_val_member_consume_member_qty_similar,
                    birth_member_coupon.issue_qty AS birth_issue_qty,
                    birth_coupon_similar.issue_qty_similar AS birth_issue_qty_similar,
                    birth_member_coupon.write_off_qty AS birth_write_off_qty,
                    birth_coupon_similar.write_off_qty_similar AS birth_write_off_qty_similar,
                    birth_coupon_similar.write_off_rate_similar AS birth_write_off_rate_similar,
                    member.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty,
                    member_similar.new_reg_non_consume_member_qty_similar AS new_reg_non_consume_member_qty_similar,
                    member_similar.new_reg_non_consume_member_rate_similar as new_reg_non_consume_ratio_similar,
                    member.high_val_member_consume_amt AS high_val_member_consume_amt,
                    member_similar.high_val_member_consume_amt_similar AS high_val_member_consume_amt_similar,
                    member.high_val_member_order_qty AS high_val_member_order_qty,
                    member_similar.high_val_member_order_qty_similar AS high_val_member_order_qty_similar,
                    member_similar.high_val_member_atv_similar AS high_val_member_atv_similar,
                    member.curr_month_new_member_order_qty AS curr_month_new_member_order_qty,
                    member.curr_month_old_member_order_qty AS curr_month_old_member_order_qty,
                    member.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty,
                    member_similar.barg_new_cust_reg_pct_similar AS barg_new_cust_reg_pct_similar,
                    member.old_member_consume_amt AS old_member_consume_amt,
                    member_similar.old_member_consume_amt_similar AS old_member_consume_amt_similar,
                    member.old_member_order_qty AS old_member_order_qty,
                    member_similar.old_member_order_qty_similar AS old_member_order_qty_similar,
                    member_similar.old_member_atv_similar AS old_member_atv_similar,
                    member.new_member_consume_amt AS new_member_consume_amt,
                    member_similar.new_member_consume_amt_similar AS new_member_consume_amt_similar,
                    member.new_member_order_qty AS new_member_order_qty,
                    member_similar.new_member_order_qty_similar AS new_member_order_qty_similar,
                    member_similar.new_member_atv_similar AS new_member_atv_similar,
                    member.member_consume_amt AS member_consume_amt,
                    member_similar.member_consume_amt_similar AS member_consume_amt_similar,
                    member_similar.high_val_member_consume_amt_pct_similar AS high_val_member_consume_amt_pct_similar,
                    member_similar.old_member_consume_amt_pct_similar AS old_member_consume_amt_pct_similar,
                    member_similar.new_member_consume_amt_pct_similar AS new_member_consume_amt_pct_similar,
                    member_similar.new_consume_member_qty_pct_similar AS new_consume_member_qty_pct_similar,
                    member_similar.old_consume_member_qty_pct_similar AS old_consume_member_qty_pct_similar,
                    member_similar.high_val_member_consume_member_qty_pct_similar AS high_val_member_consume_member_qty_pct_similar,
                    feature_target.numOfPurNewMember_target AS numOfPurNewMember_target,
                    feature_target.numOfPurMember_target AS numOfPurMember_target,
                    feature_target.numOfPurExistMember_target AS numOfPurExistMember_target,
                    feature_target.new_reg_member_qty_target AS new_reg_member_qty_target,
                    feature_target.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target,
                    feature_target.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target
        from (select
                          *
                      from
                          retail_consultant_db.t_ads_store_monthly_stats
                      where tenant_id = 'anta'
                        and stats_date = #{preMonthStatisTm}
                        and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                     ) store join ( select
                                        *
                                    from retail_consultant_db.t_ads_store_scd
                                    where tenant_id = 'anta'
                                      and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                ) scd on store.tenant_id=scd.tenant_id
                    and store.store_id=scd.store_id
                             left outer join (select tenant_id, rel_id store_id, stats_date,brand_name,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'sales' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 5)   end)  sales_target,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurNewMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurNewMember_target,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurMember_target,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurExistMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurExistMember_target,
                                                    null as new_reg_member_qty_target,
                                                    null as new_reg_has_consume_member_rate_target,
                                                    null as high_val_member_consume_member_qty_target
                                            from
                                                  retail_consultant_db.t_sc_feature_month_target_info
                                              where tenant_id = 'anta'
                                                and stats_date = #{preMonthStatisTm}
                                                and rel_type='STORE' and del_flag=0
                                                and rel_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by tenant_id,rel_id,stats_date,brand_name
                ) feature_target on store.tenant_id=feature_target.tenant_id
                    and store.store_id=feature_target.store_id
                    and toDate(store.stats_date)=feature_target.stats_date
                    and store.brand_name=feature_target.brand_name
                             left outer join (select
                                                  tenant_id,
                                                  region_id,
                                                  calc_begin_date,
                                                  brand_name,
                                                  max(case when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurExistMember_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  matchDegTopProduct_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurNewMember_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  topFullSizeRate_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  couponUsageRate_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  taskExecuteRate_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesTnumStaffs_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesOfInSeason_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurMember_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  skuEfficency_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'ANOMALY' then '异常' end)  topSalesPct_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  sales_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  atv_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  upt_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  cr_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  md_sc_label
                                              from retail_consultant_db.t_ads_feature_score_card feature_scorecard
                                              where tenant_id = 'anta'
                                                and region_type='STORE'
                                                and cycle = 'MONTH'
                                                and calc_begin_date = #{preMonthStatisTm}
                                                and region_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by tenant_id,region_id,calc_begin_date,brand_name
                ) scorecard on store.tenant_id=scorecard.tenant_id
                    and store.store_id=scorecard.region_id
                    and toDate(store.stats_date)=scorecard.calc_begin_date
                    and store.brand_name=scorecard.brand_name
                             left outer join (select
                                                  member.tenant_id tenant_id,
                                                  member.stats_date stats_date,
                                                  member.store_id store_id,
                                                  member.brand_name brand_name,
                                                  sum(member.consume_member_qty) consume_member_qty,
                                                  sum(member.old_consume_member_qty) old_consume_member_qty,
                                                  sum(member.new_consume_member_qty) new_consume_member_qty,
                                                  sum(member.new_reg_member_qty) as new_reg_member_qty,
                                                  sum(member.new_reg_has_consume_member_qty) new_reg_has_consume_member_qty,
                                                  sum(member.high_val_member_consume_member_qty) high_val_member_consume_member_qty,
                                                  sum(member.new_reg_non_consume_member_qty) new_reg_non_consume_member_qty,
                                                  sum(member.high_val_member_consume_amt) high_val_member_consume_amt,
                                                  sum(member.high_val_member_order_qty) high_val_member_order_qty,
                                                  sum(member.curr_month_new_member_order_qty) curr_month_new_member_order_qty,
                                                  sum(member.curr_month_old_member_order_qty) curr_month_old_member_order_qty,
                                                  sum(member.curr_month_total_barg_order_qty) curr_month_total_barg_order_qty,
                                                  sum(member.old_member_consume_amt) old_member_consume_amt,
                                                  sum(member.old_member_order_qty) old_member_order_qty,
                                                  sum(member.new_member_consume_amt) new_member_consume_amt,
                                                  sum(member.new_member_order_qty) new_member_order_qty,
                                                  sum(member.member_consume_amt) member_consume_amt
                                              from retail_consultant_db.t_ads_crm_mco_store_statis member
                                              where tenant_id = 'anta'
                                                and stats_date = #{preMonthStatisTm}
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by tenant_id,stats_date,store_id,brand_name
                ) member on store.tenant_id=member.tenant_id
                    and store.store_id=member.store_id
                    and store.stats_date=member.stats_date
                    and store.brand_name=member.brand_name
                             left outer join (select
                                                  member_coupon.brand_name brand_name,
                                                  member_coupon.store_id store_id,
                                                  member_coupon.tenant_id tenant_id,
                                                  member_coupon.stats_date stats_date,
                                                  sum(member_coupon.issue_qty) issue_qty,
                                                  sum(member_coupon.write_off_qty) write_off_qty
                                              from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                              where tenant_id = 'anta'
                                                and stats_date = #{preMonthStatisTm}
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by brand_name,store_id,tenant_id,stats_date
                ) member_coupon on store.tenant_id=member_coupon.tenant_id
                    and store.store_id=member_coupon.store_id
                    and store.stats_date=member_coupon.stats_date
                    and store.brand_name=member_coupon.brand_name
                             left outer join (select
                                                  member_coupon.brand_name brand_name,
                                                  member_coupon.store_id store_id,
                                                  member_coupon.tenant_id tenant_id,
                                                  member_coupon.stats_date stats_date,
                                                  sum(member_coupon.issue_qty) issue_qty,
                                                  sum(member_coupon.write_off_qty) write_off_qty
                                              from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                              where tenant_id = 'anta'
                                                and stats_date = #{preMonthStatisTm} and coupon_categ_desc = '生日券'
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by brand_name,store_id,tenant_id,stats_date
                ) birth_member_coupon on store.tenant_id=birth_member_coupon.tenant_id
                    and store.store_id=birth_member_coupon.store_id
                    and store.stats_date=birth_member_coupon.stats_date
                    and store.brand_name=birth_member_coupon.brand_name
                             left outer join (select
                                                  task.stats_date stats_date,
                                                  task.tenant_id tenant_id,
                                                  task.store_id store_id,
                                                  task.brand_name brand_name,
                                                  sum(task.exec_qty) exec_qty,
                                                  sum(
                                                          case
                                                              when task.task_type_desc = 'single_talk' then task.task_tgt_qty
                                                              else 0
                                                              end
                                                  ) task_tgt_qty_single_talk,
                                                  toInt32(
                                                          countDistinctIf(guide_id, task.task_type_desc = 'friends_area')
                                                  ) guide_count,
                                                  date_diff('day', toDate(#{preMonthStatisTm}), toDate(#{endPreMonthStatisTm})) + 1 days,
                                                  guide_count * days * 3 task_tgt_qty_friends_area,
                                                  task_tgt_qty_single_talk + task_tgt_qty_friends_area task_tgt_qty
                                              from retail_consultant_db.t_ads_crm_mco_guide_task_statis task
                                              where true
                                                and tenant_id = 'anta'
                                                and stats_date = #{preMonthStatisTm}
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by stats_date,tenant_id,store_id,brand_name
                ) task on store.tenant_id=task.tenant_id
                    and store.store_id=task.store_id
                    and store.stats_date=task.stats_date
                    and store.brand_name=task.brand_name
                             left outer join (select *
                                              from
                                                  retail_consultant_db.t_ads_similar_store_monthly_stats
                                              where tenant_id = 'anta'
                                                and stats_date = #{preMonthStatisTm}
                ) similar
                                             on  store.tenant_id = similar.tenant_id
                                                 and  store.store_type = similar.store_type
                                                 and  store.stats_date = similar.stats_date
                                                 and  store.brand_name = similar.brand_name
                             left outer join (select *
                                              from
                                                  retail_consultant_db.dm_crm_mco_store_statis_similar
                                              where statis_tm = #{preMonthStatisTm} and store_type_class != ''
                ) member_similar
                                             on   store.store_type = member_similar.store_type_class
                                                 and  store.stats_date = member_similar.statis_tm
                                                 and  store.brand_name = member_similar.brand_name
                             left outer join (select
                                                  *
                                              from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                              where statis_tm = #{preMonthStatisTm} and store_type_class != ''
                                              and coupon_categ_desc = 'ALL'
                ) coupon_similar on store.store_type = coupon_similar.store_type_class
                    and  store.stats_date = coupon_similar.statis_tm
                    and  store.brand_name = coupon_similar.brand_name
                    left outer join (select
                                        statis_tm,
                                        brand_name,
                                        sum(issue_qty_similar) as issue_qty_similar,
                                        sum(write_off_qty_similar) as write_off_qty_similar,
        case when sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) != 0 then sum(dm_crm_mco_coupon_statis_similar.write_off_qty_similar)/sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) end as write_off_rate_similar
        from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                        where statis_tm = #{preMonthStatisTm} and store_type_class != ''
                                        and coupon_categ_desc = '生日券'
                                        group by statis_tm, brand_name
                    ) birth_coupon_similar on store.store_type = coupon_similar.store_type_class
                    and  store.stats_date = birth_coupon_similar.statis_tm
                    and  store.brand_name = birth_coupon_similar.brand_name
                             left outer join (select
                                                  *
                                              from retail_consultant_db.dm_crm_mco_guide_task_statis_similar
                                              where true
                                                and statis_tm = #{preMonthStatisTm} and store_type_class != ''
                                                and task_type_desc = 'ALL' AND task_status = 'ALL'
                ) task_similar
                                             on   store.store_type = task_similar.store_type_class
                                                 and  store.stats_date = task_similar.statis_tm
                                                 and  store.brand_name = task_similar.brand_name

            ) pre_month on cur.tenant_id=pre_month.tenant_id
                and cur.store_id=pre_month.store_id
                and cur.brand_name=pre_month.brand_name
                  left join
            (
                select
                    store.cms_code AS cms_code,
                    scd.brand_store_code AS brand_store_code,
                    scd.temperature_area AS temperature_area,
                    scd.store_nature AS store_nature,
                    scd.store_type_name AS store_type_name,
                    scd.shopLevel_adjust AS shopLevel_adjust,
                    scd.org_branch_name AS org_branch_name,
                    store.stats_date AS stats_date,
                    scd.name AS name,
                    scd.address AS address,
                    scd.province_name AS province_name,
                    scd.join_type AS join_type,
                    scd.company_type AS company_type,
                    store.store_id AS store_id,
                    scd.key_store AS key_store,
                    scd.opening_date AS opening_date,
                    scd.store_type AS store_type,
                    scd.expand_type AS expand_type,
                    scd.store_image_name AS store_image_name,
                    scd.rack_type AS rack_type,
                    scd.channel_brand_code AS channel_brand_code,
                    scd.city_name AS city_name,
                    scd.tenant_id AS tenant_id,
                    scd.is_retail AS is_retail,
                    scd.district_name AS district_name,
                    scd.level AS level,
                    scd.org_office_name AS org_office_name,
                    store.brand_name AS brand_name,
                    scd.store_level_name AS store_level_name,
                    scd.org_area_name AS org_area_name,
                    scd.business_status AS business_status,
                    store.new_good_dynamic_rate AS new_good_dynamic_rate,
                    similar.vip_repurchase_cnt_similar AS vip_repurchase_cnt_similar,
                    similar.inv_sale_rate_similar AS inv_sale_rate_similar,
                    similar.upt_similar AS upt_similar,
                    store.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate,
                    similar.sales_amt_actual_per_staff_similar AS sales_amt_actual_per_staff_similar,
                    similar.aur_similar AS aur_similar,
                    similar.top50_sku_unbroken_rate_similar AS top50_sku_unbroken_rate_similar,
                    similar.one_to_seven_dynamic_rate_similar AS one_to_seven_dynamic_rate_similar,
                    store.shoes_clothing_sales_qty AS shoes_clothing_sales_qty,
                    similar.top50_sales_inv_match_rate_similar AS top50_sales_inv_match_rate_similar,
                    coupon_similar.write_off_rate_similar AS write_off_rate_similar,
                    store.thousand_retail_cnt_pct AS thousand_retail_cnt_pct,
                    similar.vip_repurchase_duration_similar AS vip_repurchase_duration_similar,
                    similar.sales_amt_list_similar AS sales_amt_list_similar,
                    store.retail_cnt AS retail_cnt,
                    store.top50_match_cnt AS top50_match_cnt,
                    store.sales_amt_actual_in_season AS sales_amt_actual_in_season,
                    store.inside_outside_ratio AS inside_outside_ratio,
                    similar.plural_retail_cnt_similar AS plural_retail_cnt_similar,
                    store.singular_tot_amt_actual AS singular_tot_amt_actual,
                    member_coupon.issue_qty AS issue_qty,
                    scorecard.numOfPurMember_sc_label AS numOfPurMember_sc_label,
                    store.top_sales_upt AS top_sales_upt,
                    store.stay_sku_cnt AS stay_sku_cnt,
                    store.vip_retail_pct AS vip_retail_pct,
                    scd.shop_area AS shop_area,
                    task.exec_qty AS exec_qty,
                    similar.gw_retail_amt_pct_similar AS gw_retail_amt_pct_similar,
                    scorecard.couponUsageRate_sc_label AS couponUsageRate_sc_label,
                    similar.plural_tot_amt_list_similar AS plural_tot_amt_list_similar,
                    similar.sales_amt_actual_similar AS sales_amt_actual_similar,
                    scorecard.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label,
                    scorecard.salesOfInSeason_sc_label AS salesOfInSeason_sc_label,
                    member.consume_member_qty AS consume_member_qty,
                    similar.avg_unit_inv_list_similar AS avg_unit_inv_list_similar,
                    store.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt,
                    similar.retial_cnt_per_staff_similar AS retial_cnt_per_staff_similar,
                    store.sales_amt_actual AS sales_amt_actual,
                    store.inv_amt AS inv_amt,
                    similar.singular_retail_cnt_similar AS singular_retail_cnt_similar,
                    member_coupon.write_off_qty AS write_off_qty,
                    store.top50_art_no_cnt AS top50_art_no_cnt,
                    scorecard.topFullSizeRate_sc_label AS topFullSizeRate_sc_label,
                    similar.plural_tot_amt_actual_similar AS plural_tot_amt_actual_similar,
                    similar.offline_amt_similar AS offline_amt_similar,
                    store.large_screen_sale_amt AS large_screen_sale_amt,
                    similar.sale_deep_similar AS sale_deep_similar,
                    store.plural_tot_amt_actual AS plural_tot_amt_actual,
                    similar.inside_outside_ratio_similar AS inside_outside_ratio_similar,
                    similar.yun_store_sale_amt_pct_similar AS yun_store_sale_amt_pct_similar,
                    similar.stay_sku_cnt_similar AS stay_sku_cnt_similar,
                    scorecard.sales_sc_label AS sales_sc_label,
                    store.shoes_clothes_ratio AS shoes_clothes_ratio,
                    scorecard.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label,
                    member.new_consume_member_qty AS new_consume_member_qty,
                    scorecard.atv_sc_label AS atv_sc_label,
                    member_similar.consume_member_qty_similar AS consume_member_qty_similar,
                    scorecard.cr_sc_label AS cr_sc_label,
                    scorecard.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label,
                    member.old_consume_member_qty AS old_consume_member_qty,
                    store.singular_tot_amt_list AS singular_tot_amt_list,
                    similar.singular_tot_amt_list_similar AS singular_tot_amt_list_similar,
                    similar.upt_excl_acc_similar AS upt_excl_acc_similar,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top50_sales_amt_actual AS top50_sales_amt_actual,
                    similar.dynamic_sale_rate_similar AS dynamic_sale_rate_similar,
                    store.total_sku_cnt AS total_sku_cnt,
                    similar.new_good_dynamic_rate_similar AS new_good_dynamic_rate_similar,
                    member_similar.old_consume_member_qty_similar AS old_consume_member_qty_similar,
                    similar.atv_similar AS atv_similar,
                    task_similar.exec_rate_similar AS exec_rate_similar,
                    similar.shoes_qty_ge2_receipt_ratio_similar AS shoes_qty_ge2_receipt_ratio_similar,
                    store.have_sale_staff_cnt AS have_sale_staff_cnt,
                    similar.amt_ge1000_receipt_ratio_similar AS amt_ge1000_receipt_ratio_similar,
                    scorecard.md_sc_label AS md_sc_label,
                    similar.plural_discount_rate_similar AS plural_discount_rate_similar,
                    similar.discount_rate_diff_similar AS discount_rate_diff_similar,
                    scorecard.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label,
                    store.plural_tot_amt_list AS plural_tot_amt_list,
                    store.top50_is_homo_cnt AS top50_is_homo_cnt,
                    store.sales_sku_cnt AS sales_sku_cnt,
                    similar.singular_tot_amt_actual_similar AS singular_tot_amt_actual_similar,
                    scorecard.upt_sc_label AS upt_sc_label,
                    scorecard.topSalesPct_sc_label AS topSalesPct_sc_label,
                    store.vip_repurchase_duration AS vip_repurchase_duration,
                    store.inv_sale_rate AS inv_sale_rate,
                    store.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct,
                    feature_target.sales_target AS sales_target,
                    similar.large_screen_sale_amt_similar AS large_screen_sale_amt_similar,
                    store.singular_retail_cnt AS singular_retail_cnt,
                    similar.vip_retail_pct_similar AS vip_retail_pct_similar,
                    similar.shoes_clothes_ratio_similar AS shoes_clothes_ratio_similar,
                    similar.sales_amt_actual_in_season_pct_similar AS sales_amt_actual_in_season_pct_similar,
                    store.sales_amt_list_in_season AS sales_amt_list_in_season,
                    store.retail_cnt_have_customer AS retail_cnt_have_customer,
                    store.customer_cnt AS customer_cnt,
                    similar.new_sku_discount_rate_similar AS new_sku_discount_rate_similar,
                    similar.gw_retail_amt_similar AS gw_retail_amt_similar,
                    similar.top_sales_upt_similar AS top_sales_upt_similar,
                    store.inv_qty AS inv_qty,
                    store.vip_repurchase_cnt AS vip_repurchase_cnt,
                    similar.large_screen_sale_amt_pct_similar AS large_screen_sale_amt_pct_similar,
                    scorecard.skuEfficency_sc_label AS skuEfficency_sc_label,
                    task.task_tgt_qty AS task_tgt_qty,
                    similar.yun_store_sale_amt_similar AS yun_store_sale_amt_similar,
                    store.sales_qty_for_upt AS sales_qty_for_upt,
                    similar.plural_retail_cnt_pct_similar AS plural_retail_cnt_pct_similar,
                    similar.deal_rate_similar AS deal_rate_similar,
                    similar.offline_amt_pct_similar AS offline_amt_pct_similar,
                    store.plural_retail_cnt AS plural_retail_cnt,
                    member_similar.new_consume_member_qty_similar AS new_consume_member_qty_similar,
                    store.yun_store_sale_amt AS yun_store_sale_amt,
                    store.gw_retail_amt AS gw_retail_amt,
                    store.sales_qty AS sales_qty,
                    similar.singular_discount_rate_similar AS singular_discount_rate_similar,
                    store.sales_amt_list AS sales_amt_list,
                    similar.discount_rate_similar AS discount_rate_similar,
                    scorecard.taskExecuteRate_sc_label AS taskExecuteRate_sc_label,
                    similar.total_amount_similar AS total_amount_similar,
                    store.top_sales_amt_actual_pct AS top_sales_amt_actual_pct,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top20_sales_match_rate AS top20_sales_match_rate,
                    similar.top20_match_rate_similar AS top20_sales_match_rate_similar,
                    member.new_reg_member_qty AS new_reg_member_qty,
                    member_similar.new_reg_member_qty_similar AS new_reg_member_qty_similar,
                    member.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty,
                    member_similar.new_reg_has_consume_member_qty_similar AS new_reg_has_consume_member_qty_similar,
                    store.sku_unbroken_rate AS sku_unbroken_rate,
                    similar.sku_unbroken_rate_similar AS sku_unbroken_rate_similar,
                    member_similar.new_reg_has_consume_member_rate_similar AS new_reg_has_consume_ratio_similar,
                    member.high_val_member_consume_member_qty AS high_val_member_consume_member_qty,
                    member_similar.high_val_member_consume_member_qty_similar AS high_val_member_consume_member_qty_similar,
                    birth_member_coupon.issue_qty AS birth_issue_qty,
                    birth_coupon_similar.issue_qty_similar AS birth_issue_qty_similar,
                    birth_member_coupon.write_off_qty AS birth_write_off_qty,
                    birth_coupon_similar.write_off_qty_similar AS birth_write_off_qty_similar,
                    birth_coupon_similar.write_off_rate_similar AS birth_write_off_rate_similar,
                    member.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty,
                    member_similar.new_reg_non_consume_member_qty_similar AS new_reg_non_consume_member_qty_similar,
                    member_similar.new_reg_non_consume_member_rate_similar as new_reg_non_consume_ratio_similar,
                    member.high_val_member_consume_amt AS high_val_member_consume_amt,
                    member_similar.high_val_member_consume_amt_similar AS high_val_member_consume_amt_similar,
                    member.high_val_member_order_qty AS high_val_member_order_qty,
                    member_similar.high_val_member_order_qty_similar AS high_val_member_order_qty_similar,
                    member_similar.high_val_member_atv_similar AS high_val_member_atv_similar,
                    member.curr_month_new_member_order_qty AS curr_month_new_member_order_qty,
                    member.curr_month_old_member_order_qty AS curr_month_old_member_order_qty,
                    member.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty,
                    member_similar.barg_new_cust_reg_pct_similar AS barg_new_cust_reg_pct_similar,
                    member.old_member_consume_amt AS old_member_consume_amt,
                    member_similar.old_member_consume_amt_similar AS old_member_consume_amt_similar,
                    member.old_member_order_qty AS old_member_order_qty,
                    member_similar.old_member_order_qty_similar AS old_member_order_qty_similar,
                    member_similar.old_member_atv_similar AS old_member_atv_similar,
                    member.new_member_consume_amt AS new_member_consume_amt,
                    member_similar.new_member_consume_amt_similar AS new_member_consume_amt_similar,
                    member.new_member_order_qty AS new_member_order_qty,
                    member_similar.new_member_order_qty_similar AS new_member_order_qty_similar,
                    member_similar.new_member_atv_similar AS new_member_atv_similar,
                    member.member_consume_amt AS member_consume_amt,
                    member_similar.member_consume_amt_similar AS member_consume_amt_similar,
                    member_similar.high_val_member_consume_amt_pct_similar AS high_val_member_consume_amt_pct_similar,
                    member_similar.old_member_consume_amt_pct_similar AS old_member_consume_amt_pct_similar,
                    member_similar.new_member_consume_amt_pct_similar AS new_member_consume_amt_pct_similar,
                    member_similar.new_consume_member_qty_pct_similar AS new_consume_member_qty_pct_similar,
                    member_similar.old_consume_member_qty_pct_similar AS old_consume_member_qty_pct_similar,
                    member_similar.high_val_member_consume_member_qty_pct_similar AS high_val_member_consume_member_qty_pct_similar,
                    feature_target.numOfPurNewMember_target AS numOfPurNewMember_target,
                    feature_target.numOfPurMember_target AS numOfPurMember_target,
                    feature_target.numOfPurExistMember_target AS numOfPurExistMember_target,
                    feature_target.new_reg_member_qty_target AS new_reg_member_qty_target,
                    feature_target.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target,
                    feature_target.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target,
                    member.new_reg_has_consume_rate AS new_reg_has_consume_ratio
        from (select
                          *
                      from
                          retail_consultant_db.t_ads_store_monthly_stats
                      where tenant_id = 'anta'
                        and stats_date = #{preYearStatisTm}
                        and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                     ) store join ( select
                                        *
                                    from retail_consultant_db.t_ads_store_scd
                                    where tenant_id = 'anta'
                                      and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                ) scd on store.tenant_id=scd.tenant_id
                    and store.store_id=scd.store_id
                             left outer join (select tenant_id, rel_id store_id, stats_date,brand_name,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'sales' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 5)   end)  sales_target,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurNewMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurNewMember_target,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurMember_target,
                                                    max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurExistMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurExistMember_target,
                                                    null as new_reg_member_qty_target,
                                                    null as new_reg_has_consume_member_rate_target,
                                                    null as high_val_member_consume_member_qty_target
                                              from
                                                  retail_consultant_db.t_sc_feature_month_target_info
                                              where tenant_id = 'anta'
                                                and stats_date = #{preYearStatisTm}
                                                and rel_type='STORE' and del_flag=0
                                                and rel_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by tenant_id,rel_id,stats_date,brand_name
                ) feature_target on store.tenant_id=feature_target.tenant_id
                    and store.store_id=feature_target.store_id
                    and toDate(store.stats_date)=feature_target.stats_date
                    and store.brand_name=feature_target.brand_name
                             left outer join (select
                                                  tenant_id,
                                                  region_id,
                                                  calc_begin_date,
                                                  brand_name,
                                                  max(case when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurExistMember_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  matchDegTopProduct_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurNewMember_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  topFullSizeRate_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  couponUsageRate_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  taskExecuteRate_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesTnumStaffs_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesOfInSeason_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurMember_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  skuEfficency_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'ANOMALY' then '异常' end)  topSalesPct_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  sales_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  atv_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  upt_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  cr_sc_label,
                                                  max(case when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  md_sc_label
                                              from retail_consultant_db.t_ads_feature_score_card feature_scorecard
                                              where tenant_id = 'anta'
                                                and region_type='STORE'
                                                and cycle = 'MONTH'
                                                and calc_begin_date = #{preYearStatisTm}
                                                and region_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by tenant_id,region_id,calc_begin_date,brand_name
                ) scorecard on store.tenant_id=scorecard.tenant_id
                    and store.store_id=scorecard.region_id
                    and toDate(store.stats_date)=scorecard.calc_begin_date
                    and store.brand_name=scorecard.brand_name
                             left outer join (select
                                                  member.tenant_id tenant_id,
                                                  member.stats_date stats_date,
                                                  member.store_id store_id,
                                                  member.brand_name brand_name,
                                                  sum(member.consume_member_qty) consume_member_qty,
                                                  sum(member.old_consume_member_qty) old_consume_member_qty,
                                                  sum(member.new_consume_member_qty) new_consume_member_qty,
                                                  sum(member.new_reg_member_qty) as new_reg_member_qty,
                                                  sum(member.new_reg_has_consume_member_qty) new_reg_has_consume_member_qty,
                                                  sum(member.high_val_member_consume_member_qty) high_val_member_consume_member_qty,
                                                  sum(member.new_reg_non_consume_member_qty) new_reg_non_consume_member_qty,
                                                  sum(member.high_val_member_consume_amt) high_val_member_consume_amt,
                                                  sum(member.high_val_member_order_qty) high_val_member_order_qty,
                                                  sum(member.curr_month_new_member_order_qty) curr_month_new_member_order_qty,
                                                  sum(member.curr_month_old_member_order_qty) curr_month_old_member_order_qty,
                                                  sum(member.curr_month_total_barg_order_qty) curr_month_total_barg_order_qty,
                                                  sum(member.old_member_consume_amt) old_member_consume_amt,
                                                  sum(member.old_member_order_qty) old_member_order_qty,
                                                  sum(member.new_member_consume_amt) new_member_consume_amt,
                                                  sum(member.new_member_order_qty) new_member_order_qty,
                                                  sum(member.member_consume_amt) member_consume_amt,
                                                  avg(member.new_reg_has_consume_rate) new_reg_has_consume_rate
                                              from retail_consultant_db.t_ads_crm_mco_store_statis member
                                              where tenant_id = 'anta'
                                                and stats_date = #{preYearStatisTm}
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by tenant_id,stats_date,store_id,brand_name
                ) member on store.tenant_id=member.tenant_id
                    and store.store_id=member.store_id
                    and store.stats_date=member.stats_date
                    and store.brand_name=member.brand_name
                             left outer join (select
                                                  member_coupon.brand_name brand_name,
                                                  member_coupon.store_id store_id,
                                                  member_coupon.tenant_id tenant_id,
                                                  member_coupon.stats_date stats_date,
                                                  sum(member_coupon.issue_qty) issue_qty,
                                                  sum(member_coupon.write_off_qty) write_off_qty
                                              from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                              where tenant_id = 'anta'
                                                and stats_date = #{preYearStatisTm}
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by brand_name,store_id,tenant_id,stats_date
                ) member_coupon on store.tenant_id=member_coupon.tenant_id
                    and store.store_id=member_coupon.store_id
                    and store.stats_date=member_coupon.stats_date
                    and store.brand_name=member_coupon.brand_name
                             left outer join (select
                                                  member_coupon.brand_name brand_name,
                                                  member_coupon.store_id store_id,
                                                  member_coupon.tenant_id tenant_id,
                                                  member_coupon.stats_date stats_date,
                                                  sum(member_coupon.issue_qty) issue_qty,
                                                  sum(member_coupon.write_off_qty) write_off_qty
                                              from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                              where tenant_id = 'anta'
                                                and stats_date = #{preYearStatisTm} and coupon_categ_desc = '生日券'
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by brand_name,store_id,tenant_id,stats_date
                ) birth_member_coupon on store.tenant_id=birth_member_coupon.tenant_id
                    and store.store_id=birth_member_coupon.store_id
                    and store.stats_date=birth_member_coupon.stats_date
                    and store.brand_name=birth_member_coupon.brand_name
                             left outer join (select
                                                  task.stats_date stats_date,
                                                  task.tenant_id tenant_id,
                                                  task.store_id store_id,
                                                  task.brand_name brand_name,
                                                  sum(task.exec_qty) exec_qty,
                                                  sum(
                                                          case
                                                              when task.task_type_desc = 'single_talk' then task.task_tgt_qty
                                                              else 0
                                                              end
                                                  ) task_tgt_qty_single_talk,
                                                  toInt32(
                                                          countDistinctIf(guide_id, task.task_type_desc = 'friends_area')
                                                  ) guide_count,
                                                  date_diff('day', toDate(#{preYearStatisTm}), toDate(#{endPreYearStatisTm})) + 1 days,
                                                  guide_count * days * 3 task_tgt_qty_friends_area,
                                                  task_tgt_qty_single_talk + task_tgt_qty_friends_area task_tgt_qty
                                              from retail_consultant_db.t_ads_crm_mco_guide_task_statis task
                                              where true
                                                and tenant_id = 'anta'
                                                and stats_date = #{preYearStatisTm}
                                                and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                              group by stats_date,tenant_id,store_id,brand_name
                ) task on store.tenant_id=task.tenant_id
                    and store.store_id=task.store_id
                    and store.stats_date=task.stats_date
                    and store.brand_name=task.brand_name
                             left outer join (select *
                                              from
                                                  retail_consultant_db.t_ads_similar_store_monthly_stats
                                              where tenant_id = 'anta'
                                                and stats_date = #{preYearStatisTm}
                ) similar
                                             on  store.tenant_id = similar.tenant_id
                                                 and  store.store_type = similar.store_type
                                                 and  store.stats_date = similar.stats_date
                                                 and  store.brand_name = similar.brand_name
                             left outer join (select *
                                              from
                                                  retail_consultant_db.dm_crm_mco_store_statis_similar
                                              where statis_tm = #{preYearStatisTm} and store_type_class != ''
                ) member_similar
                                             on   store.store_type = member_similar.store_type_class
                                                 and  store.stats_date = member_similar.statis_tm
                                                 and  store.brand_name = member_similar.brand_name
                             left outer join (select
                                                  *
                                              from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                              where statis_tm = #{preYearStatisTm} and store_type_class != ''
                                              and coupon_categ_desc = 'ALL'
                ) coupon_similar on store.store_type = coupon_similar.store_type_class
                    and  store.stats_date = coupon_similar.statis_tm
                    and  store.brand_name = coupon_similar.brand_name
                    left outer join (select
                                        statis_tm,
                                        brand_name,
                                        sum(issue_qty_similar) as issue_qty_similar,
                                        sum(write_off_qty_similar) as write_off_qty_similar,
        case when sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) != 0 then sum(dm_crm_mco_coupon_statis_similar.write_off_qty_similar)/sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) end as write_off_rate_similar
        from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                        where statis_tm = #{preYearStatisTm} and store_type_class != ''
                                        and coupon_categ_desc = '生日券'
                                        group by statis_tm, brand_name
                    ) birth_coupon_similar on store.store_type = coupon_similar.store_type_class
                    and  store.stats_date = birth_coupon_similar.statis_tm
                    and  store.brand_name = birth_coupon_similar.brand_name
                             left outer join (select
                                                  *
                                              from retail_consultant_db.dm_crm_mco_guide_task_statis_similar
                                              where true
                                                and statis_tm = #{preYearStatisTm} and store_type_class != ''
                                                and task_type_desc = 'ALL' AND task_status = 'ALL'
                ) task_similar
                                             on   store.store_type = task_similar.store_type_class
                                                 and  store.stats_date = task_similar.statis_tm
                                                 and  store.brand_name = task_similar.brand_name

            ) pre_year on cur.tenant_id=pre_year.tenant_id
                and cur.store_id=pre_year.store_id
                and cur.brand_name=pre_year.brand_name
    </insert>

    <insert id="insertSelectBatchMtd">
        insert into retail_consultant_db.t_ads_wide_store_monthly

        with di_local_pre_month_tmp AS (
        select
        scd.store_id as store_id,
        di.brand_name as brand_name,
        sum(yun_store_sale_amt) as yun_store_sale_amt,
        sum(large_screen_sale_amt) as large_screen_sale_amt,
        sum(gw_retail_amt) as gw_retail_amt,
        sum(sale_amt) as sales_amt_actual,
        sum(mw.cust_count) as cust_count,
        sum(mw.cust_order_qty) as cust_order_qty,
        sum(di.sale_tag_amt) as sales_amt_list,
        sum(di.sale_piece) as sales_qty_for_upt,
        sum(di.store_staff_cnt) as have_sale_staff_cnt,
        sum(di.new_good_sale_amt) as sales_amt_actual_in_season,
        avg(mw.top20_sales_match_rate) as top20_sales_match_rate
        from
        retail_consultant_db.dm_zy_mco_store_statis_di_local di
        left join retail_consultant_db.t_ads_store_scd scd ON di.cms_code = scd.brand_store_code
        left join dm_store_stats_daily mw on di.cms_code = mw.cms_code and di.statis_tm = mw.statis_tm and di.brand_name = mw.brand_name
        where
        scd.store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
        and di.statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}
        group by scd.store_id,di.brand_name
        ),
        di_local_pre_year_tmp AS (
        select
        scd.store_id as store_id,
        di.brand_name as brand_name,
        sum(yun_store_sale_amt) as yun_store_sale_amt,
        sum(large_screen_sale_amt) as large_screen_sale_amt,
        sum(gw_retail_amt) as gw_retail_amt,
        sum(sale_amt) as sales_amt_actual,
        sum(mw.cust_count) as cust_count,
        sum(mw.cust_order_qty) as cust_order_qty,
        sum(di.sale_tag_amt) as sales_amt_list,
        sum(di.sale_piece) as sales_qty_for_upt,
        sum(di.store_staff_cnt) as have_sale_staff_cnt,
        sum(di.new_good_sale_amt) as sales_amt_actual_in_season,
        avg(mw.top20_sales_match_rate) as top20_sales_match_rate
        from
        retail_consultant_db.dm_zy_mco_store_statis_di_local di
        left join retail_consultant_db.t_ads_store_scd scd ON di.cms_code = scd.brand_store_code
        left join dm_store_stats_daily mw on di.cms_code = mw.cms_code and di.statis_tm = mw.statis_tm and di.brand_name = mw.brand_name
        where
        scd.store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
        and di.statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}
        group by scd.store_id,di.brand_name
        ),

        sku_di_local_pre_month_tmp AS (
        SELECT scd.store_id as store_id , t1.brand_name as brand_name,
        COUNT(DISTINCT CASE WHEN sale_amt > 0 THEN art_no ELSE NULL END) AS sales_sku_cnt,
        COUNT(DISTINCT CASE WHEN to_store_days > 0 THEN art_no ELSE NULL END) AS total_sku_cnt
        FROM dm_zy_mco_good_data_fashion_num_di_local t1
        left join retail_consultant_db.t_ads_store_scd scd ON t1.cms_code = scd.brand_store_code
        WHERE statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}
        and scd.store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
        GROUP BY scd.store_id, t1.brand_name
        ),
        sku_di_local_pre_year_tmp AS (
        SELECT scd.store_id as store_id, t1.brand_name as brand_name,
        COUNT(DISTINCT CASE WHEN sale_amt > 0 THEN art_no ELSE NULL END) AS sales_sku_cnt,
        COUNT(DISTINCT CASE WHEN to_store_days > 0 THEN art_no ELSE NULL END) AS total_sku_cnt
        FROM dm_zy_mco_good_data_fashion_num_di_local t1
        left join retail_consultant_db.t_ads_store_scd scd ON t1.cms_code = scd.brand_store_code
        WHERE statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}
        and scd.store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
        GROUP BY scd.store_id, t1.brand_name
        ),

        sales_pct_di_local_pre_month_tmp AS(
        select
        scd.store_id as store_id,
        t1.brand_name as brand_name,
        case
        when sum(t2.sales_amt) >0 then sum(t1.top_sales_amt)/ sum(t2.sales_amt)
        else 0
        end as top_sales_amt_actual_pct
        from
        (
        select
        t.cms_code,
        t.brand_name,
        t.statis_tm,
        sum(t.top_sales_amt) as top_sales_amt
        from
        (
        select
        ap.cms_code,
        ap.brand_name,
        ap.statis_tm,
        sum(case when mp.div_sales_store_rank <![CDATA[ <= ]]> 30 then ap.sale_amt else 0 end) as top_sales_amt
        from
        (
        select
        dz.*
        from
        dm_zy_mco_good_data_fashion_num_di_local dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and
        dz.statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}
        and big_class = '服装') as ap
        left join (
        select
        dz.*
        from
        dm_store_good_fashion_num_daily dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and
        dz.statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}) as mp
        on
        ap.statis_tm = mp.statis_tm
        and ap.cms_code = mp.cms_code
        and ap.fashion_num = mp.fashion_num
        and ap.art_no = mp.good_id
        group by
        cms_code,
        brand_name,
        statis_tm
        union all
        select
        ap.cms_code,
        ap.brand_name,
        ap.statis_tm,
        sum(case when mp.div_sales_store_rank <![CDATA[ <= ]]> 20 then ap.sale_amt else 0 end) as top_sales_amt
        from
        (
        select
        dz.*
        from
        dm_zy_mco_good_data_fashion_num_di_local dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and
        dz.statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}
        and big_class = '鞋类') as ap
        left join (
        select
        dz.*
        from
        dm_store_good_fashion_num_daily dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and

        dz.statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}) as mp
        on
        ap.statis_tm = mp.statis_tm
        and ap.cms_code = mp.cms_code
        and ap.fashion_num = mp.fashion_num
        and ap.art_no = mp.good_id
        group by
        cms_code,
        brand_name,
        statis_tm) t
        group by
        t.cms_code,
        t.brand_name,
        t.statis_tm) t1
        left join (
        select
        dz.cms_code ,
        dz.brand_name,
        sum(dz.sale_amt) as sales_amt
        from
        dm_zy_mco_good_data_fashion_num_di_local dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and
        statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}
        group by
        cms_code,
        brand_name) t2
        on
        t1.cms_code = t2.cms_code
        and t1.brand_name = t2.brand_name
        left join retail_consultant_db.t_ads_store_scd scd ON
        t1.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and t1.statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}
        group by
        scd.store_id,
        t1.brand_name
        ),

        sales_pct_di_local_pre_year_tmp AS(select
        scd.store_id as store_id,
        t1.brand_name as brand_name,
        case
        when sum(t2.sales_amt) >0 then sum(t1.top_sales_amt)/ sum(t2.sales_amt)
        else 0
        end as top_sales_amt_actual_pct
        from
        (
        select
        t.cms_code,
        t.brand_name,
        t.statis_tm,
        sum(t.top_sales_amt) as top_sales_amt
        from
        (
        select
        ap.cms_code,
        ap.brand_name,
        ap.statis_tm,
        sum(case when mp.div_sales_store_rank <![CDATA[ <= ]]> 30 then ap.sale_amt else 0 end) as top_sales_amt
        from
        (
        select
        dz.*
        from
        dm_zy_mco_good_data_fashion_num_di_local dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and
        dz.statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}
        and big_class = '服装') as ap
        left join (
        select
        dz.*
        from
        dm_store_good_fashion_num_daily dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and
        dz.statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}) as mp
        on
        ap.statis_tm = mp.statis_tm
        and ap.cms_code = mp.cms_code
        and ap.fashion_num = mp.fashion_num
        and ap.art_no = mp.good_id
        group by
        cms_code,
        brand_name,
        statis_tm
        union all
        select
        ap.cms_code,
        ap.brand_name,
        ap.statis_tm,
        sum(case when mp.div_sales_store_rank <![CDATA[ <= ]]> 20 then ap.sale_amt else 0 end) as top_sales_amt
        from
        (
        select
        dz.*
        from
        dm_zy_mco_good_data_fashion_num_di_local dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and
        dz.statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}
        and big_class = '鞋类') as ap
        left join (
        select
        dz.*
        from
        dm_store_good_fashion_num_daily dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and

        dz.statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}) as mp
        on
        ap.statis_tm = mp.statis_tm
        and ap.cms_code = mp.cms_code
        and ap.fashion_num = mp.fashion_num
        and ap.art_no = mp.good_id
        group by
        cms_code,
        brand_name,
        statis_tm) t
        group by
        t.cms_code,
        t.brand_name,
        t.statis_tm) t1
        left join (
        select
        dz.cms_code ,
        dz.brand_name,
        sum(dz.sale_amt) as sales_amt
        from
        dm_zy_mco_good_data_fashion_num_di_local dz
        left join retail_consultant_db.t_ads_store_scd scd ON
        dz.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and
        statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}
        group by
        cms_code,
        brand_name) t2
        on
        t1.cms_code = t2.cms_code
        and t1.brand_name = t2.brand_name
        left join retail_consultant_db.t_ads_store_scd scd ON
        t1.cms_code = scd.brand_store_code
        where
        scd.store_id in <foreach collection = "storeIds" item = "item" open = "(" separator = "," close = ")">#{item}</foreach>
        and t1.statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}
        group by
        scd.store_id,
        t1.brand_name
        ),

        is_homo_di_local_pre_month_tmp AS(
        SELECT
        scd.store_id as store_id,
        t1.brand_name as brand_name,
        sum(t1.is_homo) as top50_is_homo_cnt,
        <!--        sum(t1.inv_qty) as top50_inv_qty,-->
        count(art_no) as top50_art_no_cnt
        <!--        CASE-->
        <!--        WHEN count(art_no) <![CDATA[ > ]]> 0 THEN sum(is_homo) * 1.0000 / count(art_no)-->
        <!--        END AS top50_sku_unbroken_rate,-->
        <!--        avg(related_ratio) AS top_sales_upt,-->
        <!--        CASE-->
        <!--        WHEN count(art_no) <![CDATA[ > ]]> 0 THEN sum(inv_qty) * 1.0000 / count(art_no)-->
        <!--        END AS top50_inv_depth-->
        FROM (SELECT cms_code, brand_name, statis_tm, art_no, is_homo, related_ratio, inv_qty, big_class, rank() OVER w
        AS sale_rank
        FROM dm_zy_mco_good_data_fashion_num_di_local
        WHERE statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}
        AND big_class IN ('鞋类',
        '服装'
        )
        AND sale_amt <![CDATA[ > ]]> 0 WINDOW w AS (PARTITION BY cms_code, brand_name, big_class
        ORDER BY sale_amt DESC
        RANGE UNBOUNDED PRECEDING))t1
        left join retail_consultant_db.t_ads_store_scd scd ON t1.cms_code = scd.brand_store_code
        WHERE (big_class='服装' AND
        sale_rank <![CDATA[ <= ]]> 30)
        OR (big_class!='服装' AND
        sale_rank <![CDATA[ <= ]]> 20)
        and scd.store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
        and t1.statis_tm between #{preMonthStatisTm} and #{endPreMonthStatisTm}
        GROUP BY scd.store_id,t1.brand_name
        ),
        is_homo_di_local_pre_year_tmp AS(
        SELECT
        scd.store_id as store_id,
        t1.brand_name as brand_name,
        sum(t1.is_homo) as top50_is_homo_cnt,
        <!--        sum(t1.inv_qty) as top50_inv_qty,-->
        count(art_no) as top50_art_no_cnt
        <!--        CASE-->
        <!--        WHEN count(art_no) <![CDATA[ > ]]> 0 THEN sum(is_homo) * 1.0000 / count(art_no)-->
        <!--        END AS top50_sku_unbroken_rate,-->
        <!--        avg(related_ratio) AS top_sales_upt,-->
        <!--        CASE-->
        <!--        WHEN count(art_no) <![CDATA[ > ]]> 0 THEN sum(inv_qty) * 1.0000 / count(art_no)-->
        <!--        END AS top50_inv_depth-->
        FROM (SELECT cms_code, brand_name, statis_tm, art_no, is_homo, related_ratio, inv_qty, big_class, rank() OVER w
        AS sale_rank
        FROM dm_zy_mco_good_data_fashion_num_di_local
        WHERE statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}
        AND big_class IN ('鞋类',
        '服装'
        )
        AND sale_amt <![CDATA[ > ]]> 0 WINDOW w AS (PARTITION BY cms_code, brand_name, big_class
        ORDER BY sale_amt DESC
        RANGE UNBOUNDED PRECEDING))t1
        left join retail_consultant_db.t_ads_store_scd scd ON t1.cms_code = scd.brand_store_code
        WHERE (big_class='服装' AND
        sale_rank <![CDATA[ <= ]]> 30)
        OR (big_class!='服装' AND
        sale_rank <![CDATA[ <= ]]> 20)
        and scd.store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
        and t1.statis_tm between #{preYearStatisTm} and #{endPreYearStatisTm}
        GROUP BY scd.store_id,t1.brand_name
        )

        select cur.cms_code,
            cur.brand_store_code,
            cur.temperature_area,
            cur.store_nature,
            cur.store_type_name,
            cur.shopLevel_adjust,
            cur.org_branch_name,
            cur.stats_date,
            #{endStatisTm},
            #{lastDayFlag},
            cur.name,
            cur.address,
            cur.province_name,
            cur.join_type,
            cur.company_type,
            cur.store_id,
            cur.key_store,
            cur.opening_date,
            cur.store_type,
            cur.expand_type,
            cur.store_image_name,
            cur.rack_type,
            cur.channel_brand_code,
            cur.city_name,
            cur.tenant_id,
            cur.is_retail,
            cur.district_name,
            cur.level,
            cur.org_office_name,
            cur.brand_name,
            cur.store_level_name,
            cur.org_area_name,
            cur.business_status,
            cur.new_good_dynamic_rate,
            cur.vip_repurchase_cnt_similar,
            cur.inv_sale_rate_similar,
            cur.upt_similar,
            cur.one_to_seven_dynamic_rate,
            cur.sales_amt_actual_per_staff_similar,
            cur.aur_similar,
            cur.top50_sku_unbroken_rate_similar,
            cur.one_to_seven_dynamic_rate_similar,
            cur.shoes_clothing_sales_qty,
            cur.top50_sales_inv_match_rate_similar,
            cur.write_off_rate_similar,
            cur.thousand_retail_cnt_pct,
            cur.vip_repurchase_duration_similar,
            cur.sales_amt_list_similar,
            cur.retail_cnt,
            cur.top50_match_cnt,
            cur.sales_amt_actual_in_season,
            cur.inside_outside_ratio,
            cur.plural_retail_cnt_similar,
            cur.singular_tot_amt_actual,
            cur.issue_qty,
            cur.numOfPurMember_sc_label,
            cur.top_sales_upt,
            cur.stay_sku_cnt,
            cur.vip_retail_pct,
            cur.shop_area,
            cur.exec_qty,
            cur.gw_retail_amt_pct_similar,
            cur.couponUsageRate_sc_label,
            cur.plural_tot_amt_list_similar,
            cur.sales_amt_actual_similar,
            cur.numOfPurNewMember_sc_label,
            cur.salesOfInSeason_sc_label,
            cur.consume_member_qty,
            cur.avg_unit_inv_list_similar,
            cur.shoes_clothing_retail_cnt,
            cur.retial_cnt_per_staff_similar,
            cur.sales_amt_actual,
            cur.inv_amt,
            cur.singular_retail_cnt_similar,
            cur.write_off_qty,
            cur.top50_art_no_cnt,
            cur.topFullSizeRate_sc_label,
            cur.plural_tot_amt_actual_similar,
            cur.offline_amt_similar,
            cur.large_screen_sale_amt,
            cur.sale_deep_similar,
            cur.plural_tot_amt_actual,
            cur.inside_outside_ratio_similar,
            cur.yun_store_sale_amt_pct_similar,
            cur.stay_sku_cnt_similar,
            cur.sales_sc_label,
            cur.shoes_clothes_ratio,
            cur.matchDegTopProduct_sc_label,
            cur.new_consume_member_qty,
            cur.atv_sc_label,
            cur.consume_member_qty_similar,
            cur.cr_sc_label,
            cur.numOfPurExistMember_sc_label,
            cur.old_consume_member_qty,
            cur.singular_tot_amt_list,
            cur.singular_tot_amt_list_similar,
            cur.upt_excl_acc_similar,
            cur.top_sales_amt_actual_pct_similar,
            cur.top50_sales_amt_actual,
            cur.dynamic_sale_rate_similar,
            cur.total_sku_cnt,
            cur.new_good_dynamic_rate_similar,
            cur.old_consume_member_qty_similar,
            cur.atv_similar,
            cur.exec_rate_similar,
            cur.shoes_qty_ge2_receipt_ratio_similar,
            cur.have_sale_staff_cnt,
            cur.amt_ge1000_receipt_ratio_similar,
            cur.md_sc_label,
            cur.plural_discount_rate_similar,
            cur.discount_rate_diff_similar,
            cur.salesTnumStaffs_sc_label,
            cur.plural_tot_amt_list,
            cur.top50_is_homo_cnt,
            cur.sales_sku_cnt,
            cur.singular_tot_amt_actual_similar,
            cur.upt_sc_label,
            cur.topSalesPct_sc_label,
            cur.vip_repurchase_duration,
            cur.inv_sale_rate,
            cur.two_pairs_shoes_retail_cnt_pct,
            cur.sales_target,
            cur.large_screen_sale_amt_similar,
            cur.singular_retail_cnt,
            cur.vip_retail_pct_similar,
            cur.shoes_clothes_ratio_similar,
            cur.sales_amt_actual_in_season_pct_similar,
            cur.sales_amt_list_in_season,
            cur.retail_cnt_have_customer,
            cur.customer_cnt,
            cur.new_sku_discount_rate_similar,
            cur.gw_retail_amt_similar,
            cur.top_sales_upt_similar,
            cur.inv_qty,
            cur.vip_repurchase_cnt,
            cur.large_screen_sale_amt_pct_similar,
            cur.skuEfficency_sc_label,
            cur.task_tgt_qty,
            cur.yun_store_sale_amt_similar,
            cur.sales_qty_for_upt,
            cur.plural_retail_cnt_pct_similar,
            cur.deal_rate_similar,
            cur.offline_amt_pct_similar,
            cur.plural_retail_cnt,
            cur.new_consume_member_qty_similar,
            cur.yun_store_sale_amt,
            cur.gw_retail_amt,
            cur.sales_qty,
            cur.singular_discount_rate_similar,
            cur.sales_amt_list,
            cur.discount_rate_similar,
            cur.taskExecuteRate_sc_label,
            cur.total_amount_similar,
            pre_month.new_good_dynamic_rate AS new_good_dynamic_rate_mom,
            pre_month.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate_mom,
            pre_month.shoes_clothing_sales_qty AS shoes_clothing_sales_qty_mom,
            pre_month.thousand_retail_cnt_pct AS thousand_retail_cnt_pct_mom,
            pre_month.retail_cnt AS retail_cnt_mom,
            pre_month.top50_match_cnt AS top50_match_cnt_mom,
            pre_month.sales_amt_actual_in_season AS sales_amt_actual_in_season_mom,
            pre_month.inside_outside_ratio AS inside_outside_ratio_mom,
            pre_month.singular_tot_amt_actual AS singular_tot_amt_actual_mom,
            pre_month.issue_qty AS issue_qty_mom,
            pre_month.numOfPurMember_sc_label AS numOfPurMember_sc_label_mom,
            pre_month.top_sales_upt AS top_sales_upt_mom,
            pre_month.stay_sku_cnt AS stay_sku_cnt_mom,
            pre_month.vip_retail_pct AS vip_retail_pct_mom,
            pre_month.shop_area AS shop_area_mom,
            pre_month.exec_qty AS exec_qty_mom,
            pre_month.couponUsageRate_sc_label AS couponUsageRate_sc_label_mom,
            pre_month.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label_mom,
            pre_month.salesOfInSeason_sc_label AS salesOfInSeason_sc_label_mom,
            round(pre_month.consume_member_qty * #{calculateRatio}) AS consume_member_qty_mom,
            pre_month.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt_mom,
            pre_month.sales_amt_actual AS sales_amt_actual_mom,
            pre_month.inv_amt AS inv_amt_mom,
            pre_month.write_off_qty AS write_off_qty_mom,
            pre_month.top50_art_no_cnt AS top50_art_no_cnt_mom,
            pre_month.topFullSizeRate_sc_label AS topFullSizeRate_sc_label_mom,
            pre_month.large_screen_sale_amt AS large_screen_sale_amt_mom,
            pre_month.plural_tot_amt_actual AS plural_tot_amt_actual_mom,
            pre_month.sales_sc_label AS sales_sc_label_mom,
            pre_month.shoes_clothes_ratio AS shoes_clothes_ratio_mom,
            pre_month.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label_mom,
            round(pre_month.new_consume_member_qty * #{calculateRatio}) AS new_consume_member_qty_mom,
            pre_month.atv_sc_label AS atv_sc_label_mom,
            pre_month.cr_sc_label AS cr_sc_label_mom,
            pre_month.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label_mom,
            round(pre_month.old_consume_member_qty * #{calculateRatio}) AS old_consume_member_qty_mom,
            pre_month.singular_tot_amt_list AS singular_tot_amt_list_mom,
            pre_month.top50_sales_amt_actual AS top50_sales_amt_actual_mom,
            pre_month.total_sku_cnt AS total_sku_cnt_mom,
            pre_month.have_sale_staff_cnt AS have_sale_staff_cnt_mom,
            pre_month.md_sc_label AS md_sc_label_mom,
            pre_month.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label_mom,
            pre_month.plural_tot_amt_list AS plural_tot_amt_list_mom,
            pre_month.top50_is_homo_cnt AS top50_is_homo_cnt_mom,
            pre_month.sales_sku_cnt AS sales_sku_cnt_mom,
            pre_month.upt_sc_label AS upt_sc_label_mom,
            pre_month.topSalesPct_sc_label AS topSalesPct_sc_label_mom,
            pre_month.vip_repurchase_duration AS vip_repurchase_duration_mom,
            pre_month.inv_sale_rate AS inv_sale_rate_mom,
            pre_month.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct_mom,
            pre_month.sales_target AS sales_target_mom,
            pre_month.singular_retail_cnt AS singular_retail_cnt_mom,
            pre_month.sales_amt_list_in_season AS sales_amt_list_in_season_mom,
            pre_month.retail_cnt_have_customer AS retail_cnt_have_customer_mom,
            pre_month.customer_cnt AS customer_cnt_mom,
            pre_month.inv_qty AS inv_qty_mom,
            pre_month.vip_repurchase_cnt AS vip_repurchase_cnt_mom,
            pre_month.skuEfficency_sc_label AS skuEfficency_sc_label_mom,
            pre_month.task_tgt_qty AS task_tgt_qty_mom,
            pre_month.sales_qty_for_upt AS sales_qty_for_upt_mom,
            pre_month.plural_retail_cnt AS plural_retail_cnt_mom,
            pre_month.yun_store_sale_amt AS yun_store_sale_amt_mom,
            pre_month.gw_retail_amt AS gw_retail_amt_mom,
            pre_month.sales_qty AS sales_qty_mom,
            pre_month.sales_amt_list AS sales_amt_list_mom,
            pre_month.taskExecuteRate_sc_label AS taskExecuteRate_sc_label_mom,
            pre_year.new_good_dynamic_rate AS new_good_dynamic_rate_yoy,
            pre_year.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate_yoy,
            pre_year.shoes_clothing_sales_qty AS shoes_clothing_sales_qty_yoy,
            pre_year.thousand_retail_cnt_pct AS thousand_retail_cnt_pct_yoy,
            pre_year.retail_cnt AS retail_cnt_yoy,
            pre_year.top50_match_cnt AS top50_match_cnt_yoy,
            pre_year.sales_amt_actual_in_season AS sales_amt_actual_in_season_yoy,
            pre_year.inside_outside_ratio AS inside_outside_ratio_yoy,
            pre_year.singular_tot_amt_actual AS singular_tot_amt_actual_yoy,
            pre_year.issue_qty AS issue_qty_yoy,
            pre_year.numOfPurMember_sc_label AS numOfPurMember_sc_label_yoy,
            pre_year.top_sales_upt AS top_sales_upt_yoy,
            pre_year.stay_sku_cnt AS stay_sku_cnt_yoy,
            pre_year.vip_retail_pct AS vip_retail_pct_yoy,
            pre_year.shop_area AS shop_area_yoy,
            pre_year.exec_qty AS exec_qty_yoy,
            pre_year.couponUsageRate_sc_label AS couponUsageRate_sc_label_yoy,
            pre_year.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label_yoy,
            pre_year.salesOfInSeason_sc_label AS salesOfInSeason_sc_label_yoy,
            round(pre_year.consume_member_qty * #{calculateRatio}) AS consume_member_qty_yoy,
            pre_year.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt_yoy,
            pre_year.sales_amt_actual AS sales_amt_actual_yoy,
            pre_year.inv_amt AS inv_amt_yoy,
            pre_year.write_off_qty AS write_off_qty_yoy,
            pre_year.top50_art_no_cnt AS top50_art_no_cnt_yoy,
            pre_year.topFullSizeRate_sc_label AS topFullSizeRate_sc_label_yoy,
            pre_year.large_screen_sale_amt AS large_screen_sale_amt_yoy,
            pre_year.plural_tot_amt_actual AS plural_tot_amt_actual_yoy,
            pre_year.sales_sc_label AS sales_sc_label_yoy,
            pre_year.shoes_clothes_ratio AS shoes_clothes_ratio_yoy,
            pre_year.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label_yoy,
            round(pre_year.new_consume_member_qty * #{calculateRatio}) AS new_consume_member_qty_yoy,
            pre_year.atv_sc_label AS atv_sc_label_yoy,
            pre_year.cr_sc_label AS cr_sc_label_yoy,
            pre_year.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label_yoy,
            round(pre_year.old_consume_member_qty * #{calculateRatio}) AS old_consume_member_qty_yoy,
            pre_year.singular_tot_amt_list AS singular_tot_amt_list_yoy,
            pre_year.top50_sales_amt_actual AS top50_sales_amt_actual_yoy,
            pre_year.total_sku_cnt AS total_sku_cnt_yoy,
            pre_year.have_sale_staff_cnt AS have_sale_staff_cnt_yoy,
            pre_year.md_sc_label AS md_sc_label_yoy,
            pre_year.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label_yoy,
            pre_year.plural_tot_amt_list AS plural_tot_amt_list_yoy,
            pre_year.top50_is_homo_cnt AS top50_is_homo_cnt_yoy,
            pre_year.sales_sku_cnt AS sales_sku_cnt_yoy,
            pre_year.upt_sc_label AS upt_sc_label_yoy,
            pre_year.topSalesPct_sc_label AS topSalesPct_sc_label_yoy,
            pre_year.vip_repurchase_duration AS vip_repurchase_duration_yoy,
            pre_year.inv_sale_rate AS inv_sale_rate_yoy,
            pre_year.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct_yoy,
            pre_year.sales_target AS sales_target_yoy,
            pre_year.singular_retail_cnt AS singular_retail_cnt_yoy,
            pre_year.sales_amt_list_in_season AS sales_amt_list_in_season_yoy,
            pre_year.retail_cnt_have_customer AS retail_cnt_have_customer_yoy,
            pre_year.customer_cnt AS customer_cnt_yoy,
            pre_year.inv_qty AS inv_qty_yoy,
            pre_year.vip_repurchase_cnt AS vip_repurchase_cnt_yoy,
            pre_year.skuEfficency_sc_label AS skuEfficency_sc_label_yoy,
            pre_year.task_tgt_qty AS task_tgt_qty_yoy,
            pre_year.sales_qty_for_upt AS sales_qty_for_upt_yoy,
            pre_year.plural_retail_cnt AS plural_retail_cnt_yoy,
            pre_year.yun_store_sale_amt AS yun_store_sale_amt_yoy,
            pre_year.gw_retail_amt AS gw_retail_amt_yoy,
            pre_year.sales_qty AS sales_qty_yoy,
            pre_year.sales_amt_list AS sales_amt_list_yoy,
            pre_year.taskExecuteRate_sc_label AS taskExecuteRate_sc_label_yoy,
            cur.top_sales_amt_actual_pct AS top_sales_amt_actual_pct,
            pre_month.top_sales_amt_actual_pct AS top_sales_amt_actual_pct_mom,
            pre_year.top_sales_amt_actual_pct AS top_sales_amt_actual_pct_yoy,
            cur.top20_sales_match_rate AS top20_sales_match_rate,
            pre_month.top20_sales_match_rate AS top20_sales_match_rate_mom,
            pre_year.top20_sales_match_rate AS top20_sales_match_rate_yoy,
            cur.top20_sales_match_rate_similar AS top20_sales_match_rate_similar,
            cur.new_reg_member_qty AS new_reg_member_qty,
            round(pre_year.new_reg_member_qty * #{calculateRatio}) AS new_reg_member_qty_yoy,
            pre_month.new_reg_member_qty AS new_reg_member_qty_mom,
            cur.new_reg_member_qty_similar AS new_reg_member_qty_similar,
            cur.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty,
            pre_year.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty_yoy,
            pre_month.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty_mom,
            cur.new_reg_has_consume_member_qty_similar AS new_reg_has_consume_member_qty_similar,
            cur.sku_unbroken_rate AS sku_unbroken_rate,
            pre_year.sku_unbroken_rate AS sku_unbroken_rate_yoy,
            pre_month.sku_unbroken_rate AS sku_unbroken_rate_mom,
            cur.sku_unbroken_rate_similar AS sku_unbroken_rate_similar,
            cur.new_reg_has_consume_ratio_similar AS new_reg_has_consume_ratio_similar,
            cur.high_val_member_consume_member_qty AS high_val_member_consume_member_qty,
            round(pre_year.high_val_member_consume_member_qty * #{calculateRatio}) AS high_val_member_consume_member_qty_yoy,
            pre_month.high_val_member_consume_member_qty AS high_val_member_consume_member_qty_mom,
            cur.high_val_member_consume_member_qty_similar AS high_val_member_consume_member_qty_similar,
            cur.birth_issue_qty AS birth_issue_qty,
            pre_year.birth_issue_qty AS birth_issue_qty_yoy,
            pre_month.birth_issue_qty AS birth_issue_qty_mom,
            cur.birth_issue_qty_similar AS birth_issue_qty_similar,
            cur.birth_write_off_qty AS birth_write_off_qty,
            pre_year.birth_write_off_qty AS birth_write_off_qty_yoy,
            pre_month.birth_write_off_qty AS birth_write_off_qty_mom,
            cur.birth_write_off_qty_similar AS birth_write_off_qty_similar,
            cur.birth_write_off_rate_similar AS birth_write_off_rate_similar,
            cur.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty,
            pre_year.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty_yoy,
            pre_month.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty_mom,
            cur.new_reg_non_consume_member_qty_similar AS new_reg_non_consume_member_qty_similar,
            cur.new_reg_non_consume_ratio_similar as new_reg_non_consume_ratio_similar,
            cur.high_val_member_consume_amt AS high_val_member_consume_amt,
            pre_year.high_val_member_consume_amt AS high_val_member_consume_amt_yoy,
            pre_month.high_val_member_consume_amt AS high_val_member_consume_amt_mom,
            cur.high_val_member_consume_amt_similar AS high_val_member_consume_amt_similar,
            cur.high_val_member_order_qty AS high_val_member_order_qty,
            pre_year.high_val_member_order_qty AS high_val_member_order_qty_yoy,
            pre_month.high_val_member_order_qty AS high_val_member_order_qty_mom,
            cur.high_val_member_order_qty_similar AS high_val_member_order_qty_similar,
            cur.high_val_member_atv_similar AS high_val_member_atv_similar,
            cur.curr_month_new_member_order_qty AS curr_month_new_member_order_qty,
            pre_year.curr_month_new_member_order_qty AS curr_month_new_member_order_qty_yoy,
            pre_month.curr_month_new_member_order_qty AS curr_month_new_member_order_qty_mom,
            cur.curr_month_old_member_order_qty AS curr_month_old_member_order_qty,
            pre_year.curr_month_old_member_order_qty AS curr_month_old_member_order_qty_yoy,
            pre_month.curr_month_old_member_order_qty AS curr_month_old_member_order_qty_mom,
            cur.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty,
            pre_year.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty_yoy,
            pre_month.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty_mom,
            cur.barg_new_cust_reg_pct_similar AS barg_new_cust_reg_pct_similar,
            cur.old_member_consume_amt AS old_member_consume_amt,
            pre_year.old_member_consume_amt AS old_member_consume_amt_yoy,
            pre_month.old_member_consume_amt AS old_member_consume_amt_mom,
            cur.old_member_consume_amt_similar AS old_member_consume_amt_similar,
            cur.old_member_order_qty AS old_member_order_qty,
            pre_year.old_member_order_qty AS old_member_order_qty_yoy,
            pre_month.old_member_order_qty AS old_member_order_qty_mom,
            cur.old_member_order_qty_similar AS old_member_order_qty_similar,
            cur.old_member_atv_similar AS old_member_atv_similar,
            cur.new_member_consume_amt AS new_member_consume_amt,
            pre_year.new_member_consume_amt AS new_member_consume_amt_yoy,
            pre_month.new_member_consume_amt AS new_member_consume_amt_mom,
            cur.new_member_consume_amt_similar AS new_member_consume_amt_similar,
            cur.new_member_order_qty AS new_member_order_qty,
            pre_year.new_member_order_qty AS new_member_order_qty_yoy,
            pre_month.new_member_order_qty AS new_member_order_qty_mom,
            cur.new_member_order_qty_similar AS new_member_order_qty_similar,
            cur.new_member_atv_similar AS new_member_atv_similar,
            cur.member_consume_amt AS member_consume_amt,
            pre_year.member_consume_amt AS member_consume_amt_yoy,
            pre_month.member_consume_amt AS member_consume_amt_mom,
            cur.member_consume_amt_similar AS member_consume_amt_similar,
            cur.high_val_member_consume_amt_pct_similar AS high_val_member_consume_amt_pct_similar,
            cur.old_member_consume_amt_pct_similar AS old_member_consume_amt_pct_similar,
            cur.new_member_consume_amt_pct_similar AS new_member_consume_amt_pct_similar,
            cur.new_consume_member_qty_pct_similar AS new_consume_member_qty_pct_similar,
            cur.old_consume_member_qty_pct_similar AS old_consume_member_qty_pct_similar,
            cur.high_val_member_consume_member_qty_pct_similar AS high_val_member_consume_member_qty_pct_similar,
            cur.numOfPurNewMember_target,
            pre_year.numOfPurNewMember_target AS numOfPurNewMember_target_yoy,
            pre_month.numOfPurNewMember_target AS numOfPurNewMember_target_mom,
            cur.numOfPurMember_target,
            pre_year.numOfPurMember_target AS numOfPurMember_target_yoy,
            pre_month.numOfPurMember_target AS numOfPurMember_target_mom,
            cur.numOfPurExistMember_target,
            pre_year.numOfPurExistMember_target AS numOfPurExistMember_target_yoy,
            pre_month.numOfPurExistMember_target AS numOfPurExistMember_target_mom,
            cur.new_reg_member_qty_target,
            pre_year.new_reg_member_qty_target AS new_reg_member_qty_target_yoy,
            pre_month.new_reg_member_qty_target AS new_reg_member_qty_target_mom,
            cur.new_reg_has_consume_member_rate_target,
            pre_year.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target_yoy,
            pre_month.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target_mom,
            cur.high_val_member_consume_member_qty_target,
            pre_year.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target_yoy,
            pre_month.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target_mom,
            pre_year.new_reg_has_consume_ratio AS new_reg_has_consume_ratio_yoy
        from
            (
                select
                    store.cms_code AS cms_code,
                    scd.brand_store_code AS brand_store_code,
                    scd.temperature_area AS temperature_area,
                    scd.store_nature AS store_nature,
                    scd.store_type_name AS store_type_name,
                    scd.shopLevel_adjust AS shopLevel_adjust,
                    scd.org_branch_name AS org_branch_name,
                    store.stats_date AS stats_date,
                    scd.name AS name,
                    scd.address AS address,
                    scd.province_name AS province_name,
                    scd.join_type AS join_type,
                    scd.company_type AS company_type,
                    store.store_id AS store_id,
                    scd.key_store AS key_store,
                    scd.opening_date AS opening_date,
                    scd.store_type AS store_type,
                    scd.expand_type AS expand_type,
                    scd.store_image_name AS store_image_name,
                    scd.rack_type AS rack_type,
                    scd.channel_brand_code AS channel_brand_code,
                    scd.city_name AS city_name,
                    scd.tenant_id AS tenant_id,
                    scd.is_retail AS is_retail,
                    scd.district_name AS district_name,
                    scd.level AS level,
                    scd.org_office_name AS org_office_name,
                    store.brand_name AS brand_name,
                    scd.store_level_name AS store_level_name,
                    scd.org_area_name AS org_area_name,
                    scd.business_status AS business_status,
                    store.new_good_dynamic_rate AS new_good_dynamic_rate,
                    similar.vip_repurchase_cnt_similar AS vip_repurchase_cnt_similar,
                    similar.inv_sale_rate_similar AS inv_sale_rate_similar,
                    similar.upt_similar AS upt_similar,
                    store.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate,
                    similar.sales_amt_actual_per_staff_similar AS sales_amt_actual_per_staff_similar,
                    similar.aur_similar AS aur_similar,
                    similar.top50_sku_unbroken_rate_similar AS top50_sku_unbroken_rate_similar,
                    similar.one_to_seven_dynamic_rate_similar AS one_to_seven_dynamic_rate_similar,
                    store.shoes_clothing_sales_qty AS shoes_clothing_sales_qty,
                    similar.top50_sales_inv_match_rate_similar AS top50_sales_inv_match_rate_similar,
                    coupon_similar.write_off_rate_similar AS write_off_rate_similar,
                    store.thousand_retail_cnt_pct AS thousand_retail_cnt_pct,
                    similar.vip_repurchase_duration_similar AS vip_repurchase_duration_similar,
                    similar.sales_amt_list_similar AS sales_amt_list_similar,
                    store.retail_cnt AS retail_cnt,
                    store.top50_match_cnt AS top50_match_cnt,
                    store.sales_amt_actual_in_season AS sales_amt_actual_in_season,
                    store.inside_outside_ratio AS inside_outside_ratio,
                    similar.plural_retail_cnt_similar AS plural_retail_cnt_similar,
                    store.singular_tot_amt_actual AS singular_tot_amt_actual,
                    member_coupon.issue_qty AS issue_qty,
                    scorecard.numOfPurMember_sc_label AS numOfPurMember_sc_label,
                    store.top_sales_upt AS top_sales_upt,
                    store.stay_sku_cnt AS stay_sku_cnt,
                    store.vip_retail_pct AS vip_retail_pct,
                    scd.shop_area AS shop_area,
                    task.exec_qty AS exec_qty,
                    similar.gw_retail_amt_pct_similar AS gw_retail_amt_pct_similar,
                    scorecard.couponUsageRate_sc_label AS couponUsageRate_sc_label,
                    similar.plural_tot_amt_list_similar AS plural_tot_amt_list_similar,
                    similar.sales_amt_actual_similar AS sales_amt_actual_similar,
                    scorecard.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label,
                    scorecard.salesOfInSeason_sc_label AS salesOfInSeason_sc_label,
                    member.consume_member_qty AS consume_member_qty,
                    similar.avg_unit_inv_list_similar AS avg_unit_inv_list_similar,
                    store.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt,
                    similar.retial_cnt_per_staff_similar AS retial_cnt_per_staff_similar,
                    store.sales_amt_actual AS sales_amt_actual,
                    store.inv_amt AS inv_amt,
                    similar.singular_retail_cnt_similar AS singular_retail_cnt_similar,
                    member_coupon.write_off_qty AS write_off_qty,
                    store.top50_art_no_cnt AS top50_art_no_cnt,
                    scorecard.topFullSizeRate_sc_label AS topFullSizeRate_sc_label,
                    similar.plural_tot_amt_actual_similar AS plural_tot_amt_actual_similar,
                    similar.offline_amt_similar AS offline_amt_similar,
                    store.large_screen_sale_amt AS large_screen_sale_amt,
                    similar.sale_deep_similar AS sale_deep_similar,
                    store.plural_tot_amt_actual AS plural_tot_amt_actual,
                    similar.inside_outside_ratio_similar AS inside_outside_ratio_similar,
                    similar.yun_store_sale_amt_pct_similar AS yun_store_sale_amt_pct_similar,
                    similar.stay_sku_cnt_similar AS stay_sku_cnt_similar,
                    scorecard.sales_sc_label AS sales_sc_label,
                    store.shoes_clothes_ratio AS shoes_clothes_ratio,
                    scorecard.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label,
                    member.new_consume_member_qty AS new_consume_member_qty,
                    scorecard.atv_sc_label AS atv_sc_label,
                    member_similar.consume_member_qty_similar AS consume_member_qty_similar,
                    scorecard.cr_sc_label AS cr_sc_label,
                    scorecard.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label,
                    member.old_consume_member_qty AS old_consume_member_qty,
                    store.singular_tot_amt_list AS singular_tot_amt_list,
                    similar.singular_tot_amt_list_similar AS singular_tot_amt_list_similar,
                    similar.upt_excl_acc_similar AS upt_excl_acc_similar,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top50_sales_amt_actual AS top50_sales_amt_actual,
                    similar.dynamic_sale_rate_similar AS dynamic_sale_rate_similar,
                    store.total_sku_cnt AS total_sku_cnt,
                    similar.new_good_dynamic_rate_similar AS new_good_dynamic_rate_similar,
                    member_similar.old_consume_member_qty_similar AS old_consume_member_qty_similar,
                    similar.atv_similar AS atv_similar,
                    task_similar.exec_rate_similar AS exec_rate_similar,
                    similar.shoes_qty_ge2_receipt_ratio_similar AS shoes_qty_ge2_receipt_ratio_similar,
                    store.have_sale_staff_cnt AS have_sale_staff_cnt,
                    similar.amt_ge1000_receipt_ratio_similar AS amt_ge1000_receipt_ratio_similar,
                    scorecard.md_sc_label AS md_sc_label,
                    similar.plural_discount_rate_similar AS plural_discount_rate_similar,
                    similar.discount_rate_diff_similar AS discount_rate_diff_similar,
                    scorecard.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label,
                    store.plural_tot_amt_list AS plural_tot_amt_list,
                    store.top50_is_homo_cnt AS top50_is_homo_cnt,
                    store.sales_sku_cnt AS sales_sku_cnt,
                    similar.singular_tot_amt_actual_similar AS singular_tot_amt_actual_similar,
                    scorecard.upt_sc_label AS upt_sc_label,
                    scorecard.topSalesPct_sc_label AS topSalesPct_sc_label,
                    store.vip_repurchase_duration AS vip_repurchase_duration,
                    store.inv_sale_rate AS inv_sale_rate,
                    store.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct,
                    feature_target.sales_target AS sales_target,
                    similar.large_screen_sale_amt_similar AS large_screen_sale_amt_similar,
                    store.singular_retail_cnt AS singular_retail_cnt,
                    similar.vip_retail_pct_similar AS vip_retail_pct_similar,
                    similar.shoes_clothes_ratio_similar AS shoes_clothes_ratio_similar,
                    similar.sales_amt_actual_in_season_pct_similar AS sales_amt_actual_in_season_pct_similar,
                    store.sales_amt_list_in_season AS sales_amt_list_in_season,
                    store.retail_cnt_have_customer AS retail_cnt_have_customer,
                    store.customer_cnt AS customer_cnt,
                    similar.new_sku_discount_rate_similar AS new_sku_discount_rate_similar,
                    similar.gw_retail_amt_similar AS gw_retail_amt_similar,
                    similar.top_sales_upt_similar AS top_sales_upt_similar,
                    store.inv_qty AS inv_qty,
                    store.vip_repurchase_cnt AS vip_repurchase_cnt,
                    similar.large_screen_sale_amt_pct_similar AS large_screen_sale_amt_pct_similar,
                    scorecard.skuEfficency_sc_label AS skuEfficency_sc_label,
                    task.task_tgt_qty AS task_tgt_qty,
                    similar.yun_store_sale_amt_similar AS yun_store_sale_amt_similar,
                    store.sales_qty_for_upt AS sales_qty_for_upt,
                    similar.plural_retail_cnt_pct_similar AS plural_retail_cnt_pct_similar,
                    similar.deal_rate_similar AS deal_rate_similar,
                    similar.offline_amt_pct_similar AS offline_amt_pct_similar,
                    store.plural_retail_cnt AS plural_retail_cnt,
                    member_similar.new_consume_member_qty_similar AS new_consume_member_qty_similar,
                    store.yun_store_sale_amt AS yun_store_sale_amt,
                    store.gw_retail_amt AS gw_retail_amt,
                    store.sales_qty AS sales_qty,
                    similar.singular_discount_rate_similar AS singular_discount_rate_similar,
                    store.sales_amt_list AS sales_amt_list,
                    similar.discount_rate_similar AS discount_rate_similar,
                    scorecard.taskExecuteRate_sc_label AS taskExecuteRate_sc_label,
                    similar.total_amount_similar AS total_amount_similar,
                    store.top_sales_amt_actual_pct AS top_sales_amt_actual_pct,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top20_sales_match_rate AS top20_sales_match_rate,
                    similar.top20_match_rate_similar AS top20_sales_match_rate_similar,
                    member.new_reg_member_qty AS new_reg_member_qty,
                    member_similar.new_reg_member_qty_similar AS new_reg_member_qty_similar,
                    member.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty,
                    member_similar.new_reg_has_consume_member_qty_similar AS new_reg_has_consume_member_qty_similar,
                    store.sku_unbroken_rate AS sku_unbroken_rate,
                    similar.sku_unbroken_rate_similar AS sku_unbroken_rate_similar,
                    member_similar.new_reg_has_consume_member_rate_similar AS new_reg_has_consume_ratio_similar,
                    member.high_val_member_consume_member_qty AS high_val_member_consume_member_qty,
                    member_similar.high_val_member_consume_member_qty_similar AS high_val_member_consume_member_qty_similar,
                    birth_member_coupon.issue_qty AS birth_issue_qty,
                    birth_coupon_similar.issue_qty_similar AS birth_issue_qty_similar,
                    birth_member_coupon.write_off_qty AS birth_write_off_qty,
                    birth_coupon_similar.write_off_qty_similar AS birth_write_off_qty_similar,
                    birth_coupon_similar.write_off_rate_similar AS birth_write_off_rate_similar,
                    member.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty,
                    member_similar.new_reg_non_consume_member_qty_similar AS new_reg_non_consume_member_qty_similar,
                    member_similar.new_reg_non_consume_member_rate_similar as new_reg_non_consume_ratio_similar,
                    member.high_val_member_consume_amt AS high_val_member_consume_amt,
                    member_similar.high_val_member_consume_amt_similar AS high_val_member_consume_amt_similar,
                    member.high_val_member_order_qty AS high_val_member_order_qty,
                    member_similar.high_val_member_order_qty_similar AS high_val_member_order_qty_similar,
                    member_similar.high_val_member_atv_similar AS high_val_member_atv_similar,
                    member.curr_month_new_member_order_qty AS curr_month_new_member_order_qty,
                    member.curr_month_old_member_order_qty AS curr_month_old_member_order_qty,
                    member.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty,
                    member_similar.barg_new_cust_reg_pct_similar AS barg_new_cust_reg_pct_similar,
                    member.old_member_consume_amt AS old_member_consume_amt,
                    member_similar.old_member_consume_amt_similar AS old_member_consume_amt_similar,
                    member.old_member_order_qty AS old_member_order_qty,
                    member_similar.old_member_order_qty_similar AS old_member_order_qty_similar,
                    member_similar.old_member_atv_similar AS old_member_atv_similar,
                    member.new_member_consume_amt AS new_member_consume_amt,
                    member_similar.new_member_consume_amt_similar AS new_member_consume_amt_similar,
                    member.new_member_order_qty AS new_member_order_qty,
                    member_similar.new_member_order_qty_similar AS new_member_order_qty_similar,
                    member_similar.new_member_atv_similar AS new_member_atv_similar,
                    member.member_consume_amt AS member_consume_amt,
                    member_similar.member_consume_amt_similar AS member_consume_amt_similar,
                    member_similar.high_val_member_consume_amt_pct_similar AS high_val_member_consume_amt_pct_similar,
                    member_similar.old_member_consume_amt_pct_similar AS old_member_consume_amt_pct_similar,
                    member_similar.new_member_consume_amt_pct_similar AS new_member_consume_amt_pct_similar,
                    member_similar.new_consume_member_qty_pct_similar AS new_consume_member_qty_pct_similar,
                    member_similar.old_consume_member_qty_pct_similar AS old_consume_member_qty_pct_similar,
                    member_similar.high_val_member_consume_member_qty_pct_similar AS high_val_member_consume_member_qty_pct_similar,
                    feature_target.numOfPurNewMember_target AS numOfPurNewMember_target,
                    feature_target.numOfPurMember_target AS numOfPurMember_target,
                    feature_target.numOfPurExistMember_target AS numOfPurExistMember_target,

                    feature_target.new_reg_member_qty_target AS new_reg_member_qty_target,
                    feature_target.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target,
                    feature_target.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target
                from (select
                            *
                            from
                            retail_consultant_db.t_ads_store_monthly_stats
                            where tenant_id = 'anta'
                            and stats_date = #{curStatisTm}
                            and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                ) store join ( select
                                    *
                                    from retail_consultant_db.t_ads_store_scd
                                    where tenant_id = 'anta'
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                ) scd on store.tenant_id=scd.tenant_id
                and store.store_id=scd.store_id
                left outer join (select tenant_id, rel_id store_id, stats_date,brand_name,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'sales' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 5)   end)  sales_target,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurNewMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurNewMember_target,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurMember_target,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurExistMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurExistMember_target,
                                        null as new_reg_member_qty_target,
                                        null as new_reg_has_consume_member_rate_target,
                                        null as high_val_member_consume_member_qty_target
                                 from
                                        retail_consultant_db.t_sc_feature_month_target_info
                                        where tenant_id = 'anta'
                                        and stats_date = #{curStatisTm}
                                        and rel_type='STORE' and del_flag=0
                                        and rel_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by tenant_id,rel_id,stats_date,brand_name
                ) feature_target on store.tenant_id=feature_target.tenant_id
                and store.store_id=feature_target.store_id
                and toDate(store.stats_date)=feature_target.stats_date
                and store.brand_name=feature_target.brand_name
                left outer join (select
                                        tenant_id,
                                        region_id,
                                        calc_begin_date,
                                        brand_name,
                                        max(case when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurExistMember_sc_label,
                                        max(case when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  matchDegTopProduct_sc_label,
                                        max(case when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurNewMember_sc_label,
                                        max(case when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  topFullSizeRate_sc_label,
                                        max(case when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  couponUsageRate_sc_label,
                                        max(case when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  taskExecuteRate_sc_label,
                                        max(case when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesTnumStaffs_sc_label,
                                        max(case when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesOfInSeason_sc_label,
                                        max(case when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurMember_sc_label,
                                        max(case when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  skuEfficency_sc_label,
                                        max(case when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'ANOMALY' then '异常' end)  topSalesPct_sc_label,
                                        max(case when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  sales_sc_label,
                                        max(case when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  atv_sc_label,
                                        max(case when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  upt_sc_label,
                                        max(case when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  cr_sc_label,
                                        max(case when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  md_sc_label
                                        from retail_consultant_db.t_ads_feature_score_card feature_scorecard
                                        where tenant_id = 'anta'
                                        and region_type='STORE'
                                        and cycle = 'MONTH_TOTAL'
                                        and calc_begin_date = #{curStatisTm}
                                        and region_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by tenant_id,region_id,calc_begin_date,brand_name
                ) scorecard on store.tenant_id=scorecard.tenant_id
                and store.store_id=scorecard.region_id
                and toDate(store.stats_date)=scorecard.calc_begin_date
                and store.brand_name=scorecard.brand_name
                left outer join (select
                                    member.tenant_id tenant_id,
                                    member.stats_date stats_date,
                                    member.store_id store_id,
                                    member.brand_name brand_name,
                                    sum(member.consume_member_qty) consume_member_qty,
                                    sum(member.old_consume_member_qty) old_consume_member_qty,
                                    sum(member.new_consume_member_qty) new_consume_member_qty,
                                    sum(member.new_reg_member_qty) as new_reg_member_qty,
                                    sum(member.new_reg_has_consume_member_qty) new_reg_has_consume_member_qty,
                                    sum(member.high_val_member_consume_member_qty) high_val_member_consume_member_qty,
                                    sum(member.new_reg_non_consume_member_qty) new_reg_non_consume_member_qty,
                                    sum(member.high_val_member_consume_amt) high_val_member_consume_amt,
                                    sum(member.high_val_member_order_qty) high_val_member_order_qty,
                                    sum(member.curr_month_new_member_order_qty) curr_month_new_member_order_qty,
                                    sum(member.curr_month_old_member_order_qty) curr_month_old_member_order_qty,
                                    sum(member.curr_month_total_barg_order_qty) curr_month_total_barg_order_qty,
                                    sum(member.old_member_consume_amt) old_member_consume_amt,
                                    sum(member.old_member_order_qty) old_member_order_qty,
                                    sum(member.new_member_consume_amt) new_member_consume_amt,
                                    sum(member.new_member_order_qty) new_member_order_qty,
                                    sum(member.member_consume_amt) member_consume_amt
                                    from retail_consultant_db.t_ads_crm_mco_store_statis member
                                    where tenant_id = 'anta'
                                    and stats_date = #{curStatisTm}
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                    group by tenant_id,stats_date,store_id,brand_name
                ) member on store.tenant_id=member.tenant_id
                and store.store_id=member.store_id
                and store.stats_date=member.stats_date
                and store.brand_name=member.brand_name
                left outer join (select
                                    member_coupon.brand_name brand_name,
                                    member_coupon.store_id store_id,
                                    member_coupon.tenant_id tenant_id,
                                    member_coupon.stats_date stats_date,
                                    sum(member_coupon.issue_qty) issue_qty,
                                    sum(member_coupon.write_off_qty) write_off_qty
                                    from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                    where tenant_id = 'anta'
                                    and stats_date = #{curStatisTm}
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                    group by brand_name,store_id,tenant_id,stats_date
                ) member_coupon on store.tenant_id=member_coupon.tenant_id
                and store.store_id=member_coupon.store_id
                and store.stats_date=member_coupon.stats_date
                and store.brand_name=member_coupon.brand_name
                left outer join (select
                                    member_coupon.brand_name brand_name,
                                    member_coupon.store_id store_id,
                                    member_coupon.tenant_id tenant_id,
                                    member_coupon.stats_date stats_date,
                                    sum(member_coupon.issue_qty) issue_qty,
                                    sum(member_coupon.write_off_qty) write_off_qty
                                    from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                    where tenant_id = 'anta'
                                    and stats_date = #{curStatisTm} and coupon_categ_desc = '生日券'
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                    group by brand_name,store_id,tenant_id,stats_date
                ) birth_member_coupon on store.tenant_id=birth_member_coupon.tenant_id
                and store.store_id=birth_member_coupon.store_id
                and store.stats_date=birth_member_coupon.stats_date
                and store.brand_name=birth_member_coupon.brand_name
                left outer join (select
                                    task.stats_date stats_date,
                                    task.tenant_id tenant_id,
                                    task.store_id store_id,
                                    task.brand_name brand_name,
                                    sum(task.exec_qty) exec_qty,
                                    sum(
                                        case
                                        when task.task_type_desc = 'single_talk' then task.task_tgt_qty
                                        else 0
                                        end
                                    ) task_tgt_qty_single_talk,
                                    toInt32(
                                        countDistinctIf(guide_id, task.task_type_desc = 'friends_area')
                                    ) guide_count,
                                    date_diff('day', toDate(#{curStatisTm}), toDate(#{endStatisTm})) + 1 days,
                                    guide_count * days * 3 task_tgt_qty_friends_area,
                                    task_tgt_qty_single_talk + task_tgt_qty_friends_area task_tgt_qty
                                from retail_consultant_db.t_ads_crm_mco_guide_task_statis task
                                where true
                                    and tenant_id = 'anta'
                                    and stats_date = #{curStatisTm}
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                group by stats_date,tenant_id,store_id,brand_name
                ) task on store.tenant_id=task.tenant_id
                and store.store_id=task.store_id
                and store.stats_date=task.stats_date
                and store.brand_name=task.brand_name
                left outer join (select *
                                    from
                                    retail_consultant_db.t_ads_similar_store_monthly_stats
                                    where tenant_id = 'anta'
                                    and stats_date = #{curStatisTm}
                ) similar
                on  store.tenant_id = similar.tenant_id
                and  store.store_type = similar.store_type
                and  store.stats_date = similar.stats_date
                and  store.brand_name = similar.brand_name
                left outer join (select *
                                    from
                                    retail_consultant_db.dm_crm_mco_store_statis_similar
                                    where statis_tm = #{curStatisTm} and store_type_class != ''
                ) member_similar
                on   store.store_type = member_similar.store_type_class
                and  store.stats_date = member_similar.statis_tm
                and  store.brand_name = member_similar.brand_name
                left outer join (select
                                    *
                                    from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                    where statis_tm = #{curStatisTm} and store_type_class != ''
                                    and coupon_categ_desc = 'ALL'
                ) coupon_similar on store.store_type = coupon_similar.store_type_class
                and  store.stats_date = coupon_similar.statis_tm
                and  store.brand_name = coupon_similar.brand_name
                left outer join (select
                                    statis_tm,
                                    brand_name,
                                    sum(issue_qty_similar) as issue_qty_similar,
                                    sum(write_off_qty_similar) as write_off_qty_similar,
        case when sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) != 0 then sum(dm_crm_mco_coupon_statis_similar.write_off_qty_similar)/sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) end as write_off_rate_similar
        from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                    where statis_tm = #{curStatisTm} and store_type_class != ''
                                    and coupon_categ_desc = '生日券'
                                    group by statis_tm, brand_name
                ) birth_coupon_similar on store.store_type = coupon_similar.store_type_class
                and  store.stats_date = birth_coupon_similar.statis_tm
                and  store.brand_name = birth_coupon_similar.brand_name
                left outer join (select
                                    *
                                    from retail_consultant_db.dm_crm_mco_guide_task_statis_similar
                                    where true
                                    and statis_tm = #{curStatisTm} and store_type_class != ''
                                    and task_type_desc = 'ALL' AND task_status = 'ALL'
                ) task_similar
                on   store.store_type = task_similar.store_type_class
                and  store.stats_date = task_similar.statis_tm
                and  store.brand_name = task_similar.brand_name
           ) cur left join
           (
                select
                    store.cms_code AS cms_code,
                    scd.brand_store_code AS brand_store_code,
                    scd.temperature_area AS temperature_area,
                    scd.store_nature AS store_nature,
                    scd.store_type_name AS store_type_name,
                    scd.shopLevel_adjust AS shopLevel_adjust,
                    scd.org_branch_name AS org_branch_name,
                    store.stats_date AS stats_date,
                    scd.name AS name,
                    scd.address AS address,
                    scd.province_name AS province_name,
                    scd.join_type AS join_type,
                    scd.company_type AS company_type,
                    store.store_id AS store_id,
                    scd.key_store AS key_store,
                    scd.opening_date AS opening_date,
                    scd.store_type AS store_type,
                    scd.expand_type AS expand_type,
                    scd.store_image_name AS store_image_name,
                    scd.rack_type AS rack_type,
                    scd.channel_brand_code AS channel_brand_code,
                    scd.city_name AS city_name,
                    scd.tenant_id AS tenant_id,
                    scd.is_retail AS is_retail,
                    scd.district_name AS district_name,
                    scd.level AS level,
                    scd.org_office_name AS org_office_name,
                    store.brand_name AS brand_name,
                    scd.store_level_name AS store_level_name,
                    scd.org_area_name AS org_area_name,
                    scd.business_status AS business_status,
                    store.new_good_dynamic_rate AS new_good_dynamic_rate,
                    similar.vip_repurchase_cnt_similar AS vip_repurchase_cnt_similar,
                    similar.inv_sale_rate_similar AS inv_sale_rate_similar,
                    similar.upt_similar AS upt_similar,
                    store.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate,
                    similar.sales_amt_actual_per_staff_similar AS sales_amt_actual_per_staff_similar,
                    similar.aur_similar AS aur_similar,
                    similar.top50_sku_unbroken_rate_similar AS top50_sku_unbroken_rate_similar,
                    similar.one_to_seven_dynamic_rate_similar AS one_to_seven_dynamic_rate_similar,
                    store.shoes_clothing_sales_qty AS shoes_clothing_sales_qty,
                    similar.top50_sales_inv_match_rate_similar AS top50_sales_inv_match_rate_similar,
                    coupon_similar.write_off_rate_similar AS write_off_rate_similar,
                    store.thousand_retail_cnt_pct AS thousand_retail_cnt_pct,
                    similar.vip_repurchase_duration_similar AS vip_repurchase_duration_similar,
                    similar.sales_amt_list_similar AS sales_amt_list_similar,
                    store.retail_cnt AS retail_cnt,
                    store.top50_match_cnt AS top50_match_cnt,
                    di_local_pre_month.sales_amt_actual_in_season AS sales_amt_actual_in_season,
                    store.inside_outside_ratio AS inside_outside_ratio,
                    similar.plural_retail_cnt_similar AS plural_retail_cnt_similar,
                    store.singular_tot_amt_actual AS singular_tot_amt_actual,
                    member_coupon.issue_qty AS issue_qty,
                    scorecard.numOfPurMember_sc_label AS numOfPurMember_sc_label,
                    store.top_sales_upt AS top_sales_upt,
                    store.stay_sku_cnt AS stay_sku_cnt,
                    store.vip_retail_pct AS vip_retail_pct,
                    scd.shop_area AS shop_area,
                    task.exec_qty AS exec_qty,
                    similar.gw_retail_amt_pct_similar AS gw_retail_amt_pct_similar,
                    scorecard.couponUsageRate_sc_label AS couponUsageRate_sc_label,
                    similar.plural_tot_amt_list_similar AS plural_tot_amt_list_similar,
                    similar.sales_amt_actual_similar AS sales_amt_actual_similar,
                    scorecard.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label,
                    scorecard.salesOfInSeason_sc_label AS salesOfInSeason_sc_label,
                    member.consume_member_qty AS consume_member_qty,
                    similar.avg_unit_inv_list_similar AS avg_unit_inv_list_similar,
                    store.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt,
                    similar.retial_cnt_per_staff_similar AS retial_cnt_per_staff_similar,
                    di_local_pre_month.sales_amt_actual AS sales_amt_actual,
                    store.inv_amt AS inv_amt,
                    similar.singular_retail_cnt_similar AS singular_retail_cnt_similar,
                    member_coupon.write_off_qty AS write_off_qty,
                    is_homo_di_local_pre_month.top50_art_no_cnt AS top50_art_no_cnt,
                    scorecard.topFullSizeRate_sc_label AS topFullSizeRate_sc_label,
                    similar.plural_tot_amt_actual_similar AS plural_tot_amt_actual_similar,
                    similar.offline_amt_similar AS offline_amt_similar,
                    di_local_pre_month.large_screen_sale_amt AS large_screen_sale_amt,
                    similar.sale_deep_similar AS sale_deep_similar,
                    store.plural_tot_amt_actual AS plural_tot_amt_actual,
                    similar.inside_outside_ratio_similar AS inside_outside_ratio_similar,
                    similar.yun_store_sale_amt_pct_similar AS yun_store_sale_amt_pct_similar,
                    similar.stay_sku_cnt_similar AS stay_sku_cnt_similar,
                    scorecard.sales_sc_label AS sales_sc_label,
                    store.shoes_clothes_ratio AS shoes_clothes_ratio,
                    scorecard.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label,
                    member.new_consume_member_qty AS new_consume_member_qty,
                    scorecard.atv_sc_label AS atv_sc_label,
                    member_similar.consume_member_qty_similar AS consume_member_qty_similar,
                    scorecard.cr_sc_label AS cr_sc_label,
                    scorecard.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label,
                    member.old_consume_member_qty AS old_consume_member_qty,
                    store.singular_tot_amt_list AS singular_tot_amt_list,
                    similar.singular_tot_amt_list_similar AS singular_tot_amt_list_similar,
                    similar.upt_excl_acc_similar AS upt_excl_acc_similar,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top50_sales_amt_actual AS top50_sales_amt_actual,
                    similar.dynamic_sale_rate_similar AS dynamic_sale_rate_similar,
                    sku_di_local_pre_month.total_sku_cnt AS total_sku_cnt,
                    similar.new_good_dynamic_rate_similar AS new_good_dynamic_rate_similar,
                    member_similar.old_consume_member_qty_similar AS old_consume_member_qty_similar,
                    similar.atv_similar AS atv_similar,
                    task_similar.exec_rate_similar AS exec_rate_similar,
                    similar.shoes_qty_ge2_receipt_ratio_similar AS shoes_qty_ge2_receipt_ratio_similar,
                    di_local_pre_month.have_sale_staff_cnt AS have_sale_staff_cnt,
                    similar.amt_ge1000_receipt_ratio_similar AS amt_ge1000_receipt_ratio_similar,
                    scorecard.md_sc_label AS md_sc_label,
                    similar.plural_discount_rate_similar AS plural_discount_rate_similar,
                    similar.discount_rate_diff_similar AS discount_rate_diff_similar,
                    scorecard.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label,
                    store.plural_tot_amt_list AS plural_tot_amt_list,
                    is_homo_di_local_pre_month.top50_is_homo_cnt AS top50_is_homo_cnt,
                    sku_di_local_pre_month.sales_sku_cnt AS sales_sku_cnt,
                    similar.singular_tot_amt_actual_similar AS singular_tot_amt_actual_similar,
                    scorecard.upt_sc_label AS upt_sc_label,
                    scorecard.topSalesPct_sc_label AS topSalesPct_sc_label,
                    store.vip_repurchase_duration AS vip_repurchase_duration,
                    store.inv_sale_rate AS inv_sale_rate,
                    store.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct,
                    feature_target.sales_target AS sales_target,
                    similar.large_screen_sale_amt_similar AS large_screen_sale_amt_similar,
                    store.singular_retail_cnt AS singular_retail_cnt,
                    similar.vip_retail_pct_similar AS vip_retail_pct_similar,
                    similar.shoes_clothes_ratio_similar AS shoes_clothes_ratio_similar,
                    similar.sales_amt_actual_in_season_pct_similar AS sales_amt_actual_in_season_pct_similar,
                    store.sales_amt_list_in_season AS sales_amt_list_in_season,
                    di_local_pre_month.cust_order_qty AS retail_cnt_have_customer,
                    di_local_pre_month.cust_count AS customer_cnt,
                    similar.new_sku_discount_rate_similar AS new_sku_discount_rate_similar,
                    similar.gw_retail_amt_similar AS gw_retail_amt_similar,
                    similar.top_sales_upt_similar AS top_sales_upt_similar,
                    store.inv_qty AS inv_qty,
                    store.vip_repurchase_cnt AS vip_repurchase_cnt,
                    similar.large_screen_sale_amt_pct_similar AS large_screen_sale_amt_pct_similar,
                    scorecard.skuEfficency_sc_label AS skuEfficency_sc_label,
                    task.task_tgt_qty AS task_tgt_qty,
                    similar.yun_store_sale_amt_similar AS yun_store_sale_amt_similar,
                    di_local_pre_month.sales_qty_for_upt AS sales_qty_for_upt,
                    similar.plural_retail_cnt_pct_similar AS plural_retail_cnt_pct_similar,
                    similar.deal_rate_similar AS deal_rate_similar,
                    similar.offline_amt_pct_similar AS offline_amt_pct_similar,
                    store.plural_retail_cnt AS plural_retail_cnt,
                    member_similar.new_consume_member_qty_similar AS new_consume_member_qty_similar,
                    di_local_pre_month.yun_store_sale_amt AS yun_store_sale_amt,
                    di_local_pre_month.gw_retail_amt AS gw_retail_amt,
                    store.sales_qty AS sales_qty,
                    similar.singular_discount_rate_similar AS singular_discount_rate_similar,
                    di_local_pre_month.sales_amt_list AS sales_amt_list,
                    similar.discount_rate_similar AS discount_rate_similar,
                    scorecard.taskExecuteRate_sc_label AS taskExecuteRate_sc_label,
                    similar.total_amount_similar AS total_amount_similar,
                    sales_pct_di_local_pre_month.top_sales_amt_actual_pct AS top_sales_amt_actual_pct,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    di_local_pre_month.top20_sales_match_rate AS top20_sales_match_rate,
                    similar.top20_match_rate_similar AS top20_sales_match_rate_similar,
                    member.new_reg_member_qty AS new_reg_member_qty,
                    member_similar.new_reg_member_qty_similar AS new_reg_member_qty_similar,
                    member.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty,
                    member_similar.new_reg_has_consume_member_qty_similar AS new_reg_has_consume_member_qty_similar,
                    store.sku_unbroken_rate AS sku_unbroken_rate,
                    similar.sku_unbroken_rate_similar AS sku_unbroken_rate_similar,
                    member_similar.new_reg_has_consume_member_rate_similar AS new_reg_has_consume_ratio_similar,
                    member.high_val_member_consume_member_qty AS high_val_member_consume_member_qty,
                    member_similar.high_val_member_consume_member_qty_similar AS high_val_member_consume_member_qty_similar,
                    birth_member_coupon.issue_qty AS birth_issue_qty,
                    birth_coupon_similar.issue_qty_similar AS birth_issue_qty_similar,
                    birth_member_coupon.write_off_qty AS birth_write_off_qty,
                    birth_coupon_similar.write_off_qty_similar AS birth_write_off_qty_similar,
                    birth_coupon_similar.write_off_rate_similar AS birth_write_off_rate_similar,
                    member.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty,
                    member_similar.new_reg_non_consume_member_qty_similar AS new_reg_non_consume_member_qty_similar,
                    member_similar.new_reg_non_consume_member_rate_similar as new_reg_non_consume_ratio_similar,
                    member.high_val_member_consume_amt AS high_val_member_consume_amt,
                    member_similar.high_val_member_consume_amt_similar AS high_val_member_consume_amt_similar,
                    member.high_val_member_order_qty AS high_val_member_order_qty,
                    member_similar.high_val_member_order_qty_similar AS high_val_member_order_qty_similar,
                    member_similar.high_val_member_atv_similar AS high_val_member_atv_similar,
                    member.curr_month_new_member_order_qty AS curr_month_new_member_order_qty,
                    member.curr_month_old_member_order_qty AS curr_month_old_member_order_qty,
                    member.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty,
                    member_similar.barg_new_cust_reg_pct_similar AS barg_new_cust_reg_pct_similar,
                    member.old_member_consume_amt AS old_member_consume_amt,
                    member_similar.old_member_consume_amt_similar AS old_member_consume_amt_similar,
                    member.old_member_order_qty AS old_member_order_qty,
                    member_similar.old_member_order_qty_similar AS old_member_order_qty_similar,
                    member_similar.old_member_atv_similar AS old_member_atv_similar,
                    member.new_member_consume_amt AS new_member_consume_amt,
                    member_similar.new_member_consume_amt_similar AS new_member_consume_amt_similar,
                    member.new_member_order_qty AS new_member_order_qty,
                    member_similar.new_member_order_qty_similar AS new_member_order_qty_similar,
                    member_similar.new_member_atv_similar AS new_member_atv_similar,
                    member.member_consume_amt AS member_consume_amt,
                    member_similar.member_consume_amt_similar AS member_consume_amt_similar,
                    member_similar.high_val_member_consume_amt_pct_similar AS high_val_member_consume_amt_pct_similar,
                    member_similar.old_member_consume_amt_pct_similar AS old_member_consume_amt_pct_similar,
                    member_similar.new_member_consume_amt_pct_similar AS new_member_consume_amt_pct_similar,
                    member_similar.new_consume_member_qty_pct_similar AS new_consume_member_qty_pct_similar,
                    member_similar.old_consume_member_qty_pct_similar AS old_consume_member_qty_pct_similar,
                    member_similar.high_val_member_consume_member_qty_pct_similar AS high_val_member_consume_member_qty_pct_similar,
                    feature_target.numOfPurNewMember_target AS numOfPurNewMember_target,
                    feature_target.numOfPurMember_target AS numOfPurMember_target,
                    feature_target.numOfPurExistMember_target AS numOfPurExistMember_target,

                    feature_target.new_reg_member_qty_target AS new_reg_member_qty_target,
                    feature_target.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target,
                    feature_target.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target
                from (select
                            *
                            from
                            retail_consultant_db.t_ads_store_monthly_stats
                            where tenant_id = 'anta'
                            and stats_date = #{preMonthStatisTm}
                            and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                ) store join ( select
                                    *
                                    from retail_consultant_db.t_ads_store_scd
                                    where tenant_id = 'anta'
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                ) scd on store.tenant_id=scd.tenant_id
                and store.store_id=scd.store_id
                left outer join (select tenant_id, rel_id store_id, stats_date,brand_name,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'sales' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 5)   end)  sales_target,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurNewMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurNewMember_target,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurMember_target,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurExistMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurExistMember_target,
                                        null as new_reg_member_qty_target,
                                        null as new_reg_has_consume_member_rate_target,
                                        null as high_val_member_consume_member_qty_target
                                 from
                                        retail_consultant_db.t_sc_feature_month_target_info
                                        where tenant_id = 'anta'
                                        and stats_date = #{preMonthStatisTm}
                                        and rel_type='STORE' and del_flag=0
                                        and rel_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by tenant_id,rel_id,stats_date,brand_name
                ) feature_target on store.tenant_id=feature_target.tenant_id
                and store.store_id=feature_target.store_id
                and toDate(store.stats_date)=feature_target.stats_date
                and store.brand_name=feature_target.brand_name
                left outer join (select
                                        tenant_id,
                                        region_id,
                                        calc_begin_date,
                                        brand_name,
                                        max(case when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurExistMember_sc_label,
                                        max(case when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  matchDegTopProduct_sc_label,
                                        max(case when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurNewMember_sc_label,
                                        max(case when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  topFullSizeRate_sc_label,
                                        max(case when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  couponUsageRate_sc_label,
                                        max(case when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  taskExecuteRate_sc_label,
                                        max(case when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesTnumStaffs_sc_label,
                                        max(case when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesOfInSeason_sc_label,
                                        max(case when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurMember_sc_label,
                                        max(case when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  skuEfficency_sc_label,
                                        max(case when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'ANOMALY' then '异常' end)  topSalesPct_sc_label,
                                        max(case when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  sales_sc_label,
                                        max(case when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  atv_sc_label,
                                        max(case when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  upt_sc_label,
                                        max(case when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  cr_sc_label,
                                        max(case when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  md_sc_label
                                        from retail_consultant_db.t_ads_feature_score_card feature_scorecard
                                        where tenant_id = 'anta'
                                        and region_type='STORE'
                                        and cycle = 'MONTH_TOTAL'
                                        and calc_begin_date = #{preMonthStatisTm}
                                        and region_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by tenant_id,region_id,calc_begin_date,brand_name
                ) scorecard on store.tenant_id=scorecard.tenant_id
                and store.store_id=scorecard.region_id
                and toDate(store.stats_date)=scorecard.calc_begin_date
                and store.brand_name=scorecard.brand_name
                left outer join (select
                                        member.tenant_id tenant_id,
                                        member.stats_date stats_date,
                                        member.store_id store_id,
                                        member.brand_name brand_name,
                                        sum(member.consume_member_qty) consume_member_qty,
                                        sum(member.old_consume_member_qty) old_consume_member_qty,
                                        sum(member.new_consume_member_qty) new_consume_member_qty,
                                        sum(member.new_reg_member_qty) as new_reg_member_qty,
                                        sum(member.new_reg_has_consume_member_qty) new_reg_has_consume_member_qty,
                                        sum(member.high_val_member_consume_member_qty) high_val_member_consume_member_qty,
                                        sum(member.new_reg_non_consume_member_qty) new_reg_non_consume_member_qty,
                                        sum(member.high_val_member_consume_amt) high_val_member_consume_amt,
                                        sum(member.high_val_member_order_qty) high_val_member_order_qty,
                                        sum(member.curr_month_new_member_order_qty) curr_month_new_member_order_qty,
                                        sum(member.curr_month_old_member_order_qty) curr_month_old_member_order_qty,
                                        sum(member.curr_month_total_barg_order_qty) curr_month_total_barg_order_qty,
                                        sum(member.old_member_consume_amt) old_member_consume_amt,
                                        sum(member.old_member_order_qty) old_member_order_qty,
                                        sum(member.new_member_consume_amt) new_member_consume_amt,
                                        sum(member.new_member_order_qty) new_member_order_qty,
                                        sum(member.member_consume_amt) member_consume_amt
                                        from retail_consultant_db.t_ads_crm_mco_store_statis member
                                        where tenant_id = 'anta'
                                        and stats_date = #{preMonthStatisTm}
                                        and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by tenant_id,stats_date,store_id,brand_name
                ) member on store.tenant_id=member.tenant_id
                and store.store_id=member.store_id
                and store.stats_date=member.stats_date
                and store.brand_name=member.brand_name
                left outer join (select
                                    member_coupon.brand_name brand_name,
                                    member_coupon.store_id store_id,
                                    member_coupon.tenant_id tenant_id,
                                    member_coupon.stats_date stats_date,
                                    sum(member_coupon.issue_qty) issue_qty,
                                    sum(member_coupon.write_off_qty) write_off_qty
                                    from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                    where tenant_id = 'anta'
                                    and stats_date = #{preMonthStatisTm}
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                    group by brand_name,store_id,tenant_id,stats_date
                ) member_coupon on store.tenant_id=member_coupon.tenant_id
                and store.store_id=member_coupon.store_id
                and store.stats_date=member_coupon.stats_date
                and store.brand_name=member_coupon.brand_name
                left outer join (select
                                        member_coupon.brand_name brand_name,
                                        member_coupon.store_id store_id,
                                        member_coupon.tenant_id tenant_id,
                                        member_coupon.stats_date stats_date,
                                        sum(member_coupon.issue_qty) issue_qty,
                                        sum(member_coupon.write_off_qty) write_off_qty
                                        from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                        where tenant_id = 'anta'
                                        and stats_date = #{preMonthStatisTm} and coupon_categ_desc = '生日券'
                                        and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by brand_name,store_id,tenant_id,stats_date
                ) birth_member_coupon on store.tenant_id=birth_member_coupon.tenant_id
                and store.store_id=birth_member_coupon.store_id
                and store.stats_date=birth_member_coupon.stats_date
                and store.brand_name=birth_member_coupon.brand_name
                left outer join (select
                                        task.stats_date stats_date,
                                        task.tenant_id tenant_id,
                                        task.store_id store_id,
                                        task.brand_name brand_name,
                                        sum(task.exec_qty) exec_qty,
                                        sum(
                                        case
                                        when task.task_type_desc = 'single_talk' then task.task_tgt_qty
                                        else 0
                                        end
                                        ) task_tgt_qty_single_talk,
                                        toInt32(
                                        countDistinctIf(guide_id, task.task_type_desc = 'friends_area')
                                        ) guide_count,
                                        date_diff('day', toDate(#{preMonthStatisTm}), toDate(#{endPreMonthStatisTm})) + 1 days,
                                        guide_count * days * 3 task_tgt_qty_friends_area,
                                        task_tgt_qty_single_talk + task_tgt_qty_friends_area task_tgt_qty
                                        from retail_consultant_db.t_ads_crm_mco_guide_task_statis task
                                        where true
                                        and tenant_id = 'anta'
                                        and stats_date = #{preMonthStatisTm}
                                        and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by stats_date,tenant_id,store_id,brand_name
                ) task on store.tenant_id=task.tenant_id
                and store.store_id=task.store_id
                and store.stats_date=task.stats_date
                and store.brand_name=task.brand_name
                left outer join (select *
                                        from
                                        retail_consultant_db.t_ads_similar_store_monthly_stats
                                        where tenant_id = 'anta'
                                        and stats_date = #{preMonthStatisTm}
                ) similar
                on  store.tenant_id = similar.tenant_id
                and  store.store_type = similar.store_type
                and  store.stats_date = similar.stats_date
                and  store.brand_name = similar.brand_name
                left outer join (select *
                                        from
                                        retail_consultant_db.dm_crm_mco_store_statis_similar
                                        where statis_tm = #{preMonthStatisTm} and store_type_class != ''
                ) member_similar
                on   store.store_type = member_similar.store_type_class
                and  store.stats_date = member_similar.statis_tm
                and  store.brand_name = member_similar.brand_name
                left outer join (select
                                        *
                                        from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                        where statis_tm = #{preMonthStatisTm} and store_type_class != ''
                                        and coupon_categ_desc = 'ALL'
                ) coupon_similar on store.store_type = coupon_similar.store_type_class
                and  store.stats_date = coupon_similar.statis_tm
                and  store.brand_name = coupon_similar.brand_name
                left outer join (select
                                statis_tm,
                                brand_name,
                                sum(issue_qty_similar) as issue_qty_similar,
                                sum(write_off_qty_similar) as write_off_qty_similar,
                                case when sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) != 0 then sum(dm_crm_mco_coupon_statis_similar.write_off_qty_similar)/sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) end as write_off_rate_similar
                                from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                where statis_tm = #{preMonthStatisTm} and store_type_class != ''
                                and coupon_categ_desc = '生日券'
                                group by statis_tm, brand_name
                ) birth_coupon_similar on store.store_type = coupon_similar.store_type_class
                and  store.stats_date = birth_coupon_similar.statis_tm
                and  store.brand_name = birth_coupon_similar.brand_name
                left outer join (select
                                        *
                                        from retail_consultant_db.dm_crm_mco_guide_task_statis_similar
                                        where true
                                        and statis_tm = #{preMonthStatisTm} and store_type_class != ''
                                        and task_type_desc = 'ALL' AND task_status = 'ALL'
                ) task_similar
                on   store.store_type = task_similar.store_type_class
                and  store.stats_date = task_similar.statis_tm
                and  store.brand_name = task_similar.brand_name
                left outer join (select *
                                      from di_local_pre_month_tmp
               ) di_local_pre_month on store.store_id = di_local_pre_month.store_id
                                    and store.brand_name=di_local_pre_month.brand_name
                left outer join (select *
                                    from sku_di_local_pre_month_tmp
                ) sku_di_local_pre_month on store.store_id = sku_di_local_pre_month.store_id
                                and store.brand_name=sku_di_local_pre_month.brand_name
                left outer join (select *
                                from sales_pct_di_local_pre_month_tmp
                ) sales_pct_di_local_pre_month on store.store_id = sales_pct_di_local_pre_month.store_id
                                and store.brand_name=sales_pct_di_local_pre_month.brand_name
                left outer join (select *
                                from is_homo_di_local_pre_month_tmp
                ) is_homo_di_local_pre_month on store.store_id = is_homo_di_local_pre_month.store_id
                                and store.brand_name=is_homo_di_local_pre_month.brand_name
        ) pre_month on cur.tenant_id=pre_month.tenant_id
        and cur.store_id=pre_month.store_id
        and cur.brand_name=pre_month.brand_name
        left join
        (
                select
                    store.cms_code AS cms_code,
                    scd.brand_store_code AS brand_store_code,
                    scd.temperature_area AS temperature_area,
                    scd.store_nature AS store_nature,
                    scd.store_type_name AS store_type_name,
                    scd.shopLevel_adjust AS shopLevel_adjust,
                    scd.org_branch_name AS org_branch_name,
                    store.stats_date AS stats_date,
                    scd.name AS name,
                    scd.address AS address,
                    scd.province_name AS province_name,
                    scd.join_type AS join_type,
                    scd.company_type AS company_type,
                    store.store_id AS store_id,
                    scd.key_store AS key_store,
                    scd.opening_date AS opening_date,
                    scd.store_type AS store_type,
                    scd.expand_type AS expand_type,
                    scd.store_image_name AS store_image_name,
                    scd.rack_type AS rack_type,
                    scd.channel_brand_code AS channel_brand_code,
                    scd.city_name AS city_name,
                    scd.tenant_id AS tenant_id,
                    scd.is_retail AS is_retail,
                    scd.district_name AS district_name,
                    scd.level AS level,
                    scd.org_office_name AS org_office_name,
                    store.brand_name AS brand_name,
                    scd.store_level_name AS store_level_name,
                    scd.org_area_name AS org_area_name,
                    scd.business_status AS business_status,
                    store.new_good_dynamic_rate AS new_good_dynamic_rate,
                    similar.vip_repurchase_cnt_similar AS vip_repurchase_cnt_similar,
                    similar.inv_sale_rate_similar AS inv_sale_rate_similar,
                    similar.upt_similar AS upt_similar,
                    store.one_to_seven_dynamic_rate AS one_to_seven_dynamic_rate,
                    similar.sales_amt_actual_per_staff_similar AS sales_amt_actual_per_staff_similar,
                    similar.aur_similar AS aur_similar,
                    similar.top50_sku_unbroken_rate_similar AS top50_sku_unbroken_rate_similar,
                    similar.one_to_seven_dynamic_rate_similar AS one_to_seven_dynamic_rate_similar,
                    store.shoes_clothing_sales_qty AS shoes_clothing_sales_qty,
                    similar.top50_sales_inv_match_rate_similar AS top50_sales_inv_match_rate_similar,
                    coupon_similar.write_off_rate_similar AS write_off_rate_similar,
                    store.thousand_retail_cnt_pct AS thousand_retail_cnt_pct,
                    similar.vip_repurchase_duration_similar AS vip_repurchase_duration_similar,
                    similar.sales_amt_list_similar AS sales_amt_list_similar,
                    store.retail_cnt AS retail_cnt,
                    store.top50_match_cnt AS top50_match_cnt,
                    di_local_pre_year.sales_amt_actual_in_season AS sales_amt_actual_in_season,
                    store.inside_outside_ratio AS inside_outside_ratio,
                    similar.plural_retail_cnt_similar AS plural_retail_cnt_similar,
                    store.singular_tot_amt_actual AS singular_tot_amt_actual,
                    member_coupon.issue_qty AS issue_qty,
                    scorecard.numOfPurMember_sc_label AS numOfPurMember_sc_label,
                    store.top_sales_upt AS top_sales_upt,
                    store.stay_sku_cnt AS stay_sku_cnt,
                    store.vip_retail_pct AS vip_retail_pct,
                    scd.shop_area AS shop_area,
                    task.exec_qty AS exec_qty,
                    similar.gw_retail_amt_pct_similar AS gw_retail_amt_pct_similar,
                    scorecard.couponUsageRate_sc_label AS couponUsageRate_sc_label,
                    similar.plural_tot_amt_list_similar AS plural_tot_amt_list_similar,
                    similar.sales_amt_actual_similar AS sales_amt_actual_similar,
                    scorecard.numOfPurNewMember_sc_label AS numOfPurNewMember_sc_label,
                    scorecard.salesOfInSeason_sc_label AS salesOfInSeason_sc_label,
                    member.consume_member_qty AS consume_member_qty,
                    similar.avg_unit_inv_list_similar AS avg_unit_inv_list_similar,
                    store.shoes_clothing_retail_cnt AS shoes_clothing_retail_cnt,
                    similar.retial_cnt_per_staff_similar AS retial_cnt_per_staff_similar,
                    di_local_pre_year.sales_amt_actual AS sales_amt_actual,
                    store.inv_amt AS inv_amt,
                    similar.singular_retail_cnt_similar AS singular_retail_cnt_similar,
                    member_coupon.write_off_qty AS write_off_qty,
                    is_homo_di_local_pre_year.top50_art_no_cnt AS top50_art_no_cnt,
                    scorecard.topFullSizeRate_sc_label AS topFullSizeRate_sc_label,
                    similar.plural_tot_amt_actual_similar AS plural_tot_amt_actual_similar,
                    similar.offline_amt_similar AS offline_amt_similar,
                    di_local_pre_year.large_screen_sale_amt AS large_screen_sale_amt,
                    similar.sale_deep_similar AS sale_deep_similar,
                    store.plural_tot_amt_actual AS plural_tot_amt_actual,
                    similar.inside_outside_ratio_similar AS inside_outside_ratio_similar,
                    similar.yun_store_sale_amt_pct_similar AS yun_store_sale_amt_pct_similar,
                    similar.stay_sku_cnt_similar AS stay_sku_cnt_similar,
                    scorecard.sales_sc_label AS sales_sc_label,
                    store.shoes_clothes_ratio AS shoes_clothes_ratio,
                    scorecard.matchDegTopProduct_sc_label AS matchDegTopProduct_sc_label,
                    member.new_consume_member_qty AS new_consume_member_qty,
                    scorecard.atv_sc_label AS atv_sc_label,
                    member_similar.consume_member_qty_similar AS consume_member_qty_similar,
                    scorecard.cr_sc_label AS cr_sc_label,
                    scorecard.numOfPurExistMember_sc_label AS numOfPurExistMember_sc_label,
                    member.old_consume_member_qty AS old_consume_member_qty,
                    store.singular_tot_amt_list AS singular_tot_amt_list,
                    similar.singular_tot_amt_list_similar AS singular_tot_amt_list_similar,
                    similar.upt_excl_acc_similar AS upt_excl_acc_similar,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    store.top50_sales_amt_actual AS top50_sales_amt_actual,
                    similar.dynamic_sale_rate_similar AS dynamic_sale_rate_similar,
                    sku_di_local_pre_year.total_sku_cnt AS total_sku_cnt,
                    similar.new_good_dynamic_rate_similar AS new_good_dynamic_rate_similar,
                    member_similar.old_consume_member_qty_similar AS old_consume_member_qty_similar,
                    similar.atv_similar AS atv_similar,
                    task_similar.exec_rate_similar AS exec_rate_similar,
                    similar.shoes_qty_ge2_receipt_ratio_similar AS shoes_qty_ge2_receipt_ratio_similar,
                    di_local_pre_year.have_sale_staff_cnt AS have_sale_staff_cnt,
                    similar.amt_ge1000_receipt_ratio_similar AS amt_ge1000_receipt_ratio_similar,
                    scorecard.md_sc_label AS md_sc_label,
                    similar.plural_discount_rate_similar AS plural_discount_rate_similar,
                    similar.discount_rate_diff_similar AS discount_rate_diff_similar,
                    scorecard.salesTnumStaffs_sc_label AS salesTnumStaffs_sc_label,
                    store.plural_tot_amt_list AS plural_tot_amt_list,
                    is_homo_di_local_pre_year.top50_is_homo_cnt AS top50_is_homo_cnt,
                    sku_di_local_pre_year.sales_sku_cnt AS sales_sku_cnt,
                    similar.singular_tot_amt_actual_similar AS singular_tot_amt_actual_similar,
                    scorecard.upt_sc_label AS upt_sc_label,
                    scorecard.topSalesPct_sc_label AS topSalesPct_sc_label,
                    store.vip_repurchase_duration AS vip_repurchase_duration,
                    store.inv_sale_rate AS inv_sale_rate,
                    store.two_pairs_shoes_retail_cnt_pct AS two_pairs_shoes_retail_cnt_pct,
                    feature_target.sales_target AS sales_target,
                    similar.large_screen_sale_amt_similar AS large_screen_sale_amt_similar,
                    store.singular_retail_cnt AS singular_retail_cnt,
                    similar.vip_retail_pct_similar AS vip_retail_pct_similar,
                    similar.shoes_clothes_ratio_similar AS shoes_clothes_ratio_similar,
                    similar.sales_amt_actual_in_season_pct_similar AS sales_amt_actual_in_season_pct_similar,
                    store.sales_amt_list_in_season AS sales_amt_list_in_season,
                    di_local_pre_year.cust_order_qty AS retail_cnt_have_customer,
                    di_local_pre_year.cust_count AS customer_cnt,
                    similar.new_sku_discount_rate_similar AS new_sku_discount_rate_similar,
                    similar.gw_retail_amt_similar AS gw_retail_amt_similar,
                    similar.top_sales_upt_similar AS top_sales_upt_similar,
                    store.inv_qty AS inv_qty,
                    store.vip_repurchase_cnt AS vip_repurchase_cnt,
                    similar.large_screen_sale_amt_pct_similar AS large_screen_sale_amt_pct_similar,
                    scorecard.skuEfficency_sc_label AS skuEfficency_sc_label,
                    task.task_tgt_qty AS task_tgt_qty,
                    similar.yun_store_sale_amt_similar AS yun_store_sale_amt_similar,
                    di_local_pre_year.sales_qty_for_upt AS sales_qty_for_upt,
                    similar.plural_retail_cnt_pct_similar AS plural_retail_cnt_pct_similar,
                    similar.deal_rate_similar AS deal_rate_similar,
                    similar.offline_amt_pct_similar AS offline_amt_pct_similar,
                    store.plural_retail_cnt AS plural_retail_cnt,
                    member_similar.new_consume_member_qty_similar AS new_consume_member_qty_similar,
                    di_local_pre_year.yun_store_sale_amt AS yun_store_sale_amt,
                    di_local_pre_year.gw_retail_amt AS gw_retail_amt,
                    store.sales_qty AS sales_qty,
                    similar.singular_discount_rate_similar AS singular_discount_rate_similar,
                    di_local_pre_year.sales_amt_list AS sales_amt_list,
                    similar.discount_rate_similar AS discount_rate_similar,
                    scorecard.taskExecuteRate_sc_label AS taskExecuteRate_sc_label,
                    similar.total_amount_similar AS total_amount_similar,
                    sales_pct_di_local_pre_year.top_sales_amt_actual_pct AS top_sales_amt_actual_pct,
                    similar.top_sales_amt_actual_pct_similar AS top_sales_amt_actual_pct_similar,
                    di_local_pre_year.top20_sales_match_rate AS top20_sales_match_rate,
                    similar.top20_match_rate_similar AS top20_sales_match_rate_similar,
                    member.new_reg_member_qty AS new_reg_member_qty,
                    member_similar.new_reg_member_qty_similar AS new_reg_member_qty_similar,
                    member.new_reg_has_consume_member_qty AS new_reg_has_consume_member_qty,
                    member_similar.new_reg_has_consume_member_qty_similar AS new_reg_has_consume_member_qty_similar,
                    store.sku_unbroken_rate AS sku_unbroken_rate,
                    similar.sku_unbroken_rate_similar AS sku_unbroken_rate_similar,
                    member_similar.new_reg_has_consume_member_rate_similar AS new_reg_has_consume_ratio_similar,
                    member.high_val_member_consume_member_qty AS high_val_member_consume_member_qty,
                    member_similar.high_val_member_consume_member_qty_similar AS high_val_member_consume_member_qty_similar,
                    birth_member_coupon.issue_qty AS birth_issue_qty,
                    birth_coupon_similar.issue_qty_similar AS birth_issue_qty_similar,
                    birth_member_coupon.write_off_qty AS birth_write_off_qty,
                    birth_coupon_similar.write_off_qty_similar AS birth_write_off_qty_similar,
                    birth_coupon_similar.write_off_rate_similar AS birth_write_off_rate_similar,
                    member.new_reg_non_consume_member_qty AS new_reg_non_consume_member_qty,
                    member_similar.new_reg_non_consume_member_qty_similar AS new_reg_non_consume_member_qty_similar,
                    member_similar.new_reg_non_consume_member_rate_similar as new_reg_non_consume_ratio_similar,
                    member.high_val_member_consume_amt AS high_val_member_consume_amt,
                    member_similar.high_val_member_consume_amt_similar AS high_val_member_consume_amt_similar,
                    member.high_val_member_order_qty AS high_val_member_order_qty,
                    member_similar.high_val_member_order_qty_similar AS high_val_member_order_qty_similar,
                    member_similar.high_val_member_atv_similar AS high_val_member_atv_similar,
                    member.curr_month_new_member_order_qty AS curr_month_new_member_order_qty,
                    member.curr_month_old_member_order_qty AS curr_month_old_member_order_qty,
                    member.curr_month_total_barg_order_qty AS curr_month_total_barg_order_qty,
                    member_similar.barg_new_cust_reg_pct_similar AS barg_new_cust_reg_pct_similar,
                    member.old_member_consume_amt AS old_member_consume_amt,
                    member_similar.old_member_consume_amt_similar AS old_member_consume_amt_similar,
                    member.old_member_order_qty AS old_member_order_qty,
                    member_similar.old_member_order_qty_similar AS old_member_order_qty_similar,
                    member_similar.old_member_atv_similar AS old_member_atv_similar,
                    member.new_member_consume_amt AS new_member_consume_amt,
                    member_similar.new_member_consume_amt_similar AS new_member_consume_amt_similar,
                    member.new_member_order_qty AS new_member_order_qty,
                    member_similar.new_member_order_qty_similar AS new_member_order_qty_similar,
                    member_similar.new_member_atv_similar AS new_member_atv_similar,
                    member.member_consume_amt AS member_consume_amt,
                    member_similar.member_consume_amt_similar AS member_consume_amt_similar,
                    member_similar.high_val_member_consume_amt_pct_similar AS high_val_member_consume_amt_pct_similar,
                    member_similar.old_member_consume_amt_pct_similar AS old_member_consume_amt_pct_similar,
                    member_similar.new_member_consume_amt_pct_similar AS new_member_consume_amt_pct_similar,
                    member_similar.new_consume_member_qty_pct_similar AS new_consume_member_qty_pct_similar,
                    member_similar.old_consume_member_qty_pct_similar AS old_consume_member_qty_pct_similar,
                    member_similar.high_val_member_consume_member_qty_pct_similar AS high_val_member_consume_member_qty_pct_similar,
                    feature_target.numOfPurNewMember_target AS numOfPurNewMember_target,
                    feature_target.numOfPurMember_target AS numOfPurMember_target,
                    feature_target.numOfPurExistMember_target AS numOfPurExistMember_target,
                    feature_target.new_reg_member_qty_target AS new_reg_member_qty_target,
                    feature_target.new_reg_has_consume_member_rate_target AS new_reg_has_consume_member_rate_target,
                    feature_target.high_val_member_consume_member_qty_target AS high_val_member_consume_member_qty_target,
                    member.new_reg_has_consume_rate AS new_reg_has_consume_ratio
                from (select
                            *
                            from
                            retail_consultant_db.t_ads_store_monthly_stats
                            where tenant_id = 'anta'
                            and stats_date = #{preYearStatisTm}
                            and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                ) store join ( select
                                    *
                                    from retail_consultant_db.t_ads_store_scd
                                    where tenant_id = 'anta'
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                ) scd on store.tenant_id=scd.tenant_id
                and store.store_id=scd.store_id
                left outer join (select tenant_id, rel_id store_id, stats_date,brand_name,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'sales' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 5)   end)  sales_target,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurNewMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurNewMember_target,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurMember_target,
                                        max(case when visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'featureCode') = 'numOfPurExistMember' then toDecimal128(ifNull(visitParamExtractString(arrayJoin(  JSONExtractArrayRaw( ifNull(fetch_target_json, '[]') )   ), 'targetValue'), '0'), 0)   end)  numOfPurExistMember_target,
                                        null as new_reg_member_qty_target,
                                        null as new_reg_has_consume_member_rate_target,
                                        null as high_val_member_consume_member_qty_target
                                 from
                                            retail_consultant_db.t_sc_feature_month_target_info
                                        where tenant_id = 'anta'
                                            and stats_date = #{preYearStatisTm}
                                            and rel_type='STORE' and del_flag=0
                                            and rel_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by tenant_id,rel_id,stats_date,brand_name
                ) feature_target on store.tenant_id=feature_target.tenant_id
                and store.store_id=feature_target.store_id
                and toDate(store.stats_date)=feature_target.stats_date
                and store.brand_name=feature_target.brand_name
                left outer join (select
                                        tenant_id,
                                        region_id,
                                        calc_begin_date,
                                        brand_name,
                                        max(case when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurExistMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurExistMember_sc_label,
                                        max(case when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'matchDegTopProduct' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  matchDegTopProduct_sc_label,
                                        max(case when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurNewMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurNewMember_sc_label,
                                        max(case when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topFullSizeRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  topFullSizeRate_sc_label,
                                        max(case when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'couponUsageRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  couponUsageRate_sc_label,
                                        max(case when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'taskExecuteRate' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  taskExecuteRate_sc_label,
                                        max(case when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesTnumStaffs' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesTnumStaffs_sc_label,
                                        max(case when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'salesOfInSeason' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  salesOfInSeason_sc_label,
                                        max(case when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'numOfPurMember' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  numOfPurMember_sc_label,
                                        max(case when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'skuEfficency' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  skuEfficency_sc_label,
                                        max(case when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'topSalesPct' and  feature_scorecard.score_code = 'ANOMALY' then '异常' end)  topSalesPct_sc_label,
                                        max(case when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'sales' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  sales_sc_label,
                                        max(case when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'atv' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  atv_sc_label,
                                        max(case when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'upt' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  upt_sc_label,
                                        max(case when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'cr' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  cr_sc_label,
                                        max(case when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'HEALTHY' then '健康'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'EARLY_WARNING' then '预警'  when feature_scorecard.feature_code = 'md' and  feature_scorecard.score_code = 'ANOMALY' then '异常'  end)  md_sc_label
                                        from retail_consultant_db.t_ads_feature_score_card feature_scorecard
                                        where tenant_id = 'anta'
                                        and region_type='STORE'
                                        and cycle = 'MONTH_TOTAL'
                                        and calc_begin_date = #{preYearStatisTm}
                                        and region_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by tenant_id,region_id,calc_begin_date,brand_name
                ) scorecard on store.tenant_id=scorecard.tenant_id
                and store.store_id=scorecard.region_id
                and toDate(store.stats_date)=scorecard.calc_begin_date
                and store.brand_name=scorecard.brand_name
                left outer join (select
                                        member.tenant_id tenant_id,
                                        member.stats_date stats_date,
                                        member.store_id store_id,
                                        member.brand_name brand_name,
                                        sum(member.consume_member_qty) consume_member_qty,
                                        sum(member.old_consume_member_qty) old_consume_member_qty,
                                        sum(member.new_consume_member_qty) new_consume_member_qty,
                                        sum(member.new_reg_member_qty) as new_reg_member_qty,
                                        sum(member.new_reg_has_consume_member_qty) new_reg_has_consume_member_qty,
                                        sum(member.high_val_member_consume_member_qty) high_val_member_consume_member_qty,
                                        sum(member.new_reg_non_consume_member_qty) new_reg_non_consume_member_qty,
                                        sum(member.high_val_member_consume_amt) high_val_member_consume_amt,
                                        sum(member.high_val_member_order_qty) high_val_member_order_qty,
                                        sum(member.curr_month_new_member_order_qty) curr_month_new_member_order_qty,
                                        sum(member.curr_month_old_member_order_qty) curr_month_old_member_order_qty,
                                        sum(member.curr_month_total_barg_order_qty) curr_month_total_barg_order_qty,
                                        sum(member.old_member_consume_amt) old_member_consume_amt,
                                        sum(member.old_member_order_qty) old_member_order_qty,
                                        sum(member.new_member_consume_amt) new_member_consume_amt,
                                        sum(member.new_member_order_qty) new_member_order_qty,
                                        sum(member.member_consume_amt) member_consume_amt,
                                        avg(member.new_reg_has_consume_rate) new_reg_has_consume_rate
                                from retail_consultant_db.t_ads_crm_mco_store_statis member
                                        where tenant_id = 'anta'
                                        and stats_date = #{preYearStatisTm}
                                        and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by tenant_id,stats_date,store_id,brand_name
                ) member on store.tenant_id=member.tenant_id
                and store.store_id=member.store_id
                and store.stats_date=member.stats_date
                and store.brand_name=member.brand_name
                left outer join (select
                                    member_coupon.brand_name brand_name,
                                    member_coupon.store_id store_id,
                                    member_coupon.tenant_id tenant_id,
                                    member_coupon.stats_date stats_date,
                                    sum(member_coupon.issue_qty) issue_qty,
                                    sum(member_coupon.write_off_qty) write_off_qty
                                    from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                    where tenant_id = 'anta'
                                    and stats_date = #{preYearStatisTm}
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                    group by brand_name,store_id,tenant_id,stats_date
                ) member_coupon on store.tenant_id=member_coupon.tenant_id
                and store.store_id=member_coupon.store_id
                and store.stats_date=member_coupon.stats_date
                and store.brand_name=member_coupon.brand_name
                left outer join (select
                                    member_coupon.brand_name brand_name,
                                    member_coupon.store_id store_id,
                                    member_coupon.tenant_id tenant_id,
                                    member_coupon.stats_date stats_date,
                                    sum(member_coupon.issue_qty) issue_qty,
                                    sum(member_coupon.write_off_qty) write_off_qty
                                    from retail_consultant_db.t_ads_crm_mco_coupon_statis member_coupon
                                    where tenant_id = 'anta'
                                    and stats_date = #{preYearStatisTm} and coupon_categ_desc = '生日券'
                                    and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                    group by brand_name,store_id,tenant_id,stats_date
                ) birth_member_coupon on store.tenant_id=birth_member_coupon.tenant_id
                and store.store_id=birth_member_coupon.store_id
                and store.stats_date=birth_member_coupon.stats_date
                and store.brand_name=birth_member_coupon.brand_name
                left outer join (select
                                        task.stats_date stats_date,
                                        task.tenant_id tenant_id,
                                        task.store_id store_id,
                                        task.brand_name brand_name,
                                        sum(task.exec_qty) exec_qty,
                                        sum(
                                        case
                                        when task.task_type_desc = 'single_talk' then task.task_tgt_qty
                                        else 0
                                        end
                                        ) task_tgt_qty_single_talk,
                                        toInt32(
                                        countDistinctIf(guide_id, task.task_type_desc = 'friends_area')
                                        ) guide_count,
                                        date_diff('day', toDate(#{preYearStatisTm}), toDate(#{endPreYearStatisTm})) + 1 days,
                                        guide_count * days * 3 task_tgt_qty_friends_area,
                                        task_tgt_qty_single_talk + task_tgt_qty_friends_area task_tgt_qty
                                    from retail_consultant_db.t_ads_crm_mco_guide_task_statis task
                                    where true
                                        and tenant_id = 'anta'
                                        and stats_date = #{preYearStatisTm}
                                        and store_id in <foreach collection="storeIds" item="item" open="(" separator="," close=")">#{item}</foreach>
                                        group by stats_date,tenant_id,store_id,brand_name
                ) task on store.tenant_id=task.tenant_id
                and store.store_id=task.store_id
                and store.stats_date=task.stats_date
                and store.brand_name=task.brand_name
                left outer join (select *
                                    from
                                    retail_consultant_db.t_ads_similar_store_monthly_stats
                                    where tenant_id = 'anta'
                                    and stats_date = #{preYearStatisTm}
                ) similar
                on  store.tenant_id = similar.tenant_id
                and  store.store_type = similar.store_type
                and  store.stats_date = similar.stats_date
                and  store.brand_name = similar.brand_name
                left outer join (select *
                                    from
                                    retail_consultant_db.dm_crm_mco_store_statis_similar
                                    where statis_tm = #{preYearStatisTm} and store_type_class != ''
                ) member_similar
                on   store.store_type = member_similar.store_type_class
                and  store.stats_date = member_similar.statis_tm
                and  store.brand_name = member_similar.brand_name
                left outer join (select
                                    *
                                    from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                    where statis_tm = #{preYearStatisTm} and store_type_class != ''
                                    and coupon_categ_desc = 'ALL'
                ) coupon_similar on store.store_type = coupon_similar.store_type_class
                and  store.stats_date = coupon_similar.statis_tm
                and  store.brand_name = coupon_similar.brand_name
                left outer join (select
                                        statis_tm,
                                        brand_name,
                                        sum(issue_qty_similar) as issue_qty_similar,
                                        sum(write_off_qty_similar) as write_off_qty_similar,
                                        case when sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) != 0 then sum(dm_crm_mco_coupon_statis_similar.write_off_qty_similar)/sum(dm_crm_mco_coupon_statis_similar.issue_qty_similar) end as write_off_rate_similar
        from retail_consultant_db.dm_crm_mco_coupon_statis_similar
                                    where statis_tm = #{preYearStatisTm} and store_type_class != ''
                                        and coupon_categ_desc = '生日券'
                                    group by statis_tm, brand_name
                ) birth_coupon_similar on store.store_type = coupon_similar.store_type_class
                and  store.stats_date = birth_coupon_similar.statis_tm
                and  store.brand_name = birth_coupon_similar.brand_name
                left outer join (select
                                    *
                                    from retail_consultant_db.dm_crm_mco_guide_task_statis_similar
                                    where true
                                    and statis_tm = #{preYearStatisTm} and store_type_class != ''
                                    and task_type_desc = 'ALL' AND task_status = 'ALL'
                ) task_similar
                on   store.store_type = task_similar.store_type_class
                and  store.stats_date = task_similar.statis_tm
                and  store.brand_name = task_similar.brand_name
                left outer join (select *
                                    from di_local_pre_year_tmp
                ) di_local_pre_year on store.store_id = di_local_pre_year.store_id
                and store.brand_name=di_local_pre_year.brand_name
                left outer join (select *
                                    from sku_di_local_pre_year_tmp
                ) sku_di_local_pre_year on store.store_id = sku_di_local_pre_year.store_id
                                    and store.brand_name=sku_di_local_pre_year.brand_name
                left outer join (select *
                                    from sales_pct_di_local_pre_year_tmp
                ) sales_pct_di_local_pre_year on store.store_id = sales_pct_di_local_pre_year.store_id
                                    and store.brand_name=sales_pct_di_local_pre_year.brand_name
                left outer join (select *
                                    from is_homo_di_local_pre_year_tmp
                ) is_homo_di_local_pre_year on store.store_id = is_homo_di_local_pre_year.store_id
                                    and store.brand_name=is_homo_di_local_pre_year.brand_name
        ) pre_year on cur.tenant_id=pre_year.tenant_id
        and cur.store_id=pre_year.store_id
        and cur.brand_name=pre_year.brand_name
    </insert>

</mapper>
