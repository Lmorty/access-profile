import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.junit.Test;
import description.HiveTableDes;
import util.HiveDDLParserUtil;

public class HiveDDLParserUtilTest {


    @Test
    public void testAnalyse(){


        HiveDDLParserUtil instance = HiveDDLParserUtil.createInstance();

        HiveTableDes parse = instance.parse(ddlSQL);

        System.out.println(parse.dbName);
        System.out.println(parse.tableName);
        System.out.println(parse.tableComment);

        parse.colsDescription.forEach(x -> System.out.println(x));
        parse.partColsDescription.forEach(x -> System.out.println(x));
    }


    String ddlSQL = "CREATE TABLE `access_cdm.dim_seller_all_dh_f`(\n" +
            "  `id_code` bigint COMMENT '经销商ID', \n" +
            "  `user_id` bigint COMMENT '用户ID', \n" +
            "  `user_name` string COMMENT '用户姓名', \n" +
            "  `user_nickname` string COMMENT '用户昵称', \n" +
            "  `user_mobile` string COMMENT '用户手机号码(密文)', \n" +
            "  `user_gender` int COMMENT '用户性别:0.未知,1.男,2.女', \n" +
            "  `user_gender_desc` string COMMENT '用户性别描述', \n" +
            "  `user_age` int COMMENT '用户年龄', \n" +
            "  `idcard_user_gender` int COMMENT '用户性别:0.未知,1.男,2.女(身份证号码解析)', \n" +
            "  `idcard_user_gender_desc` string COMMENT '用户性别描述(身份证号码解析)', \n" +
            "  `idcard_user_age` int COMMENT '用户年龄(身份证号码解析)', \n" +
            "  `user_head_image` string COMMENT '用户头像', \n" +
            "  `user_special_code` string COMMENT '用户靓号', \n" +
            "  `user_country_area_id` bigint COMMENT '用户国家区域ID(用户填写)', \n" +
            "  `user_country_area` string COMMENT '用户国家区域(用户填写)', \n" +
            "  `user_country_id` bigint COMMENT '用户国家ID(用户填写)', \n" +
            "  `user_country` string COMMENT '用户国家(用户填写)', \n" +
            "  `user_province_id` bigint COMMENT '用户省份ID(用户填写)', \n" +
            "  `user_province` string COMMENT '用户省份(用户填写)', \n" +
            "  `user_city_id` bigint COMMENT '用户城市ID(用户填写)', \n" +
            "  `user_city` string COMMENT '用户城市(用户填写)', \n" +
            "  `user_district_id` bigint COMMENT '用户地区ID(用户填写)', \n" +
            "  `user_district` string COMMENT '用户地区(用户填写)', \n" +
            "  `invite_code` string COMMENT '邀请码', \n" +
            "  `is_changed_invite_code` int COMMENT '是否修改过邀请码: 1-是, 0-否', \n" +
            "  `user_created_at` string COMMENT '用户创建时间', \n" +
            "  `user_level` int COMMENT '用户等级', \n" +
            "  `user_level_desc` string COMMENT '等级描述:1：注册用户、2：粉卡、3：白金、4：黑钻、5：黑钻plus', \n" +
            "  `inviter_id_code` bigint COMMENT '原始推荐人', \n" +
            "  `inviter_user_level` int COMMENT '原始推荐人用户等级', \n" +
            "  `inviter_user_level_desc` string COMMENT '原始推荐人用户等级描述', \n" +
            "  `inviter_user_name` string COMMENT '原始推荐人用户姓名', \n" +
            "  `inviter_user_nickname` string COMMENT '原始推荐人用户昵称', \n" +
            "  `abm_role` int COMMENT 'ABM角色', \n" +
            "  `abm_role_desc` string COMMENT 'ABM角色描述：0.无角色 1.推广者 51.经销商 53.经销商+代理商', \n" +
            "  `is_co_account` int COMMENT '是否是公司账号:1/0', \n" +
            "  `is_sharer` int COMMENT '是否是推广者:1/0', \n" +
            "  `sharer_level_desc` string COMMENT '当前推广者等级', \n" +
            "  `is_sign_sharer_contract` int COMMENT '是否签订推广者协议:1/0', \n" +
            "  `sign_sharer_contract_at` string COMMENT '签订推广者协议时间', \n" +
            "  `is_sign_seller_contract` int COMMENT '是否签订经销商协议:1/0', \n" +
            "  `sign_seller_contract_at` string COMMENT '签订经销商协议时间', \n" +
            "  `star_role` int COMMENT '星级身份', \n" +
            "  `star_role_desc` string COMMENT '星级身份 0：无星级身份，100：服务商，200：运营商', \n" +
            "  `star_level` int COMMENT '星级等级', \n" +
            "  `star_level_desc` string COMMENT '星级等级 0：无星级等级，10：城市服务商1星，20：城市服务商2星，30：城市服务商3星，40：城市服务商4星，50：EC，60：SEC，70: EEC', \n" +
            "  `star_created_at` string COMMENT '签署星级协议时间', \n" +
            "  `pre_star_level` int COMMENT '预备星级等级', \n" +
            "  `pre_star_level_desc` string COMMENT '预备星级等级描述', \n" +
            "  `p_id_code` bigint COMMENT '上级id_code', \n" +
            "  `p_user_level` int COMMENT '上级用户等级', \n" +
            "  `p_user_level_desc` string COMMENT '上级用户等级描述', \n" +
            "  `p_user_name` string COMMENT '上级用户姓名', \n" +
            "  `p_user_nickname` string COMMENT '上级用户昵称', \n" +
            "  `bl_cust_id_code` bigint COMMENT '所属客户id_code', \n" +
            "  `bl_cust_user_name` string COMMENT '所属客户用户姓名', \n" +
            "  `bl_cust_user_nickname` string COMMENT '所属客户用户昵称', \n" +
            "  `bl_assc_id_code` bigint COMMENT '所属社群id_code', \n" +
            "  `bl_assc_user_name` string COMMENT '所属社群用户姓名', \n" +
            "  `bl_assc_user_nickname` string COMMENT '所属社群用户昵称', \n" +
            "  `bl_star1_id_code` bigint COMMENT '所属1星城市服务商id_code', \n" +
            "  `bl_star1_user_name` string COMMENT '所属1星城市服务商用户姓名', \n" +
            "  `bl_star1_user_nickname` string COMMENT '所属1星城市服务商用户昵称', \n" +
            "  `bl_star2_id_code` bigint COMMENT '所属2星城市服务商id_code', \n" +
            "  `bl_star2_user_name` string COMMENT '所属2星城市服务商用户姓名', \n" +
            "  `bl_star2_user_nickname` string COMMENT '所属2星城市服务商用户昵称', \n" +
            "  `bl_star3_id_code` bigint COMMENT '所属3星城市服务商id_code', \n" +
            "  `bl_star3_user_name` string COMMENT '所属3星城市服务商用户姓名', \n" +
            "  `bl_star3_user_nickname` string COMMENT '所属3星城市服务商用户昵称', \n" +
            "  `bl_star4_id_code` bigint COMMENT '所属4星城市服务商id_code', \n" +
            "  `bl_star4_user_name` string COMMENT '所属4星城市服务商用户姓名', \n" +
            "  `bl_star4_user_nickname` string COMMENT '所属4星城市服务商用户昵称', \n" +
            "  `bl_ec_id_code` bigint COMMENT '所属ECid_code', \n" +
            "  `bl_ec_user_name` string COMMENT '所属EC用户姓名', \n" +
            "  `bl_ec_user_nickname` string COMMENT '所属EC用户昵称', \n" +
            "  `bl_sec_id_code` bigint COMMENT '所属SECid_code', \n" +
            "  `bl_sec_user_name` string COMMENT '所属SEC用户姓名', \n" +
            "  `bl_sec_user_nickname` string COMMENT '所属SEC用户昵称', \n" +
            "  `bl_club_id` bigint COMMENT '所属俱乐部id', \n" +
            "  `bl_club_name` string COMMENT '所属俱乐部名称', \n" +
            "  `bl_club_head_image` string COMMENT '所属俱乐部头像', \n" +
            "  `bl_club_founder_id_code` bigint COMMENT '所属俱乐部创始人ID_CODE', \n" +
            "  `bl_zone_id` bigint COMMENT '所属战区id', \n" +
            "  `bl_zone_name` string COMMENT '所属战区名称', \n" +
            "  `bl_zone_head_image` string COMMENT '所属战区头像', \n" +
            "  `bl_ka_user_nickname` string COMMENT '所属ka经理名称', \n" +
            "  `bl_ka_leader_user_nickname` string COMMENT '所属总监名称', \n" +
            "  `bl_director_user_nickname` string COMMENT '所属总监组长名称', \n" +
            "  `bl_steward_user_nickname` string COMMENT '所属管家名称', \n" +
            "  `bl_steward_mobile` string COMMENT '所属管家手机号', \n" +
            "  `bl_steward_vx` string COMMENT '所属管家微信号', \n" +
            "  `label` string COMMENT '荣誉体系标签(无标签， 普通 ，核心 ，预备EC ，EC ，SEC)（老）', \n" +
            "  `label_status` string COMMENT '上月状态(无变化 ，晋级 ，降级 保级1，保级2 ，保级成功)（老）', \n" +
            "  `sec_id_code` bigint COMMENT '所属sec（老）', \n" +
            "  `sec_name` string COMMENT '所属sec线名（老）', \n" +
            "  `sec_team_name` string COMMENT '所属sec战队名（老）', \n" +
            "  `ec_id_code` bigint COMMENT '所属ec（老）', \n" +
            "  `ec_name` string COMMENT '所属ec线名（老）', \n" +
            "  `pro_ec_id_code` bigint COMMENT '所属预备ec（老）', \n" +
            "  `pro_ec_name` string COMMENT '所属预备ec线名（老）', \n" +
            "  `core_id_code` bigint COMMENT '所属核心（老）', \n" +
            "  `core_name` string COMMENT '所属核心线名（老）', \n" +
            "  `etl_create_at` string COMMENT 'ETL创建时间', \n" +
            "  `etl_update_at` string COMMENT 'ETL更新时间', \n" +
            "  `abm_user_country_area_id` bigint COMMENT '用户国家区域ID(ABM)', \n" +
            "  `abm_user_country_area` string COMMENT '用户国家区域(ABM)', \n" +
            "  `abm_user_country_id` bigint COMMENT '用户国家ID(ABM)', \n" +
            "  `abm_user_country` string COMMENT '用户国家(ABM)', \n" +
            "  `abm_user_province_id` bigint COMMENT '用户省份ID(ABM)', \n" +
            "  `abm_user_province` string COMMENT '用户省份(ABM)', \n" +
            "  `abm_user_city_id` bigint COMMENT '用户城市ID(ABM)', \n" +
            "  `abm_user_city` string COMMENT '用户城市(ABM)', \n" +
            "  `abm_user_district_id` bigint COMMENT '用户地区ID(ABM)', \n" +
            "  `abm_user_district` string COMMENT '用户地区(ABM)', \n" +
            "  `user_auth_identity_no` string COMMENT 'abm实名认证身份证号码（加密）', \n" +
            "  `user_auth_real_name` string COMMENT 'abm实名认证姓名（加密）', \n" +
            "  `abm_level` int COMMENT 'ABM角色等级：1-ABM注册用户，2-初级推广者，3-中级推广者，4-高级推广者，5-经销商', \n" +
            "  `abm_level_desc` string COMMENT 'ABM角色等级描述', \n" +
            "  `bl_grow_or_sup_id_code` bigint COMMENT '用户直属上级id', \n" +
            "  `bl_grow_or_sup_user_level` int COMMENT '用户直属上级基础等级', \n" +
            "  `bl_grow_or_sup_abm_level` int COMMENT '用户直属上级ABM等级', \n" +
            "  `bl_grow_or_sup_user_name` string COMMENT '用户直属上级姓名', \n" +
            "  `bl_grow_or_sup_user_nickname` string COMMENT '用户直属上级昵称', \n" +
            "  `bl_cust_user_level` int COMMENT '用户客户经理基础等级', \n" +
            "  `bl_cust_abm_level` int COMMENT '用户客户经理ABM等级', \n" +
            "  `user_vtn_nickname` string COMMENT 'vtn昵称', \n" +
            "  `user_vtn_head_image` string COMMENT 'vtn头像', \n" +
            "  `user_abm_nickname` string COMMENT 'abm昵称', \n" +
            "  `user_abm_head_image` string COMMENT 'abm头像', \n" +
            "  `bl_star1_id_code_or` bigint, \n" +
            "  `bl_star2_id_code_or` bigint, \n" +
            "  `bl_star3_id_code_or` bigint, \n" +
            "  `bl_star4_id_code_or` bigint, \n" +
            "  `bl_ec_id_code_or` bigint, \n" +
            "  `bl_sec_id_code_or` bigint)\n" +
            "PARTITIONED BY ( \n" +
            "  `dt` string COMMENT '统计日期-小时')\n" +
            "ROW FORMAT SERDE \n" +
            "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \n" +
            "STORED AS INPUTFORMAT \n" +
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' \n" +
            "OUTPUTFORMAT \n" +
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\n" +
            "LOCATION\n" +
            "  'hdfs://hacluster/user/hive/warehouse/access_cdm.db/dim_seller_all_dh_f'\n" +
            "TBLPROPERTIES (\n" +
            "  'bucketing_version'='2', \n" +
            "  'last_modified_by'='datadev', \n" +
            "  'last_modified_time'='1635140927', \n" +
            "  'owner'='dingwei', \n" +
            "  'parquet.compression'='uncompressed', \n" +
            "  'spark.sql.create.version'='2.2 or prior', \n" +
            "  'spark.sql.sources.schema.numPartCols'='1', \n" +
            "  'spark.sql.sources.schema.numParts'='14', \n" +
            "  'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"经销商ID\"}},{\"name\":\"user_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户ID\"}},{\"name\":\"user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户姓名\"}},{\"name\":\"user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户昵称\"}},{\"name\":\"user_mobile\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户手机号码(密文)\"}},{\"name\":\"user_gender\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"用户性别:0.未知,1.男,2.女\"}},{\"name\":\"user_gender_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户性别描述\"}},{\"name\":\"user_age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"用户年龄\"}},{\"name\":\"idcard_user_gender\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"用户性别:0.未知,1.男,2.女(身份证号码解析)\"}},{\"name\":\"idcard_user_gender_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户性别描述(身份证号码解析)\"}},{\"name\":\"idcard_user_age\",\"type\":\"integer\",\"nullable\":tr', \n" +
            "  'spark.sql.sources.schema.part.1'='ue,\"metadata\":{\"comment\":\"用户年龄(身份证号码解析)\"}},{\"name\":\"user_head_image\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户头像\"}},{\"name\":\"user_special_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户靓号\"}},{\"name\":\"user_country_area_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户国家区域ID(用户填写)\"}},{\"name\":\"user_country_area\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户国家区域(用户填写)\"}},{\"name\":\"user_country_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户国家ID(用户填写)\"}},{\"name\":\"user_country\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户国家(用户填写)\"}},{\"name\":\"user_province_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户省份ID(用户填写)\"}},{\"name\":\"user_province\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户省份(用户填写)\"}},{\"name\":\"user_city_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户城市ID(用户填写)\"}},{\"name\":\"user_city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户城市(用户填写)\"}},{\"name\":\"user_distr', \n" +
            "  'spark.sql.sources.schema.part.10'='{\"comment\":\"ETL更新时间\"}},{\"name\":\"abm_user_country_area_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户国家区域ID(ABM)\"}},{\"name\":\"abm_user_country_area\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户国家区域(ABM)\"}},{\"name\":\"abm_user_country_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户国家ID(ABM)\"}},{\"name\":\"abm_user_country\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户国家(ABM)\"}},{\"name\":\"abm_user_province_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户省份ID(ABM)\"}},{\"name\":\"abm_user_province\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户省份(ABM)\"}},{\"name\":\"abm_user_city_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户城市ID(ABM)\"}},{\"name\":\"abm_user_city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户城市(ABM)\"}},{\"name\":\"abm_user_district_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户地区ID(ABM)\"}},{\"name\":\"abm_user_district\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户地区(ABM)\"}},', \n" +
            "  'spark.sql.sources.schema.part.11'='{\"name\":\"user_auth_identity_no\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"abm实名认证身份证号码（加密）\"}},{\"name\":\"user_auth_real_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"abm实名认证姓名（加密）\"}},{\"name\":\"abm_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"ABM角色等级：1-ABM注册用户，2-初级推广者，3-中级推广者，4-高级推广者，5-经销商\"}},{\"name\":\"abm_level_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"ABM角色等级描述\"}},{\"name\":\"bl_grow_or_sup_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户直属上级id\"}},{\"name\":\"bl_grow_or_sup_user_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"用户直属上级基础等级\"}},{\"name\":\"bl_grow_or_sup_abm_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"用户直属上级ABM等级\"}},{\"name\":\"bl_grow_or_sup_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户直属上级姓名\"}},{\"name\":\"bl_grow_or_sup_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户直属上级昵称\"}},{\"name\":\"bl_cust_user_level\",\"type\":\"integer\",\"nullab', \n" +
            "  'spark.sql.sources.schema.part.12'='le\":true,\"metadata\":{\"comment\":\"用户客户经理基础等级\"}},{\"name\":\"bl_cust_abm_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"用户客户经理ABM等级\"}},{\"name\":\"user_vtn_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"vtn昵称\"}},{\"name\":\"user_vtn_head_image\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"vtn头像\"}},{\"name\":\"user_abm_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"abm昵称\"}},{\"name\":\"user_abm_head_image\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"abm头像\"}},{\"name\":\"bl_star1_id_code_or\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"bl_star2_id_code_or\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"bl_star3_id_code_or\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"bl_star4_id_code_or\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"bl_ec_id_code_or\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"bl_sec_id_code_or\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dt\",\"type\":\"string\",\"nullable', \n" +
            "  'spark.sql.sources.schema.part.13'='\":true,\"metadata\":{\"comment\":\"统计日期-小时\"}}]}', \n" +
            "  'spark.sql.sources.schema.part.2'='ict_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"用户地区ID(用户填写)\"}},{\"name\":\"user_district\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户地区(用户填写)\"}},{\"name\":\"invite_code\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"邀请码\"}},{\"name\":\"is_changed_invite_code\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"是否修改过邀请码: 1-是, 0-否\"}},{\"name\":\"user_created_at\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"用户创建时间\"}},{\"name\":\"user_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"用户等级\"}},{\"name\":\"user_level_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"等级描述:1：注册用户、2：粉卡、3：白金、4：黑钻、5：黑钻plus\"}},{\"name\":\"inviter_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"原始推荐人\"}},{\"name\":\"inviter_user_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"原始推荐人用户等级\"}},{\"name\":\"inviter_user_level_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"原始推荐人用户等级描述\"}},{\"name\":\"inviter_user_name\",\"type\":\"string\",\"null', \n" +
            "  'spark.sql.sources.schema.part.3'='able\":true,\"metadata\":{\"comment\":\"原始推荐人用户姓名\"}},{\"name\":\"inviter_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"原始推荐人用户昵称\"}},{\"name\":\"abm_role\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"ABM角色\"}},{\"name\":\"abm_role_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"ABM角色描述：0.无角色 1.推广者 51.经销商 53.经销商+代理商\"}},{\"name\":\"is_co_account\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"是否是公司账号:1/0\"}},{\"name\":\"is_sharer\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"是否是推广者:1/0\"}},{\"name\":\"sharer_level_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"当前推广者等级\"}},{\"name\":\"is_sign_sharer_contract\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"是否签订推广者协议:1/0\"}},{\"name\":\"sign_sharer_contract_at\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"签订推广者协议时间\"}},{\"name\":\"is_sign_seller_contract\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"是否签订经销商协议:1/0\"}},{\"name\":\"sign_seller_contract_at\",\"type\":\"string\",\"nul', \n" +
            "  'spark.sql.sources.schema.part.4'='lable\":true,\"metadata\":{\"comment\":\"签订经销商协议时间\"}},{\"name\":\"star_role\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"星级身份\"}},{\"name\":\"star_role_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"星级身份 0：无星级身份，100：服务商，200：运营商\"}},{\"name\":\"star_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"星级等级\"}},{\"name\":\"star_level_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"星级等级 0：无星级等级，10：城市服务商1星，20：城市服务商2星，30：城市服务商3星，40：城市服务商4星，50：EC，60：SEC，70: EEC\"}},{\"name\":\"star_created_at\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"签署星级协议时间\"}},{\"name\":\"pre_star_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"预备星级等级\"}},{\"name\":\"pre_star_level_desc\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"预备星级等级描述\"}},{\"name\":\"p_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"上级id_code\"}},{\"name\":\"p_user_level\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"comment\":\"上级用户等级\"}},{\"name\":\"p_user_level_desc\",\"type\":\"string\",\"nullable', \n" +
            "  'spark.sql.sources.schema.part.5'='\":true,\"metadata\":{\"comment\":\"上级用户等级描述\"}},{\"name\":\"p_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"上级用户姓名\"}},{\"name\":\"p_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"上级用户昵称\"}},{\"name\":\"bl_cust_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属客户id_code\"}},{\"name\":\"bl_cust_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属客户用户姓名\"}},{\"name\":\"bl_cust_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属客户用户昵称\"}},{\"name\":\"bl_assc_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属社群id_code\"}},{\"name\":\"bl_assc_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属社群用户姓名\"}},{\"name\":\"bl_assc_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属社群用户昵称\"}},{\"name\":\"bl_star1_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属1星城市服务商id_code\"}},{\"name\":\"bl_star1_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属1星城市服务商用户姓名\"}},{\"n', \n" +
            "  'spark.sql.sources.schema.part.6'='ame\":\"bl_star1_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属1星城市服务商用户昵称\"}},{\"name\":\"bl_star2_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属2星城市服务商id_code\"}},{\"name\":\"bl_star2_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属2星城市服务商用户姓名\"}},{\"name\":\"bl_star2_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属2星城市服务商用户昵称\"}},{\"name\":\"bl_star3_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属3星城市服务商id_code\"}},{\"name\":\"bl_star3_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属3星城市服务商用户姓名\"}},{\"name\":\"bl_star3_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属3星城市服务商用户昵称\"}},{\"name\":\"bl_star4_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属4星城市服务商id_code\"}},{\"name\":\"bl_star4_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属4星城市服务商用户姓名\"}},{\"name\":\"bl_star4_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment', \n" +
            "  'spark.sql.sources.schema.part.7'='\":\"所属4星城市服务商用户昵称\"}},{\"name\":\"bl_ec_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属ECid_code\"}},{\"name\":\"bl_ec_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属EC用户姓名\"}},{\"name\":\"bl_ec_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属EC用户昵称\"}},{\"name\":\"bl_sec_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属SECid_code\"}},{\"name\":\"bl_sec_user_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属SEC用户姓名\"}},{\"name\":\"bl_sec_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属SEC用户昵称\"}},{\"name\":\"bl_club_id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属俱乐部id\"}},{\"name\":\"bl_club_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属俱乐部名称\"}},{\"name\":\"bl_club_head_image\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属俱乐部头像\"}},{\"name\":\"bl_club_founder_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属俱乐部创始人ID_CODE\"}},{\"name\":\"bl_zone_id\",\"type\":\"long\",', \n" +
            "  'spark.sql.sources.schema.part.8'='\"nullable\":true,\"metadata\":{\"comment\":\"所属战区id\"}},{\"name\":\"bl_zone_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属战区名称\"}},{\"name\":\"bl_zone_head_image\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属战区头像\"}},{\"name\":\"bl_ka_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属ka经理名称\"}},{\"name\":\"bl_ka_leader_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属总监名称\"}},{\"name\":\"bl_director_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属总监组长名称\"}},{\"name\":\"bl_steward_user_nickname\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属管家名称\"}},{\"name\":\"bl_steward_mobile\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属管家手机号\"}},{\"name\":\"bl_steward_vx\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属管家微信号\"}},{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"荣誉体系标签(无标签， 普通 ，核心 ，预备EC ，EC ，SEC)（老）\"}},{\"name\":\"label_status\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"com', \n" +
            "  'spark.sql.sources.schema.part.9'='ment\":\"上月状态(无变化 ，晋级 ，降级 保级1，保级2 ，保级成功)（老）\"}},{\"name\":\"sec_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属sec（老）\"}},{\"name\":\"sec_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属sec线名（老）\"}},{\"name\":\"sec_team_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属sec战队名（老）\"}},{\"name\":\"ec_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属ec（老）\"}},{\"name\":\"ec_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属ec线名（老）\"}},{\"name\":\"pro_ec_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属预备ec（老）\"}},{\"name\":\"pro_ec_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属预备ec线名（老）\"}},{\"name\":\"core_id_code\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"所属核心（老）\"}},{\"name\":\"core_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"所属核心线名（老）\"}},{\"name\":\"etl_create_at\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"ETL创建时间\"}},{\"name\":\"etl_update_at\",\"type\":\"string\",\"nullable\":true,\"metadata\":', \n" +
            "  'spark.sql.sources.schema.partCol.0'='dt', \n" +
            "  'transient_lastDdlTime'='1635144346')\n";
}
