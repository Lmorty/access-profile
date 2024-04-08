package util;

import com.google.common.collect.Maps;
import description.HiveColumnDes;
import description.HiveTableDes;
import freemarker.template.Configuration;
import freemarker.template.Template;

import java.io.*;
import java.util.*;

import static util.StringUtil.upperFirstLatter;

public class GenerateHiveEntityUtil {

    public static final String HIVE_FTL_TEMPLATE_PATH = "dao/src/main/java/hive/templates";
    public static final String HIVE_PO_ROOT_DIR = "dao/src/main/java/hive/po";
    public static final String HIVE_PO_ROOT_PACKAGE_NAME = "hive.po";
    public static final String HIVE_TEMPLATE_FILE = "hive.ftl";


    public static void createEntity(String ddl){
        // step1 解析DDL
        HiveDDLParserUtil hiveParser = HiveDDLParserUtil.createInstance();
        HiveTableDes tableDes = hiveParser.parse(ddl);

        // step2 创建freeMarker配置实例
        Configuration configuration = new Configuration(Configuration.VERSION_2_3_22);
        Writer out = null;
        try {
            // step3 获取模版路径
            configuration.setDirectoryForTemplateLoading(new File(HIVE_FTL_TEMPLATE_PATH));
            // step4 创建数据模型
            Map<String, Object> dataMap = Maps.newHashMap();

            // step4.1 库名当包名
            String dbName = tableDes.getDbName().toLowerCase(Locale.ROOT);

            dataMap.put("packageName",HIVE_PO_ROOT_PACKAGE_NAME+"."+dbName);

            File dir = new File(HIVE_PO_ROOT_DIR + "/" + dbName);
            if(!dir.exists()){
                dir.mkdirs();
            }

            // step4.2 表名当类名
            String tableName = upperFirstLatter(StringUtil.convertIntoCamelCase(tableDes.getTableName(),"_"));

            dataMap.put("className", tableName);


            // step4.3 字段当成员变量
            ArrayList<Map<String, String>> maps = new ArrayList<>();
            List<HiveColumnDes> colsDescription = tableDes.getColsDescription();
            List<HiveColumnDes> partColsDescription = tableDes.getPartColsDescription();


            //数据字段信息
            for (HiveColumnDes hcd : colsDescription) {
                HashMap<String, String> map = Maps.newHashMap();
                map.put("type",hcd.colType);
                map.put("name",StringUtil.convertIntoCamelCase(hcd.colName,"_"));
                maps.add(map);
            }

            //分区字段信息
            for (HiveColumnDes hcd : partColsDescription) {
                HashMap<String, String> map = Maps.newHashMap();
                map.put("type",hcd.colType);
                map.put("name",StringUtil.convertIntoCamelCase(hcd.colName,"_"));
                maps.add(map);
            }

            dataMap.put("properties",maps);

            dataMap.put("tableName",tableDes.getFullTableName());

            // step5 加载模版文件
            Template template = configuration.getTemplate(HIVE_TEMPLATE_FILE);
            // step6 生成数据
            File docFile = new File( HIVE_PO_ROOT_DIR + "/" + dbName + "/" + tableName + ".java");

            if(docFile.exists()){
                docFile.delete();
            }

            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(docFile)));
            // step7 输出文件
            template.process(dataMap, out);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != out) {
                    out.flush();
                }
                if( null != out){
                    out.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        String ddl = "CREATE TABLE `access_cdm.dim_award_activity_dh_f`(                                            \n" +
                "   `act_id` bigint COMMENT '主键',                                                             \n" +
                "   `act_name` string COMMENT '活动名称',                                                       \n" +
                "   `act_start_time` string COMMENT '活动开始时间',                                             \n" +
                "   `act_end_time` string COMMENT '活动结束时间',                                               \n" +
                "   `draw_start_time` string COMMENT '抽奖开始时间',                                            \n" +
                "   `draw_end_time` string COMMENT '抽奖结束时间',                                              \n" +
                "   `draw_type` bigint COMMENT '抽奖类型： 1-特惠升黑钻、2-积分抽奖、3-指定用户、4-会员活动',       \n" +
                "   `draw_type_desc` string COMMENT '抽奖类型描述',                                             \n" +
                "   `act_status` bigint COMMENT '活动状态： 1-预热、2-上线、3-下线',                               \n" +
                "   `act_status_desc` string COMMENT '活动状态描述',                                            \n" +
                "   `award_send_type` bigint COMMENT '发奖方式： 1-自动发奖、2-手动发奖',                         \n" +
                "   `award_send_desc` string COMMENT '发奖方式描述',                                            \n" +
                "   `act_rule` string COMMENT '规则说明',                                                       \n" +
                "   `is_share_flag` bigint COMMENT '是否分享 0-不分享、1-分享',                                  \n" +
                "   `share_poster` string COMMENT '分享海报',                                                   \n" +
                "   `share_title` string COMMENT '分享标题',                                                    \n" +
                "   `share_desc` string COMMENT '分享描述',                                                     \n" +
                "   `win_award_rule` string COMMENT '兑奖说明',                                                 \n" +
                "   `created_at` string COMMENT '创建时间',                                                     \n" +
                "   `created_user` bigint COMMENT '创建人',                                                     \n" +
                "   `updated_at` string COMMENT '最后更新时间',                                                 \n" +
                "   `updated_user` bigint COMMENT '最后更新人',                                                 \n" +
                "   `is_deleted` bigint COMMENT '逻辑删除： 0-未删除、1-已删除')                                  \n" +
                " COMMENT '抽奖活动维度表'                                                                      \n" +
                " PARTITIONED BY (                                                                              \n" +
                "   `dt` string COMMENT '天分区(yyyy-MM-dd)')   ";

        GenerateHiveEntityUtil.createEntity(ddl);
    }
}
