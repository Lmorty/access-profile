package util;

import description.HiveTableDes;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import javax.management.Attribute;
import java.io.*;
import java.net.URL;
import java.util.*;

public class GenerateHivePO {

    private static final String TEMPLATE_PATH = "dao/src/main/java/hive/templates";
    private static final String CLASS_PATH = "dao/src/main/java/hive/po/cdm";
    private static final String PACKAGE_NAME = "";

    private static final String ddlSQL = "";

    public static void main(String[] args) {
        // step1 解析DDL
        HiveDDLParserUtil hiveParser = HiveDDLParserUtil.createInstance();
        HiveTableDes tableDes = hiveParser.parse(ddlSQL);


        // step2 创建freeMarker配置实例
        Configuration configuration = new Configuration(Configuration.VERSION_2_3_22);
        Writer out = null;
        try {
            // step2 获取模版路径
            configuration.setDirectoryForTemplateLoading(new File(TEMPLATE_PATH));
            // step3 创建数据模型
            Map<String, Object> dataMap = new HashMap<String, Object>(8);
            dataMap.put("packageName", "hive.po.cdm");
            dataMap.put("className", "AutoCodeDemo");
            dataMap.put("tableName", "access_cdm.dim_seller_all_dh_f");

            ArrayList<Map<String, String>> maps = new ArrayList<>();

            HashMap<String, String> map1 = new HashMap<>(8);
            map1.put("type","Integer");
            map1.put("name","age");

            HashMap<String, String> map2 = new HashMap<>(8);
            map2.put("type","String");
            map2.put("name","name");

//            maps.add(map2);
            maps.add(map1);

            dataMap.put("properties",maps);

            // step4 加载模版文件
            Template template = configuration.getTemplate("hello.ftl");
            // step5 生成数据
            File docFile = new File( CLASS_PATH+"/"+"AutoCodeDemo.java");

            if(docFile.exists()){
                docFile.delete();
            }

            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(docFile)));
            // step6 输出文件
            template.process(dataMap, out);
            System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^AutoCodeDemo.java 文件创建成功 !");
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




}
