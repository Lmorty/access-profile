package util;


import description.HiveColumnDes;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import description.HiveTableDes;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.parse.ParseUtils.findRootNonNullToken;


/**
 * hive ddl解析工具
 */
public class HiveDDLParserUtil {

   //hive数据类型
   public static final String HIVE_AST_TOK_TYPE_SMALL_INT = "TOK_SMALLINT";
   public static final String HIVE_AST_TOK_TYPE_TINY_INT = "TOK_TINYINT";
   public static final String HIVE_AST_TOK_TYPE_INT = "TOK_INT";
   public static final String HIVE_AST_TOK_TYPE_BIGINT = "TOK_BIGINT";
   public static final String HIVE_AST_TOK_TYPE_FLOAT = "TOK_FLOAT";
   public static final String HIVE_AST_TOK_TYPE_DOUBLE = "TOK_DOUBLE";
   public static final String HIVE_AST_TOK_TYPE_DECIMAL = "TOK_DECIMAL";
   public static final String HIVE_AST_TOK_TYPE_TIMESTAMP = "TOK_TIMESTAMP";
   public static final String HIVE_AST_TOK_TYPE_DATE = "TOK_DATE";
   public static final String HIVE_AST_TOK_TYPE_STRING = "TOK_STRING";
   public static final String HIVE_AST_TOK_TYPE_VARCHAR = "TOK_VARCHAR";
   public static final String HIVE_AST_TOK_TYPE_CHAR = "TOK_CHAR";
   public static final String HIVE_AST_TOK_TYPE_BOOLEAN = "TOK_BOOLEAN";
   public static final String HIVE_AST_TOK_TYPE_LIST = "TOK_LIST";
   public static final String HIVE_AST_TOK_TYPE_MAP = "TOK_MAP";
   public static final String HIVE_AST_TOK_TYPE_STRUCT = "TOK_STRUCT";
   public static final String HIVE_AST_TOK_TYPE_UNIONTYPE = "TOK_UNIONTYPE";


   //java类型
   //hive数据类型
   public static final String JAVA_TYPE_INTEGER = "Integer";
   public static final String JAVA_TYPE_LONG = "Long";
   public static final String JAVA_TYPE_FLOAT = "Float";
   public static final String JAVA_TYPE_DOUBLE = "Double";
   public static final String JAVA_TYPE_DECIMAL = "BigDecimal";
   public static final String JAVA_TYPE_DATE = "Date";
   public static final String JAVA_TYPE_STRING = "String";
   public static final String JAVA_TYPE_BOOLEAN = "Boolean";
   public static final String JAVA_TYPE_LIST = "List";
   public static final String JAVA_TYPE_MAP = "Map";

   //TODO 待定
   public static final String JAVA_TYPE_STRUCT = "";
   public static final String JAVA_TYPE_UNIONTYPE = "";



   private ParseDriver pd = new ParseDriver();
   private ASTNode node = null;
   private ArrayList<Node> rootChildren = null;

   private HiveDDLParserUtil(){};

   public static HiveDDLParserUtil createInstance(){
      return new HiveDDLParserUtil();
   }

   public HiveTableDes parse(String ddlStatement){
      try {
         node = pd.parse(ddlStatement.replace("`", " ").replace(";",""));
         node = findRootNonNullToken(node);
         rootChildren = node.getChildren();


         String fullTableName = extractTableName();
         ArrayList<HiveColumnDes> columnDes = extractColumnDesc();
         ArrayList<HiveColumnDes> partColumnDesc = extractPartColumnDesc();
         String tableComment = extractTableComment();

         return new HiveTableDes(fullTableName,tableComment,columnDes,partColumnDesc);

      }catch (Exception e){
         e.printStackTrace();
      }

      return null;

   }

   /**
    * 从Node中找出库表名
    */
   private String extractTableName(){
      String dbName = null;
      String tableName = null;
      if(null != rootChildren){
         for (Node n : rootChildren) {
            ASTNode astn = (ASTNode) n;

            if("TOK_TABNAME".equals(astn.token.getText())){
               dbName =  ((ASTNode) n.getChildren().get(0)).token.getText();
               tableName =  ((ASTNode) n.getChildren().get(1)).token.getText();
               break;
            }
         }
      }
      return dbName + "." + tableName;
   }



   /**
    * 抽取表描述信息
    */
   private String extractTableComment(){
      String tableComment = null;

      if(null != rootChildren){
         for (Node n : rootChildren) {
            ASTNode astn = (ASTNode) n;

            if("TOK_TABLECOMMENT".equals(astn.token.getText())){
               tableComment =  ((ASTNode) n.getChildren().get(0)).token.getText().replace("'","");
               break;
            }
         }
      }
      return tableComment;
   }



   /**
    * 抽取字段信息
    * @return
    */
   private ArrayList<HiveColumnDes> extractColumnDesc(){
      ArrayList<HiveColumnDes> list = new ArrayList<>(32);

      ASTNode TokTabColListNode = null;

      if(null != rootChildren){
         for (Node n : rootChildren) {
            ASTNode astn = (ASTNode) n;
            if("TOK_TABCOLLIST".equals(astn.token.getText())){
               TokTabColListNode = astn;
               break;
            }
         }
      }


      //astNode为每一个字段的描述对象
      for (Node astNode : TokTabColListNode.getChildren()) {

         HiveColumnDes columnDes = new HiveColumnDes();

         List<ASTNode> colDes = astNode.getChildren().stream().map(node -> (ASTNode) node).collect(Collectors.toList());

         try{
            String colName = ListUtil.getIfExists(colDes, 0).token.getText();
            columnDes.setColName(colName);

            String colType = ListUtil.getIfExists(colDes, 1).token.getText();
            columnDes.setColType(HiveJavaDataMappingUtil.parseHiveIntoJava(colType));

            ASTNode commentNode = ListUtil.getIfExists(colDes, 2);
            if(null != commentNode && null != commentNode.token.getText()){
               columnDes.setColComment(commentNode.token.getText());
            }
         }catch (Exception e){
            e.printStackTrace();
         }

         list.add(columnDes);
      }

      return list;
   }


   /**
    * 抽取分区信息
    */
   private ArrayList<HiveColumnDes> extractPartColumnDesc(){
      ArrayList<HiveColumnDes> list = new ArrayList<>(32);

      ASTNode TokTabColListNode = null;

      if(null != rootChildren){
         for (Node n : rootChildren) {
            ASTNode astn = (ASTNode) n;
            if("TOK_TABLEPARTCOLS".equals(astn.token.getText())){
               TokTabColListNode = astn;
               break;
            }
         }
      }


      //astNode为每一个字段的描述对象
      for (Node astNode : TokTabColListNode.getChildren()) {

         HiveColumnDes columnDes = new HiveColumnDes();

         List<ASTNode> partColDes = astNode.getChildren().stream().map(node -> (ASTNode) node).collect(Collectors.toList())
                 .get(0).getChildren()
                 .stream().map(node -> (ASTNode) node)
                 .collect(Collectors.toList());

         try{
            String colName = ListUtil.getIfExists(partColDes, 0).token.getText();
            columnDes.setColName(colName);

            String colType = ListUtil.getIfExists(partColDes, 1).token.getText();
            columnDes.setColType(HiveJavaDataMappingUtil.parseHiveIntoJava(colType));

            ASTNode commentNode = ListUtil.getIfExists(partColDes, 2);
            if(null != commentNode && null != commentNode.token.getText()){
               columnDes.setColComment(commentNode.token.getText());
            }
         }catch (Exception e){
            e.printStackTrace();
         }

         list.add(columnDes);
      }

      return list;
   }

}
