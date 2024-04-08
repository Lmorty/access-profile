package util;

import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.codehaus.janino.Java;

import static util.HiveDDLParserUtil.*;

public class HiveJavaDataMappingUtil {

    /**
     * hive类型映射为java
     * @param hiveTokType
     * @return
     */
    public static String parseHiveIntoJava(String hiveTokType){
        switch (hiveTokType){
            case HIVE_AST_TOK_TYPE_SMALL_INT :
            case HIVE_AST_TOK_TYPE_TINY_INT  :
            case HIVE_AST_TOK_TYPE_INT  :
                return JAVA_TYPE_INTEGER;

            case HIVE_AST_TOK_TYPE_BIGINT :
                return JAVA_TYPE_LONG;

            case HIVE_AST_TOK_TYPE_FLOAT :
                return JAVA_TYPE_FLOAT;

            case HIVE_AST_TOK_TYPE_DOUBLE :
                return JAVA_TYPE_DOUBLE;

            case HIVE_AST_TOK_TYPE_DECIMAL :
                return JAVA_TYPE_DECIMAL;

            case HIVE_AST_TOK_TYPE_DATE:
                return JAVA_TYPE_DATE;

            case HIVE_AST_TOK_TYPE_TIMESTAMP :
            case HIVE_AST_TOK_TYPE_CHAR:
            case HIVE_AST_TOK_TYPE_VARCHAR:
            case HIVE_AST_TOK_TYPE_STRING:
                return JAVA_TYPE_STRING;

            case HIVE_AST_TOK_TYPE_BOOLEAN:
                return JAVA_TYPE_BOOLEAN;

            //TODO 集合类型后面补充
            //HIVE_AST_TOK_TYPE_LIST
            //HIVE_AST_TOK_TYPE_MAP
            //HIVE_AST_TOK_TYPE_STRUCT
            //HIVE_AST_TOK_TYPE_UNIONTYPE

            default: return null;
        }
    }
}
