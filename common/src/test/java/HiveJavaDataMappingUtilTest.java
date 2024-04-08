import org.junit.Test;
import util.HiveJavaDataMappingUtil;

import static util.HiveDDLParserUtil.*;

public class HiveJavaDataMappingUtilTest {


    @Test
    public void testDataMapping(){

        System.out.println(HiveJavaDataMappingUtil.parseHiveIntoJava(HIVE_AST_TOK_TYPE_SMALL_INT));
        System.out.println(HiveJavaDataMappingUtil.parseHiveIntoJava(HIVE_AST_TOK_TYPE_STRING));
        System.out.println(HiveJavaDataMappingUtil.parseHiveIntoJava(HIVE_AST_TOK_TYPE_BIGINT));
        System.out.println(HiveJavaDataMappingUtil.parseHiveIntoJava(""));


    }
}
