import description.HiveTableDes;
import org.junit.Test;

public class HiveTableDesTest {

    @Test
    public void testObj(){

        HiveTableDes hiveTableDes = new HiveTableDes("access_cdm.dim_seller_all", "null", null, null);

        System.out.println(hiveTableDes.dbName);
        System.out.println(hiveTableDes.tableName);
    }
}
