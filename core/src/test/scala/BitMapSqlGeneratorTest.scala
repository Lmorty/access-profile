import com.access.core.BitMapSqlGenerator.generate
import org.junit.Test


class BitMapSqlGeneratorTest {

  @Test
  def test(): Unit ={
    val CLICKHOUSE_JDBC_URL="jdbc:clickhouse://10.90.21.211:8123"
    val CLICKHOUSE_TABLE_NAME="test.user_label_detail_bitmap_view_distribute"
    val HIVE_TABLE_NAME="access_cdm.dwt_user_property_user_dh_f"
    val DIM_IDS_BESIDES_USER=""
    val COLUMNS="user_level,v_value,user_created_date,is_co_account,inviter_is_co_account,bl_cust_is_co_account,user_country,user_province,user_city,is_fill_invite_code,user_life_cycle,v_score"
    val BATCH_DATE="2021-10-29"
    val PARTED="2021-11-02-10"
    val SCHEMA="user"

    val sql: String = generate(HIVE_TABLE_NAME,DIM_IDS_BESIDES_USER,COLUMNS,BATCH_DATE)

    println(sql)
  }

}
