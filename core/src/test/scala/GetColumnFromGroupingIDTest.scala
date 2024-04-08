import com.access.core.GetColumnFromGroupingID.{convertIntToBinaryWithSize, getColumnsFromID}
import org.junit.Test

class GetColumnFromGroupingIDTest {

  @Test
  def test(): Unit = {
    val CLICKHOUSE_JDBC_URL="jdbc:clickhouse://10.90.21.211:8123"
    val CLICKHOUSE_TABLE_NAME="test.user_label_detail_period_bitmap_view_distribute"
    val HIVE_TABLE_NAME="access_cdm.dwt_seller_profit_seller_cycle_dh_f"
    val DIM_IDS_BESIDES_USER="cycle"
    val COLUMNS="monthly_abm_income,monthly_three_income,monthly_abm_income_range,monthly_three_income_range"
    val BATCH_DATE="2021-10-29"
    val PARTED="2021-11-01-19"
    val SCHEMA="user&cycle"

    val strings = getColumnsFromID(DIM_IDS_BESIDES_USER, COLUMNS, 14)

    println("*********************  start ********************* ")
    strings.foreach(println)

    val strings1 = convertIntToBinaryWithSize(2047, 12)
    strings1.foreach(println)
  }
}
