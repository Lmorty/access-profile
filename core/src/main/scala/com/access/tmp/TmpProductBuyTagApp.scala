package com.access.tmp

import com.access.core.{RBMFunctionRegistration, RBMSerializeToClickhouse}
import com.access.core.RBMSerializeToClickhouse.serialize
import hive.po.access_cdm.{DimUserLabelDhF, DwtSellerStudySellerCourseDhF, DwtSellerStudySellerPackageDhF}
import hive.po.bigdata_ods.{OdsMarketLuckyAwardDrawRecordDhF, OdsMarketLuckyAwardTimesRecordDhF}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.unsafe.types.ByteArray
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, DataInputStream}
import java.text.SimpleDateFormat
import java.util
import util.{Base64, Calendar, Properties}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try

/**
 * 标品购买标签需要考虑未购买的人,以此种方式来测试性能
 */
object TmpProductBuyTagApp {

  def main(args: Array[String]): Unit = {


    val BATCH_DATE = args(0)
    var version = args(1)

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = sdf.parse(version)
    val c = Calendar.getInstance()
    c.setTime(date)
    c.add(Calendar.HOUR, -1)
    val newsdf = new SimpleDateFormat("yyyy-MM-dd-HH")

    version = newsdf.format(c.getTime)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    RBMFunctionRegistration.registerFunctions(spark)

    import spark.implicits._


    spark.sql("ADD JAR hdfs:///resource/jar/access-udf-jar-with-dependencies.jar")
    spark.sql("create TEMPORARY FUNCTION bitmap_collect as 'com.access.bigdata.udaf.hive.CollectBitmapUDAF'")
    spark.sql("create TEMPORARY FUNCTION bitmap_andnot as 'com.access.bigdata.udf.hive.BitmapAndNot'")
    spark.sql("create TEMPORARY FUNCTION bitmap_to_base64_string as 'com.access.bigdata.udf.hive.BitmapToBase64String'")

    val frame = spark.sql(
        s"""
           |with tmp_1 as (
           |select
           |    product_name,
           |    bitmap_collect(id_code) as buyed_users
           |from access_cdm.dwt_user_order_user_product_dh_f
           |where dt = '${BATCH_DATE}' and last_order_date_product is not null and id_code is not null
           |group by product_name
           |),
           |tmp_3 as (
           |select
           |    a.product_name,
           |    b.buyed_users
           |from bigdata_ods.ods_goods_center_base_product_dh_f a
           |left join tmp_1 b on a.product_name = b.product_name
           |where a.dt = '${BATCH_DATE}' and a.deleted = 0 and a.status = 0
           |),
           |tmp_2 as (
           |    select bitmap_collect(id_code) as all_users from access_cdm.dim_seller_all_dh_f where dt = '${BATCH_DATE}' and id_code is not null
           |)
           |select
           |    product_name,1 as label_value,bitmap_to_base64_string(buyed_users) as user_set
           |from tmp_3
           |where buyed_users is not null
           |
           |union all
           |
           |select
           |    product_name,0 as label_value,bitmap_to_base64_string(bitmap_andnot(b.all_users,a.buyed_users)) as user_set
           |from tmp_3 a
           |left join tmp_2 b on true
           |where buyed_users is not null
           |
           |union  all
           |
           |select
           |    product_name,0 as label_value,bitmap_to_base64_string(b.all_users) as user_set
           |from tmp_3 a
           |left join tmp_2 b on true
           |where buyed_users is null
           |""".stripMargin)
      .repartition(800)
      .rdd
      .mapPartitions { iter =>
        val lb = new ListBuffer[(String, String, String, String, String, String)]

        while (iter.hasNext) {
          val row = iter.next()
          val product_name = row.getAs[String]("product_name")
          val label_value = row.getAs[Int]("label_value")
          val user_set = row.getAs[String]("user_set")
          val label_id = "is_unpurchased_product"

          val decode = Base64.getDecoder.decode(user_set.getBytes)
          val bitmap = new RoaringBitmap
          bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(decode)))

          val ckBitMapByteArray = RBMSerializeToClickhouse.serialize(bitmap).array()

          val ckBitMapString = new String(Base64.getEncoder.encode(ckBitMapByteArray))

          lb.+=(
            (label_id, label_id, label_value.toString, product_name, version, ckBitMapString)
          )
        }
        lb.iterator
      }.toDF("label_id", "label_name", "label_value", "product", "parted", "id_codes")



    val prop = new Properties()
    prop.put(JDBCOptions.JDBC_DRIVER_CLASS, "ru.yandex.clickhouse.ClickHouseDriver")
    prop.put(JDBCOptions.JDBC_BATCH_INSERT_SIZE, "2000000")
    prop.put(JDBCOptions.JDBC_NUM_PARTITIONS, "800")
    prop.put("rewriteBatchedStatements", "true")

    frame.write
      .mode(SaveMode.Append)
      .option("isolationLevel", "NONE")
      .option("socket_timeout", "120000")
      .option("dataTransferTimeout", "120000")
      .option("keepAliveTimeout", "240000")
      .jdbc("jdbc:clickhouse://10.90.21.211:8123", "bigdata_profile.user_label_detail_product_bitmap_view_distribute", prop)




    spark.stop()
  }
}
