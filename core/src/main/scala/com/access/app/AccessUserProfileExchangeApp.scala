package com.access.app

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import util.ParameterTool

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

object AccessUserProfileExchangeApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "omm")

    val tool = ParameterTool.fromArgs(args)


    val hiveTableName = tool.getRequired("hive_tbl")
    val partitionKeys = tool.get("partition_keys", "1")
    val partitionValues = tool.get("partition_values", "1")
    var version = tool.getRequired("version")

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = sdf.parse(version)
    val c = Calendar.getInstance()
    c.setTime(date)
    c.add(Calendar.HOUR, -1)
    val newsdf = new SimpleDateFormat("yyyy-MM-dd-HH")

    version = newsdf.format(c.getTime)


    val sqlWhereClause =
      if (!"null".eq(partitionKeys.toLowerCase())) {
        val tuples: Array[(String, String)] = partitionKeys.split(",").zip(partitionValues.split(","))
        " where " + tuples.map { t =>
          s" ${t._1} = '${t._2}' "
        }.mkString(" and ")
      } else {
        " "
      }


    val prop = new Properties()
    prop.put(JDBCOptions.JDBC_DRIVER_CLASS, "ru.yandex.clickhouse.ClickHouseDriver")
    prop.put(JDBCOptions.JDBC_BATCH_INSERT_SIZE, "2000000")
    prop.put(JDBCOptions.JDBC_NUM_PARTITIONS, "40")
    prop.put("rewriteBatchedStatements", "true")


    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(s"user_profile_bitmap_exchange")
      .enableHiveSupport()
      .getOrCreate()


    try {
      spark.catalog.refreshTable(hiveTableName)
      val tuple = matchColumns(hiveTableName, sqlWhereClause, version)

      val df = spark.sql(tuple._1)

      df.cache()

      df.write
        .mode(SaveMode.Append)
        .option("isolationLevel", "NONE")
        .option("socket_timeout", "120000")
        .option("dataTransferTimeout", "120000")
        .option("keepAliveTimeout", "240000")
        .jdbc("jdbc:clickhouse://10.90.21.211:8123", tuple._2, prop)
    } catch {
      case e: Exception => throw new Exception(e.getMessage)
    } finally {
      spark.stop()
    }
  }

  def matchColumns(tbl: String, whereCluse: String, version: String) = {
    tbl match {
      case "access_ads.ads_profile_user_bitmap_dh_f" => {
        (
          s"""
             |select
             |    label_id,label_name,label_value,'${version}' as parted,rbm_base64_string as id_codes
             |from access_ads.ads_profile_user_bitmap_dh_f
             |${whereCluse}
             |""".stripMargin
          ,
          "bigdata_profile.user_label_detail_bitmap_view_distribute"
        )
      }

      case "access_ads.ads_profile_user_category_bitmap_dh_f" => {
        (
          s"""
             |select
             |    label_id,label_name,label_value,preference,'${version}' as parted,rbm_base64_string as id_codes
             |from access_ads.ads_profile_user_category_bitmap_dh_f
             |${whereCluse}
             |""".stripMargin,
          "bigdata_profile.user_label_detail_preference_bitmap_view_distribute"
        )

      }

      case "access_ads.ads_profile_user_brand_bitmap_dh_f" => {
        (
          s"""
             |select
             |    label_id,label_name,label_value,brand,'${version}' as parted,rbm_base64_string as id_codes
             |from access_ads.ads_profile_user_brand_bitmap_dh_f
             |${whereCluse}
             |""".stripMargin,
          "bigdata_profile.user_label_detail_brand_bitmap_view_distribute"
        )
      }

      case "access_ads.ads_profile_user_cycle_bitmap_dh_f" => {
        (
          s"""
             |select
             |    label_id,label_name,label_value,period,'${version}' as parted,rbm_base64_string as id_codes
             |from access_ads.ads_profile_user_cycle_bitmap_dh_f
             |${whereCluse}
             |""".stripMargin,
          "bigdata_profile.user_label_detail_period_bitmap_view_distribute"
        )
      }

      case "access_ads.ads_profile_user_brand_cycle_bitmap_dh_f" => {
        (
          s"""
             |select
             |    label_id,label_name,label_value,brand,period,'${version}' as parted,rbm_base64_string as id_codes
             |from access_ads.ads_profile_user_brand_cycle_bitmap_dh_f
             |${whereCluse}
             |""".stripMargin,
          "bigdata_profile.user_label_detail_brand_period_bitmap_view_distribute"
        )
      }

      case "access_ads.ads_profile_user_product_bitmap_dh_f" => {
        (
          s"""
             |select
             |    label_id,label_name,label_value,product,'${version}' as parted,rbm_base64_string as id_codes
             |from access_ads.ads_profile_user_product_bitmap_dh_f
             |${whereCluse}
             |""".stripMargin,
          "bigdata_profile.user_label_detail_product_bitmap_view_distribute"
        )
      }

    }

  }

}



