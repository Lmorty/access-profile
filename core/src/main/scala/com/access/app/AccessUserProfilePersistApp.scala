package com.access.app

import com.access.core.ProfileSchemaUtil.{GRANULARITY_USER, GRANULARITY_USER_BRAND, GRANULARITY_USER_BRAND_CYCLE, GRANULARITY_USER_CATEGORY, GRANULARITY_USER_CYCLE, GRANULARITY_USER_PRODUCT}
import com.access.core._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.roaringbitmap.RoaringBitmap
import util.ParameterTool

import java.io.{ByteArrayInputStream, DataInputStream}
import java.text.SimpleDateFormat
import java.util.{Base64, Calendar, Properties}
import scala.collection.immutable
import scala.collection.mutable.ListBuffer

object AccessUserProfilePersistApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport()
      .getOrCreate()

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sparkContext.setLogLevel("error")

    val parameterTool = ParameterTool.fromArgs(args)


    // 为SparkSQL使用注册RBM functions
    RBMFunctionRegistration.registerFunctions(spark)

    //hive表名
    val hiveTableName = parameterTool.getRequired("hive_tbl")
    //除了id_code以外的维度名
    val dimIdsBesidesUser = parameterTool.get("extra_dims", "").toLowerCase()
    //需要用到的列
    val hiveColumns = parameterTool.getRequired("hive_columns")
    //hive分区
    val batchDate = parameterTool.getRequired("dt")
    //更新类型
    val strategy = parameterTool.getRequired("strategy")
    //粒度
    val granularity = parameterTool.getRequired("granularity")


    //刷新元数据缓存
    spark.catalog.refreshTable(hiveTableName)

    val runSQL: String = BitMapSqlGenerator.generate(hiveTableName, dimIdsBesidesUser, hiveColumns, batchDate)


    val frame = spark.sql(runSQL).persist()


    val rawRdd: RDD[Row] = frame.rdd.mapPartitions { iter =>
      val listBuffer = new ListBuffer[Row]
      while (iter.hasNext) {
        val row = iter.next()

        val valueTuple = GetColumnFromGroupingID.generateResultFormat(row, dimIdsBesidesUser, hiveColumns)

        val values = valueTuple._1
        val labelID = valueTuple._2

        val bytes = row.getAs[Array[Byte]]("rbm_arr")


        //解决因为字段中带逗号导致的分割错误
        if (null != values && null != bytes) {
          val rbm = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

          val ckBitMapByteArray = RBMSerializeToClickhouse.serialize(rbm).array()

          val ckBitMapString = new String(Base64.getEncoder.encode(ckBitMapByteArray))

          //放弃以前分隔符分割容易因为特殊字符导致问问题
          val parted: String = strategy match {
            case "day" => {
              val sdf = new SimpleDateFormat("yyyy-MM-dd")
              val date = sdf.parse(batchDate)
              val bdt = Calendar.getInstance
              bdt.setTime(date)
              bdt.add(Calendar.DATE, 1)
              sdf.format(bdt.getTime)
            }
            case "hour" => batchDate
          }

          val strings: immutable.Seq[String] = values ++ Array(labelID, labelID, ckBitMapString, parted, strategy, hiveTableName)
          listBuffer.+=(Row.fromSeq(strings))
        }
      }

      listBuffer.iterator
    }


    spark.createDataFrame(rawRdd, matchSchema(granularity))
      .write
      //      .partitionBy("dt","update_strategy","source")
      .mode(SaveMode.Overwrite)
      .insertInto(matchSink(granularity))

    spark.stop()
  }


  private def matchSink(granularityType: String): String = {
    granularityType match {
      case GRANULARITY_USER => "access_ads.ads_profile_user_bitmap_dh_f"

      case GRANULARITY_USER_CATEGORY => "access_ads.ads_profile_user_category_bitmap_dh_f"

      case GRANULARITY_USER_BRAND => "access_ads.ads_profile_user_brand_bitmap_dh_f"

      case GRANULARITY_USER_CYCLE => "access_ads.ads_profile_user_cycle_bitmap_dh_f"

      case GRANULARITY_USER_BRAND_CYCLE => "access_ads.ads_profile_user_brand_cycle_bitmap_dh_f"

      case GRANULARITY_USER_PRODUCT => "access_ads.ads_profile_user_product_bitmap_dh_f"
    }
  }

  private def matchSchema(granularityType: String): StructType = {
    granularityType match {
      case GRANULARITY_USER =>
        StructType(Array(
          StructField("label_value", StringType),
          StructField("label_id", StringType),
          StructField("label_name", StringType),
          StructField("rbm_base64_string", StringType),
          StructField("dt", StringType),
          StructField("update_strategy", StringType),
          StructField("source", StringType)
        ))


      case GRANULARITY_USER_CATEGORY =>
        StructType(
          List(
            StructField("preference", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("rbm_base64_string", StringType),
            StructField("dt", StringType),
            StructField("update_strategy", StringType),
            StructField("source", StringType)
          ))


      case GRANULARITY_USER_BRAND =>
        StructType(
          List(
            StructField("brand", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("rbm_base64_string", StringType),
            StructField("dt", StringType),
            StructField("update_strategy", StringType),
            StructField("source", StringType)
          ))


      case GRANULARITY_USER_CYCLE =>
        StructType(
          List(
            StructField("period", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("rbm_base64_string", StringType),
            StructField("dt", StringType),
            StructField("update_strategy", StringType),
            StructField("source", StringType)
          ))


      case GRANULARITY_USER_BRAND_CYCLE =>
        StructType(
          List(
            StructField("brand", StringType),
            StructField("period", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("rbm_base64_string", StringType),
            StructField("dt", StringType),
            StructField("update_strategy", StringType),
            StructField("source", StringType)
          ))


      case GRANULARITY_USER_PRODUCT =>
        StructType(
          List(
            StructField("product", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("rbm_base64_string", StringType),
            StructField("dt", StringType),
            StructField("update_strategy", StringType),
            StructField("source", StringType)
          ))

    }
  }
}
