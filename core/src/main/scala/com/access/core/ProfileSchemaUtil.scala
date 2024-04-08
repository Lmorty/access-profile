package com.access.core

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ProfileSchemaUtil {

  /**
   * 用户粒度常量
   */
  val GRANULARITY_USER = "user"

  /**
   * 用户&品牌粒度常量
   */
  val GRANULARITY_USER_BRAND = "user&brand"

  /**
   * 用户&类目粒度常量
   */
  val GRANULARITY_USER_CATEGORY = "user&category"

  /**
   * 用户&周期粒度常量
   */
  val GRANULARITY_USER_CYCLE = "user&cycle"

  /**
   * 用户&品牌&周期粒度常量
   */
  val GRANULARITY_USER_BRAND_CYCLE = "user&brand&cycle"

  /**
   * 用户&标品粒度常量
   */
  val GRANULARITY_USER_PRODUCT = "user&product"

  def getScheme(granularityType:String): StructType ={

    granularityType match {
      case GRANULARITY_USER => {
        StructType(
          List(
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("id_codes", StringType),
            StructField("parted", StringType)
          ))
      }

      case GRANULARITY_USER_CATEGORY => {
        StructType(
          List(
            StructField("preference", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("id_codes", StringType),
            StructField("parted", StringType)
          ))
      }


      case GRANULARITY_USER_BRAND => {
        StructType(
          List(
            StructField("brand", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("id_codes", StringType),
            StructField("parted", StringType)
          ))
      }

      case GRANULARITY_USER_CYCLE => {
        StructType(
          List(
            StructField("period", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("id_codes", StringType),
            StructField("parted", StringType)
          ))
      }

      case GRANULARITY_USER_BRAND_CYCLE => {
        StructType(
          List(
            StructField("brand", StringType),
            StructField("period", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("id_codes", StringType),
            StructField("parted", StringType)
          ))
      }

      case GRANULARITY_USER_PRODUCT => {
        StructType(
          List(
            StructField("product", StringType),
            StructField("label_value", StringType),
            StructField("label_id", StringType),
            StructField("label_name", StringType),
            StructField("id_codes", StringType),
            StructField("parted", StringType)
          ))
      }

    }

  }
}
