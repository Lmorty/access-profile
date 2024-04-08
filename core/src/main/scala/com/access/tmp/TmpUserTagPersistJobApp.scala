package com.access.tmp


import com.access.core.RBMFunctionRegistration
import com.access.core.RBMSerializeToClickhouse.serialize
import hive.po.access_cdm.{DimUserLabelDhF, DwtSellerStudySellerCourseDhF, DwtSellerStudySellerPackageDhF}
import hive.po.bigdata_ods.{OdsMarketLuckyAwardDrawRecordDhF, OdsMarketLuckyAwardTimesRecordDhF}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util.Base64
import scala.collection.mutable.ListBuffer

object TmpUserTagPersistJobApp {

  def main(args: Array[String]): Unit = {


    val BATCH_DATE = args(0)
    val BATCH_HOUR = args(1)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    RBMFunctionRegistration.registerFunctions(spark)

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val schema = StructType(Array(
      StructField("label_value", StringType),
      StructField("label_id", StringType),
      StructField("label_name", StringType),
      StructField("rbm_base64_string", StringType),
      StructField("dt", StringType),
      StructField("update_strategy", StringType),
      StructField("source", StringType)
    ))

    //此次只有读取未使用的优惠券标签


    spark.sql(
      s"""
        |select
        |    id_code,
        |    cast(coupon_id as string) as coupon_id
        |from access_cdm.dwd_mbs_user_coupon_dh_f
        |where dt = '${BATCH_DATE}'
        |  and used_status = 0 and is_delete = 0
        |  and coupon_end_time >= concat('${BATCH_DATE}',' ','${BATCH_HOUR}',':00:00')
        |""".stripMargin).persist().createOrReplaceTempView("tmp")


    val frame = spark.sql(
      """
        |with
        |tmp_conver_into_rbm_id as (
        |select
        |   cast(coupon_id as string) as coupon_id,
        |   rbm_init(cast(id_code as int)) as rbm_id
        |from tmp
        |),
        |tmp_convert_into_rbm as (
        |select
        |   coupon_id,
        |   rbm_merge(rbm_id) as rbm_arr,
        |   rbm_cardinality(rbm_merge(rbm_id)) as rbm_length
        |from tmp_conver_into_rbm_id
        |group by coupon_id
        |)
        |select
        |    coupon_id,
        |    rbm_arr
        |from tmp_convert_into_rbm
        |""".stripMargin).persist()



    val couponRDD = frame.rdd.mapPartitions { iter =>
      val lb = new ListBuffer[Row]
      while (iter.hasNext) {
        val row = iter.next()
        val coupon_id = row.getAs[String]("coupon_id")
        val bytes = row.getAs[Array[Byte]]("rbm_arr")

        val rbm = new RoaringBitmap()
        rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

        val str = new String(Base64.getEncoder.encode(serialize(rbm).array()))

        lb.+=(Row.fromSeq(Seq(coupon_id, "unused_coupon", "unused_coupon", str, BATCH_DATE, "hour", "temp_tag")))
      }

      lb.iterator
    }


    val activityRDD = spark.sql(
      s"""
         |with tmp_1 as (
         |SELECT
         |    activity_id,user_id + 200000 as id_code
         |FROM ${OdsMarketLuckyAwardTimesRecordDhF.getTableName}
         |where dt = '${BATCH_DATE}' and status = 0 and deleted = 0
         |group by activity_id,user_id
         |)
         |select
         |   cast(activity_id as string) as activity_id,
         |   rbm_merge(rbm_init(cast(id_code as int))) as rbm_arr
         |from tmp_1
         |group by activity_id
         |""".stripMargin)
      .rdd
      .mapPartitions { iter =>
        val lb = new ListBuffer[Row]
        while (iter.hasNext) {
          val row = iter.next()
          val activity_id = row.getAs[String]("activity_id")
          val bytes = row.getAs[Array[Byte]]("rbm_arr")

          val rbm = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

          val str = new String(Base64.getEncoder.encode(serialize(rbm).array()))

          lb.+=(Row.fromSeq(Seq(activity_id, "qualified_not_draw", "qualified_not_draw", str, BATCH_DATE, "hour", "temp_tag")))
        }
        lb.iterator
      }




    val courseIsCompletedRDD = spark.sql(
      s"""
         |with tmp_1 as (
         |SELECT
         |    course_id,
         |    id_code
         |FROM ${DwtSellerStudySellerCourseDhF.getTableName}
         |where dt = '${BATCH_DATE}' and course_is_completed = 1
         |)
         |select
         |   cast(course_id as string) as course_id,
         |   rbm_merge(rbm_init(cast(id_code as int))) as rbm_arr
         |from tmp_1
         |group by course_id
         |""".stripMargin)
      .rdd
      .mapPartitions { iter =>
        val lb = new ListBuffer[Row]
        while (iter.hasNext) {
          val row = iter.next()
          val course_id = row.getAs[String]("course_id")
          val bytes = row.getAs[Array[Byte]]("rbm_arr")

          val rbm = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

          val str = new String(Base64.getEncoder.encode(serialize(rbm).array()))
          lb.+=(Row.fromSeq(Seq(course_id, "course_is_completed", "course_is_completed", str, BATCH_DATE, "hour", "temp_tag")))
        }
        lb.iterator
      }
    /****************************************************************************************************/


    val courseIsNotCompletedRDD = spark.sql(
      s"""
         |with tmp_1 as (
         |SELECT
         |    course_id,
         |    id_code
         |FROM ${DwtSellerStudySellerCourseDhF.getTableName}
         |where dt = '${BATCH_DATE}' and course_is_completed = 0
         |)
         |select
         |   cast(course_id as string) as course_id,
         |   rbm_merge(rbm_init(cast(id_code as int))) as rbm_arr
         |from tmp_1
         |group by course_id
         |""".stripMargin)
      .rdd
      .mapPartitions { iter =>
        val lb = new ListBuffer[Row]
        while (iter.hasNext) {
          val row = iter.next()
          val course_id = row.getAs[String]("course_id")
          val bytes = row.getAs[Array[Byte]]("rbm_arr")

          val rbm = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

          val str = new String(Base64.getEncoder.encode(serialize(rbm).array()))

          lb.+=(Row.fromSeq(Seq(course_id, "course_is_not_completed", "course_is_not_completed", str, BATCH_DATE, "hour", "temp_tag")))
        }
        lb.iterator
      }


    //经销商&课程包
    val sellerPackageIsCompleteRDD = spark.sql(
      s"""
         |with tmp_1 as (
         |SELECT
         |    package_id,
         |    id_code
         |FROM ${DwtSellerStudySellerPackageDhF.getTableName}
         |where dt = '${BATCH_DATE}' and package_is_completed = 1
         |)
         |select
         |   cast(package_id as string) as package_id,
         |   rbm_merge(rbm_init(cast(id_code as int))) as rbm_arr
         |from tmp_1
         |group by package_id
         |""".stripMargin)
      .rdd
      .mapPartitions { iter =>
        val lb = new ListBuffer[Row]
        while (iter.hasNext) {
          val row = iter.next()
          val package_id = row.getAs[String]("package_id")
          val bytes = row.getAs[Array[Byte]]("rbm_arr")

          val rbm = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

          val str = new String(Base64.getEncoder.encode(serialize(rbm).array()))

          lb.+=(Row.fromSeq(Seq(package_id, "package_is_completed", "package_is_completed", str, BATCH_DATE, "hour", "temp_tag")))
        }
        lb.iterator
      }

    val sellerPackageIsNotCompleteDF = spark.sql(
      s"""
         |with tmp_1 as (
         |SELECT
         |    package_id,
         |    id_code
         |FROM ${DwtSellerStudySellerPackageDhF.getTableName}
         |where dt = '${BATCH_DATE}' and package_is_completed = 0
         |)
         |select
         |   cast(package_id as string) as package_id,
         |   rbm_merge(rbm_init(cast(id_code as int))) as rbm_arr
         |from tmp_1
         |group by package_id
         |""".stripMargin)
      .rdd
      .mapPartitions { iter =>
        val lb = new ListBuffer[Row]
        while (iter.hasNext) {
          val row = iter.next()
          val package_id = row.getAs[String]("package_id")
          val bytes = row.getAs[Array[Byte]]("rbm_arr")

          val rbm = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

          val str = new String(Base64.getEncoder.encode(serialize(rbm).array()))

          lb.+=(Row.fromSeq(Seq(package_id, "package_is_not_completed", "package_is_not_completed", str, BATCH_DATE, "hour", "temp_tag")))
        }
        lb.iterator
      }
    /****************************************************************************************************/




    /*****************************************经销商出货额TOP10000***********************************************************/
    val sellerMoneyOutTop10000RDD = spark.sql(
      s"""
         |with
         |--获取数据对应的结算周期
         |ods_data_key_config as (
         |select
         |    11111 as data_key,
         |    concat(date_sub('${BATCH_DATE}',365),' 00:00:00') as start_time,
         |    date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as end_time
         |),
         |--获取工单信息
         |ods_gongdan as (
         |    select
         |        id,
         |        create_time
         |    from bigdata_ods.ods_gongdan_afs_tickets_dh_f
         |    where dt = '${BATCH_DATE}'
         |),
         |--获取money表信息
         |ods_money as (
         |    select
         |        id_code,
         |        from_user_id + 200000 as from_id_code,
         |        team_ratio_type,
         |        money_out,
         |        status,
         |        return_id,
         |        created_at,
         |        updated_at,
         |        cancel_order_time
         |    from bigdata_ods.ods_abmau_w_user_brand_provider_money_dh_f a
         |    left join ods_data_key_config b on true
         |    where a.dt = '${BATCH_DATE}' and abm_level = 5 and money_out != 0
         |),
         |--对money表进行字段扩宽
         |dwd_money as (
         |    select *
         |    from
         |        (select
         |             a.id_code,
         |             a.from_id_code,
         |             a.team_ratio_type,
         |             a.return_id,
         |             a.money_out,
         |             a.status,
         |             to_date(a.created_at) as create_date,
         |             if(a.return_id > 0 or a.status = 2,to_date(coalesce(b.create_time, a.cancel_order_time, a.updated_at)),null) as closed_date,
         |             c.data_key,
         |             c.start_time,
         |             c.end_time
         |         from ods_money a
         |                  left join ods_gongdan b on a.return_id - 100000 = b.id
         |                  left join ods_data_key_config c on true) t
         |    where create_date between start_time and end_time or closed_date between start_time and end_time
         |),
         |ads_money_out as (
         |    select
         |        id_code,
         |        data_key,
         |        team_ratio_type,
         |        --个人本次结算出货额,自购
         |        --条件 id_code = from_id_code, create_date处于范围, return_id 无效
         |        sum(if(
         |                        id_code = from_id_code and create_date between start_time and end_time and (return_id = 0 or return_id is null),
         |                        money_out,
         |                        0
         |            )) as self_total_money_out,
         |        --个人本次结算内取消出货额,条件 id_code = from_id_code,create_date处于结算范围,close_date处于范围, 当status = 2时,money_out,当 return_id > 0时 money_out * -1
         |        sum(case
         |                when id_code = from_id_code and create_date between start_time and end_time and closed_date between start_time and end_time and status = 2 then money_out
         |                when id_code = from_id_code and create_date between start_time and end_time and closed_date between start_time and end_time and return_id > 0 then money_out * -1
         |                else 0
         |            end
         |            ) as self_cancel_money_out,
         |        --小团队本次结算周日内总共出货额,不包含自己 条件 id_code != from_id_code, create_date处于范围, return_id 无效
         |        sum(if(
         |                        (id_code != from_id_code or from_id_code is null) and create_date between start_time and end_time and (return_id = 0 or return_id is null),
         |                        money_out,
         |                        0
         |            )) as small_team_total_money_out,
         |        --小团队本次结算周期内取消出货额,条件 id_code != from_id_code,create_date和close_date处于范围, 当status = 2时,money_out,当 return_id > 0时 money_out * -1
         |        sum(case
         |                when (id_code != from_id_code or from_id_code is null) and create_date between start_time and end_time and closed_date between start_time and end_time and status = 2 then money_out
         |                when (id_code != from_id_code or from_id_code is null) and create_date between start_time and end_time and closed_date between start_time and end_time and return_id > 0 then money_out * -1
         |                else 0
         |            end
         |            ) as small_team_cancel_money_out,
         |        --个人取消非本本次结算周期内出货额,条件 id_code = from_id_code,close_date处于范围,create_date不处于范围内，当status = 2时,money_out,当 return_id > 0时 money_out * -1
         |        sum(case
         |                when id_code = from_id_code and create_date not between start_time and end_time and closed_date between start_time and end_time and status = 2 then money_out
         |                when id_code = from_id_code and create_date not between start_time and end_time and closed_date between start_time and end_time and return_id > 0 then money_out * -1
         |                else 0
         |            end
         |            ) as self_cancel_before_money_out,
         |        --小团队取消非本本次结算周期内出货额,条件 id_code != from_id_code,close_date处于范围,create_date不处于范围内，当status = 2时,money_out,当 return_id > 0时 money_out * -1
         |        sum(case
         |                when (id_code != from_id_code or from_id_code is null) and create_date not between start_time and end_time and closed_date between start_time and end_time and status = 2 then money_out
         |                when (id_code != from_id_code or from_id_code is null) and create_date not between start_time and end_time and closed_date between start_time and end_time and return_id > 0 then money_out * -1
         |                else 0
         |            end
         |            ) as small_team_cancel_before_money_out
         |    from dwd_money
         |    where create_date between start_time and end_time or end_time between start_time and end_time
         |    group by id_code,team_ratio_type,data_key
         |),
         |tmp_1 as (
         |select
         |   id_code,
         |   sum(self_total_money_out - self_cancel_money_out + small_team_total_money_out - small_team_cancel_money_out - self_cancel_before_money_out - small_team_cancel_before_money_out) as money_out
         |from ads_money_out
         |group by id_code
         |),
         |tmp_2 as (
         |select
         |   id_code,
         |   if(rn <= 10000,1,0) as top_10000_money_out_seller_p1y
         |from
         |(select
         |   id_code,
         |   row_number() over (order by money_out desc) as rn
         |from tmp_1) t
         |)
         |select
         |   cast(top_10000_money_out_seller_p1y as string) as top_10000_money_out_seller_p1y,
         |   rbm_merge(rbm_init(cast(id_code as int))) as rbm_arr
         |from tmp_2
         |group by top_10000_money_out_seller_p1y
         |""".stripMargin)
      .rdd
      .mapPartitions { iter =>
        val lb = new ListBuffer[Row]
        while (iter.hasNext) {
          val row = iter.next()
          val flag = row.getAs[String]("top_10000_money_out_seller_p1y")
          val bytes = row.getAs[Array[Byte]]("rbm_arr")

          val rbm = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

          val str = new String(Base64.getEncoder.encode(serialize(rbm).array()))

          lb.+=(Row.fromSeq(Seq(flag, "top_10000_money_out_seller_p1y", "top_10000_money_out_seller_p1y", str, BATCH_DATE, "hour", "temp_tag")))
        }
        lb.iterator
      }




    /*****************************************经销商出货额TOP10000***********************************************************/



    /******************************************已抽奖用户**********************************************************/
    val drawedUserDF = spark.sql(
      s"""
         |with tmp_1 as (
         |SELECT
         |    act_id,user_id + 200000 as id_code
         |FROM ${OdsMarketLuckyAwardDrawRecordDhF.getTableName}
         |where dt = '${BATCH_DATE}' and deleted = 0
         |group by act_id,user_id
         |)
         |select
         |   cast(act_id as string) as act_id,
         |   rbm_merge(rbm_init(cast(id_code as int))) as rbm_arr
         |from tmp_1
         |group by act_id
         |""".stripMargin)
      .rdd
      .mapPartitions { iter =>
        val lb = new ListBuffer[Row]
        while (iter.hasNext) {
          val row = iter.next()
          val act_id = row.getAs[String]("act_id")
          val bytes = row.getAs[Array[Byte]]("rbm_arr")

          val rbm = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

          val str = new String(Base64.getEncoder.encode(serialize(rbm).array()))

          lb.+=(Row.fromSeq(Seq(act_id, "drawed_user", "drawed_user", str, BATCH_DATE, "hour", "temp_tag")))
        }
        lb.iterator
      }
    /******************************************已抽奖用户**********************************************************/


    spark.createDataFrame(couponRDD,schema)
      .unionAll(spark.createDataFrame(activityRDD,schema))
      .unionAll(spark.createDataFrame(courseIsCompletedRDD,schema))
      .unionAll(spark.createDataFrame(courseIsNotCompletedRDD,schema))
      .unionAll(spark.createDataFrame(sellerPackageIsCompleteRDD,schema))
      .unionAll(spark.createDataFrame(sellerPackageIsNotCompleteDF,schema))
      .unionAll(spark.createDataFrame(sellerMoneyOutTop10000RDD,schema))
      .unionAll(spark.createDataFrame(drawedUserDF,schema))
      .write
      .mode(SaveMode.Overwrite)
      .insertInto("access_ads.ads_profile_user_bitmap_dh_f")

    spark.stop()
  }
}
