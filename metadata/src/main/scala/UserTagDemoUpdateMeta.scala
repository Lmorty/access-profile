import hive.po.access_cdm.{DimAwardActivityDhF, DimCouponDhF}
import hive.po.bigdata_ods.{OdsMarketLuckyAwardDrawRecordDhF, OdsMarketLuckyAwardTimesRecordDhF}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * 更新画像元数据任务
 * TODO: 因为元数据设计问题,一期未考虑太多。实现方式不好,后期再改。元数据模块先独立出来。
 */
object UserTagDemoUpdateMeta {

  val META_PRAM_MEASURE_INFO = "param_measure_info"
  val url = "jdbc:mysql://10.110.5.240:3306/anole_pro?useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false"
  val username = "data_analysis"
  val password = "p5AWT85sj9RSOvm8oif1"
  val driver = "com.mysql.jdbc.Driver"

  def main(args: Array[String]): Unit = {


    val BATCH_DATE = args(0)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    //更新券ID
    updateCouponList(spark,BATCH_DATE)

    //更新抽奖活动ID
    updateDrawIdList(spark,BATCH_DATE)

    //更新抽奖活动ID - 2
    updateDrawActIdList(spark,BATCH_DATE)


    /**************************************** 抽奖活动ID-END *********************************************************/

    spark.stop()

  }

  def updateCouponList(spark:SparkSession,BATCH_DATE:String): Unit ={
    import spark.implicits._
    val COUPON_PARAM_ID = 128

    val properties = new Properties()
    properties.setProperty("user", username)
    properties.setProperty("password",password)
    properties.setProperty("driver",driver)
    properties.setProperty("url",url)

    /**************************************** 优惠券ID-START *********************************************************/

    val mysqlCouponIdSet: Set[String] = spark.read.jdbc(url, META_PRAM_MEASURE_INFO, properties)
      .where(s" param_id = ${COUPON_PARAM_ID}")
      .persist()
      .rdd
      .map(_.getAs[String]("param_value"))
      .collect()
      .toSet



    val hiveCouponIdSet: Set[String] = spark.sql(
      s"""
         |select
         |    cast(coupon_id as string) as coupon_id
         |from ${DimCouponDhF.getTableName}
         |where dt = '${BATCH_DATE}'
         |""".stripMargin)
      .rdd
      .collect()
      .map(_.getAs[String]("coupon_id"))
      .toSet


    val set: Set[String] = hiveCouponIdSet.filter(!mysqlCouponIdSet.contains(_))


    import spark.implicits._

    val frame : DataFrame = set.toList.map(couponId =>
      (COUPON_PARAM_ID, couponId, System.currentTimeMillis(), System.currentTimeMillis())
    ).toDF("param_id", "param_value", "create_time", "update_time")
      .persist()

    frame.write.
      option(JDBCOptions.JDBC_BATCH_INSERT_SIZE,200)
      .mode(SaveMode.Append)
      .jdbc(url, META_PRAM_MEASURE_INFO, properties)


    /**************************************** 优惠券ID-END *********************************************************/
  }


  def updateDrawIdList(spark:SparkSession,BATCH_DATE:String): Unit ={
    import spark.implicits._
    /**************************************** 抽奖活动ID-START *********************************************************/

    val DRAW_PARM_ID = 130

    val properties = new Properties()
    properties.setProperty("user", username)
    properties.setProperty("password",password)
    properties.setProperty("driver",driver)
    properties.setProperty("url",url)

    val mysqlActivitySet: Set[String] = spark.read.jdbc(url, META_PRAM_MEASURE_INFO, properties)
      .where(s" param_id = ${DRAW_PARM_ID}")
      .persist()
      .rdd
      .map(_.getAs[String]("param_value"))
      .collect()
      .toSet

    val hiveActivitySet: Set[String] = spark.sql(
      s"""
         |SELECT
         |    distinct activity_id
         |FROM ${OdsMarketLuckyAwardTimesRecordDhF.getTableName}
         |where dt = '${BATCH_DATE}' and status = 0 and deleted = 0
         |""".stripMargin)
      .rdd
      .collect()
      .map(_.getAs[Long]("activity_id").toString)
      .toSet


    val changeSet: Set[String] = hiveActivitySet.filter(!mysqlActivitySet.contains(_))

    val activityChangeDF : DataFrame = changeSet.toList.map(activityId =>
      (DRAW_PARM_ID, activityId, System.currentTimeMillis(), System.currentTimeMillis())
    ).toDF("param_id", "param_value", "create_time", "update_time")
      .persist()

    activityChangeDF.write.
      option(JDBCOptions.JDBC_BATCH_INSERT_SIZE,200)
      .mode(SaveMode.Append)
      .jdbc(url, META_PRAM_MEASURE_INFO, properties)
  }

  def updateDrawActIdList(spark:SparkSession,BATCH_DATE:String): Unit ={
    import spark.implicits._
    /**************************************** 抽奖活动ID-START *********************************************************/

    val DRAW_ACT_PARM_ID = 290

    val properties = new Properties()
    properties.setProperty("user", username)
    properties.setProperty("password",password)
    properties.setProperty("driver",driver)
    properties.setProperty("url",url)

    val mysqlActivitySet: Set[String] = spark.read.jdbc(url, META_PRAM_MEASURE_INFO, properties)
      .where(s" param_id = ${DRAW_ACT_PARM_ID}")
      .persist()
      .rdd
      .map(_.getAs[String]("param_value"))
      .collect()
      .toSet

    val hiveActivitySet: Set[String] = spark.sql(
      s"""
         |SELECT
         |    distinct act_id
         |FROM ${DimAwardActivityDhF.getTableName}
         |where dt = '${BATCH_DATE}'
         |""".stripMargin)
      .rdd
      .collect()
      .map(_.getAs[Long]("act_id").toString)
      .toSet


    val changeSet: Set[String] = hiveActivitySet.filter(!mysqlActivitySet.contains(_))

    val activityChangeDF : DataFrame = changeSet.toList.map(activityId =>
      (DRAW_ACT_PARM_ID, activityId, System.currentTimeMillis(), System.currentTimeMillis())
    ).toDF("param_id", "param_value", "create_time", "update_time")
      .persist()

    activityChangeDF.write.
      option(JDBCOptions.JDBC_BATCH_INSERT_SIZE,200)
      .mode(SaveMode.Append)
      .jdbc(url, META_PRAM_MEASURE_INFO, properties)
  }
}
