package com.access.tmp

import com.access.core.RBMSerializeToClickhouse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import ru.yandex.clickhouse.BalancedClickhouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

import java.sql.{Connection, DriverManager, Statement}
import scala.collection.mutable.ListBuffer

object AccessGroupToHiveApp {
  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._

    val ckURL = "jdbc:clickhouse://10.90.20.128:8123"
    //    val ckURL = "jdbc:clickhouse://10.70.20.72:8123"

    val clickHouseProperties = new ClickHouseProperties
    clickHouseProperties.setUser("default")
    val dataSource = new BalancedClickhouseDataSource(ckURL, clickHouseProperties)
    dataSource.actualize
    val connection = dataSource.getConnection
    val statement = connection.createStatement

    val rs1 = statement.executeQuery("select max(toUInt64(group_id)) as max_id,min(toUInt64(group_id)) as min_id from bigdata_profile.user_group_result_distribute final where toUInt64(group_id) < 100000")
    val rs2 = statement.executeQuery("select max(toUInt64(group_id)) as max_id,min(toUInt64(group_id)) as min_id from bigdata_profile.user_mark_result_distribute final ")

    //1.读取非手动打标
    {
      //分段读取
      var maxID = 0
      var minID = 0
      if (rs1.next()) {
        maxID = rs1.getInt("max_id")
        minID = rs1.getInt("min_id")
      }

      rs1.close()

      val rs1StepRDD: RDD[Int] = spark.sparkContext.makeRDD(Range(minID, maxID + 1).toArray, 40)

      rs1StepRDD.mapPartitions { iter =>
        val lb = new ListBuffer[(String, String, Int)]

        val distriButeCKProp = new ClickHouseProperties
        distriButeCKProp.setUser("default")

        val distributeDataSource = new BalancedClickhouseDataSource(ckURL, distriButeCKProp)
        distributeDataSource.actualize
        val distributeConnection = distributeDataSource.getConnection
        val distributeStatement = distributeConnection.createStatement()

        while (iter.hasNext) {
          val groupID = iter.next()
          val sql = s"select group_id,group_name,idCodes,bitmapCardinality(idCodes) as cardinality from bigdata_profile.user_group_result_distribute final where group_id = '${groupID}' "
          val separateRs = distributeStatement.executeQuery(sql)

          while (separateRs.next()) {
            val bitmapBytes: Array[Byte] = separateRs.getBytes("idCodes")
            val group_id = separateRs.getString("group_id")
            val group_name = separateRs.getString("group_name")
            val cardinality = separateRs.getInt("cardinality")

            logger.info(s"人群ID:${groupID},size=${cardinality}")


            val bitmap = RBMSerializeToClickhouse.deserialization(bitmapBytes, cardinality)
            for (id <- bitmap.toArray) {
              lb += ((group_id, group_name, id))
            }
          }
        }

        if(null != distributeStatement){
          distributeStatement.close()
        }
        if(null != distributeConnection){
          distributeConnection.close()
        }

        lb.iterator
      }.toDF("group_id", "group_name", "id_code").createOrReplaceTempView("tmp")
    }

    //2.读取手动打标
    {
      //分段读取
      var maxID = 0
      var minID = 0
      if (rs2.next()) {
        maxID = rs2.getInt("max_id")
        minID = rs2.getInt("min_id")
      }

      rs2.close()

      val rs2StepRDD: RDD[Int] = spark.sparkContext.makeRDD(Range(minID, maxID + 1).toArray, 40)

      rs2StepRDD.mapPartitions { iter =>
        val lb = new ListBuffer[(String, String, Int)]

        val distriButeCKProp = new ClickHouseProperties
        distriButeCKProp.setUser("default")

        val distributeDataSource = new BalancedClickhouseDataSource(ckURL, distriButeCKProp)
        distributeDataSource.actualize
        val distributeConnection = distributeDataSource.getConnection
        val distributeStatement = distributeConnection.createStatement()

        while (iter.hasNext) {
          val groupID = iter.next()
          val sql = s"select group_id,group_name,idCodes,bitmapCardinality(idCodes) as cardinality from bigdata_profile.user_mark_result_distribute final where group_id = '${groupID}' order by create_time desc limit 1 "
          val separateRs = distributeStatement.executeQuery(sql)

          while (separateRs.next()) {
            val bitmapBytes: Array[Byte] = separateRs.getBytes("idCodes")
            val group_id = separateRs.getString("group_id")
            val group_name = separateRs.getString("group_name")
            val cardinality = separateRs.getInt("cardinality")

            val bitmap = RBMSerializeToClickhouse.deserialization(bitmapBytes, cardinality)
            for (id <- bitmap.toArray) {
              lb += ((group_id, group_name, id))
            }
          }
        }

        if (null != distributeStatement) {
          distributeStatement.close()
        }
        if (null != distributeConnection) {
          distributeConnection.close()
        }

        lb.iterator
      }.toDF("group_id", "group_name", "id_code").createOrReplaceTempView("tmp_mark")
    }

    try{

      //      spark.sql(
      //        """
      //          |insert overwrite table test_db.tmp
      //          |select group_id,group_name,id_code from tmp
      //          |""".stripMargin)
      //      spark.sql(
      //        """
      //          |insert overwrite table test_db.tmp_mark
      //          |select group_id,group_name,id_code from tmp_mark
      //          |""".stripMargin)


      spark.sql(
        """
          |insert overwrite table access_cdm.dim_profile_user_group_mapping
          |select group_id,group_name,id_code from tmp
          |union all
          |select group_id,group_name,id_code from tmp_mark
          |""".stripMargin)
    }catch{
      case e:Exception =>{
        e.printStackTrace()
        throw new Exception("报错")
      }
    }finally{
      statement.close()
      connection.close()
      spark.stop()
    }
  }

}
