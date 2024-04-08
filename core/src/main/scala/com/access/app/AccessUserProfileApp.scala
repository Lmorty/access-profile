package com.access.app

import com.access.core.{BitMapSqlGenerator, GetColumnFromGroupingID, ProfileSchemaUtil, RBMFunctionRegistration, RBMSerializeToClickhouse}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util.{Base64, Properties}
import scala.collection.immutable
import scala.collection.mutable.ListBuffer

object AccessUserProfileApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    // 为SparkSQL使用注册RBM functions
    RBMFunctionRegistration.registerFunctions(spark)

    //如参配置
    //ck链接
    val clickHouseJdbcUrl = args(0)
    //ck表名
    val clickHOuseTableName = args(1)
    //hive表名
    val table = args(2)
    //除了id_code以外的维度名
    val dimIdsBesidesUser = if("null".equals(args(3).toLowerCase)) "" else args(3)
    //需要用到的列
    val columns = args(4)
    //hive分区
    val batchDate = args(5)
    //分区标识 yyyy-MM-dd-HH
    val parted = args(6)
    //schema类型标识
    val schemaType = args(7)


    //刷新元数据缓存
    spark.catalog.refreshTable(table)

    val runSQL: String = BitMapSqlGenerator.generate(table,dimIdsBesidesUser,columns,batchDate)


    val schema = ProfileSchemaUtil.getScheme(schemaType)

    val frame = spark.sql(runSQL).persist()

    frame.show(100,true)

    val ckRawDF: RDD[Row] = frame.rdd.mapPartitions { iter =>
      val listBuffer = new ListBuffer[Row]
      while (iter.hasNext) {
        val row = iter.next()

        val valueTuple = GetColumnFromGroupingID.generateResultFormat(row, dimIdsBesidesUser, columns)

        val values = valueTuple._1
        val labelID = valueTuple._2

        val bytes = row.getAs[Array[Byte]]("rbm_arr")


        //解决因为字段中带逗号导致的分割错误
        if(null != values && null != bytes){
          val rbm = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))

          val ckBitMapByteArray = RBMSerializeToClickhouse.serialize(rbm).array()

          val ckBitMapString = new String(Base64.getEncoder.encode(ckBitMapByteArray))

          //放弃以前分隔符分割容易因为特殊字符导致问问题
          val strings: immutable.Seq[String] = values ++ Array(labelID, labelID, ckBitMapString, parted)
          listBuffer.+=(Row.fromSeq(strings))
        }

      }

      listBuffer.iterator
    }


    ckRawDF.persist()

    frame.unpersist()

    val ckDF = spark.createDataFrame(ckRawDF, schema)

    ckDF.persist()


    println("-------------------------------------分割线-------------------------------------")
    ckRawDF.take(1).foreach(println(_))
    println("-------------------------------------分割线-------------------------------------")
    println(schema)
    println("-------------------------------------分割线-------------------------------------")
    println(ckRawDF.filter(_.getString(0) == null).count())
    println(ckRawDF.filter(_.getString(1) == null).count())
    println(ckRawDF.filter(_.getString(2) == null).count())
    println(ckRawDF.filter(_.getString(3) == null).count())
    println("-------------------------------------分割线-------------------------------------")
    ckDF.show(5,false)
    println("-------------------------------------分割线-------------------------------------")
    ckDF.printSchema()
    println("-------------------------------------分割线-------------------------------------")

    val prop = new Properties()
    prop.put(JDBCOptions.JDBC_DRIVER_CLASS,"ru.yandex.clickhouse.ClickHouseDriver")
    prop.put(JDBCOptions.JDBC_BATCH_INSERT_SIZE,"2000")
    prop.put(JDBCOptions.JDBC_NUM_PARTITIONS,"20")
    prop.put("rewriteBatchedStatements","true")


    ckDF
      .write
      .mode(SaveMode.Append)
      .option("isolationLevel", "NONE")
      .option("socket_timeout","120000")
      .option("dataTransferTimeout","120000")
      .option("keepAliveTimeout","240000")
      .jdbc(clickHouseJdbcUrl,clickHOuseTableName,prop)

    spark.stop()
  }




}
