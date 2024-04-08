import com.access.core.{GetColumnFromGroupingID, RBMFunctionRegistration, RBMSerializeToClickhouse, VarInt}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.junit.Test
import org.roaringbitmap.RoaringBitmap
import ru.yandex.clickhouse.response.ClickHouseResultSet
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseArray, ClickHouseConnection, ClickHouseStatement}
import ru.yandex.clickhouse.settings.ClickHouseProperties

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.{sql, util}
import java.util.{ArrayList, Base64, Calendar, Properties}
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

class AppTest {

  @Test
  def test(): Unit ={

    val spark = SparkSession.builder().enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    // 为SparkSQL使用注册RBM functions
    RBMFunctionRegistration.registerFunctions(spark)

    // 构造测试数据
    spark.range(100000).toDF("id").createOrReplaceTempView("ids")


    println("原始数据")
    spark.sql(
      """
        |select
        |  *
        |from ids
        |""".stripMargin).show(false)

    spark.sql(
      """
        |select
        |   id % 10 as id_mod,
        |   rbm_init(cast(id as int)) as rbm_id
        |from ids
        |""".stripMargin).createOrReplaceTempView("preaggregate")

    println("rbm_init数据,将id转为字节数组")
    spark.sql("select * from preaggregate").show(false)


    spark.sql(
      """
        |select
        |   id_mod,
        |   rbm_merge(rbm_id) as rbm
        |from preaggregate
        |group by id_mod
        |order by id_mod
        |""".stripMargin).printSchema()



    println("按 id_mod聚合之后将结果聚合")

    // rbm_cardinality 用来计算位图的个数

    spark.sql(
      """
        |select
        |   id_mod,
        |   rbm_cardinality(rbm_merge(rbm_id)) as acntd
        |from preaggregate
        |group by id_mod
        |order by id_mod
        |""".stripMargin).show()


    spark.sql("select id_mod % 2 as id_mod2,  rbm_cardinality(rbm_merge(rbm_id)) as acntd from preaggregate group by id_mod % 2").show()


    //导出
    // 构造待写入数据
    val rows = new util.ArrayList[Row]()
    rows.add(Row(1234561L, new String(Base64.getEncoder.encode(RBMSerializeToClickhouse.serialize(RoaringBitmap.bitmapOf(1 until 13: _*)).array()))))
    rows.add(Row(1234562L, new String(Base64.getEncoder.encode(RBMSerializeToClickhouse.serialize(RoaringBitmap.bitmapOf(1 until 133: _*)).array()))))
    rows.add(Row(1234563L, new String(Base64.getEncoder.encode(RBMSerializeToClickhouse.serialize(RoaringBitmap.bitmapOf(1 until 1334: _*)).array()))))
    rows.add(Row(1234564L, new String(Base64.getEncoder.encode(RBMSerializeToClickhouse.serialize(RoaringBitmap.bitmapOf(1 until 133334: _*)).array()))))
    val schema = StructType(
      List(
        StructField("ds", LongType),
        StructField("user", StringType)
      ))
    val df = spark.createDataFrame(rows, schema)

    df.show()


//    val prop = new Properties()
//    prop.put(JDBCOptions.JDBC_DRIVER_CLASS,"ru.yandex.clickhouse.ClickHouseDriver")
//    prop.put(JDBCOptions.JDBC_BATCH_INSERT_SIZE,"100")
//    prop.put(JDBCOptions.JDBC_NUM_PARTITIONS,"2")
//    prop.put("rewriteBatchedStatements","true")
//
//
//    val clickHouseJdbcUrl = "jdbc:clickhouse://10.70.20.72:8123"
//    val clickHOuseTableName = "test.bitmap_test"
//
//    df.repartition(400)
//      .write
//      .mode(SaveMode.Append)
//      .option("isolationLevel", "NONE")
//      .option("socket_timeout","120000")
//      .option("dataTransferTimeout","120000")
//      .option("keepAliveTimeout","240000")
//      .jdbc(clickHouseJdbcUrl,clickHOuseTableName,prop)

    spark.stop()
  }

  @Test
  def test1(): Unit = {
    val BATCH_DATE = "2022-01-31"

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(BATCH_DATE)


    val current = Calendar.getInstance
    current.setTime(date)

    val CURRENT_MONTH_DAYS = current.get(Calendar.DAY_OF_MONTH)
    val CURRENT_MONTH_MAX_DAYS = current.getActualMaximum(Calendar.DAY_OF_MONTH)

    val lastMonth = Calendar.getInstance
    lastMonth.setTime(date)
    lastMonth.add(Calendar.MONTH, -1)
    val LAST_MONTH_DAYS = lastMonth.getActualMaximum(Calendar.DAY_OF_MONTH)

    val LAST_MONTH_END = if (CURRENT_MONTH_DAYS >= LAST_MONTH_DAYS) {
      val c = Calendar.getInstance()
      c.setTime(date)
      c.set(Calendar.DAY_OF_MONTH, 1)
      c.add(Calendar.DATE, -1)
      sdf.format(c.getTime)
    } else if (CURRENT_MONTH_DAYS.equals(CURRENT_MONTH_MAX_DAYS)) {
      val c = Calendar.getInstance
      c.setTime(date)
      c.set(Calendar.DAY_OF_MONTH, 1)
      c.add(Calendar.DATE, -1)
      sdf.format(c.getTime)
    } else {
      val c = Calendar.getInstance
      c.setTime(date)
      c.set(Calendar.DAY_OF_MONTH, CURRENT_MONTH_DAYS)
      c.add(Calendar.MONTH, -1)
      sdf.format(c.getTime)
    }

    val LAST_MONTH_START: String = {
      val c = Calendar.getInstance
      c.setTime(date)
      c.set(Calendar.DAY_OF_MONTH, 1)
      c.add(Calendar.MONTH, -1)
      sdf.format(c.getTime)
    }

    val CURRENT_MONTH_START = {
      val c = Calendar.getInstance
      c.setTime(date)
      c.set(Calendar.DAY_OF_MONTH, 1)
      sdf.format(c.getTime)
    }

    val CURRENT_MONTH_END = BATCH_DATE

    println(
      s"""
         |当前月份的计算周期: 开始日期=$CURRENT_MONTH_START , 结束日期=$CURRENT_MONTH_END
         |上个月份的计算周期: 开始日期=$LAST_MONTH_START , 结束日期=$LAST_MONTH_END
         |""".stripMargin)


    //季度部分
    val ccc = Calendar.getInstance
    ccc.setTime(date)

    val currentDayOfMonth = ccc.get(Calendar.DAY_OF_MONTH)
    val currentDayOfMonthMax = ccc.getActualMaximum(Calendar.DAY_OF_MONTH)

    val currentMonth = ccc.get(Calendar.MONTH) + 1

    if (currentMonth >= 1 && currentMonth <= 3) {
      ccc.set(Calendar.MONTH, 0)
    } else if (currentMonth >= 4 && currentMonth <= 6) {
      ccc.set(Calendar.MONTH, 3)
    } else if (currentMonth >= 7 && currentMonth <= 9) {
      ccc.set(Calendar.MONTH, 6)
    } else if (currentMonth >= 10 && currentMonth <= 12) {
      ccc.set(Calendar.MONTH, 9)
    }
    ccc.set(Calendar.DAY_OF_MONTH, 1)

    val THIS_QUARTER_START = sdf.format(ccc.getTime)
    val THIS_QUARTER_END = BATCH_DATE

    ccc.add(Calendar.MONTH, -3)


    val LAST_QUARTER_START = sdf.format(ccc.getTime)

    ccc.setTime(date)


    ccc.add(Calendar.MONTH, -3)


    val LAST_QUARTER_END = if (currentDayOfMonth.equals(currentDayOfMonthMax)) {
      ccc.add(Calendar.MONTH, 1)
      ccc.set(Calendar.DAY_OF_MONTH, 1)
      ccc.add(Calendar.DATE, -1)
      sdf.format(ccc.getTime)
    } else {
      val LAST_QUARTER_MAX_DAYS = ccc.getActualMaximum(Calendar.DAY_OF_MONTH)
      ccc.set(Calendar.DAY_OF_MONTH, Math.min(CURRENT_MONTH_DAYS, LAST_QUARTER_MAX_DAYS))
      sdf.format(ccc.getTime)
    }

    println(
      s"""
         |当前季度的计算周期: 开始日期=$THIS_QUARTER_START , 结束日期=$THIS_QUARTER_END
         |上个季度的计算周期: 开始日期=$LAST_QUARTER_START , 结束日期=$LAST_QUARTER_END
         |""".stripMargin)

    val version = System.currentTimeMillis()

    println(
      s"""
         |当前计算版本: version = $version
         |""".stripMargin)
  }


  @Test
  def test3(): Unit = {

    val version = "20221222161939"

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = sdf.parse(version)

    val c = Calendar.getInstance()
    c.setTime(date)

    c.add(Calendar.HOUR,-1)


    val newsdf = new SimpleDateFormat("yyyy-MM-dd-HH")


    println(newsdf.format(c.getTime))
  }

  @Test
  def test4(): Unit = {
    val clickHouseProperties = new ClickHouseProperties
    clickHouseProperties.setUser("default")

    val dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://10.70.20.72:8123", clickHouseProperties)
    //对每个host进行ping 操作，排除不可用链接
    dataSource.actualize
    val connection = dataSource.getConnection
    val statement = connection.createStatement

    val resultSet = statement.executeQuery("select parted,idCodes,bitmapCardinality(idCodes) as cardinality  from bigdata_profile.user_label_detail_bitmap_view_distribute final ")

    val buffer = new ListBuffer[(String,  Array[Byte], Int)]
    while(resultSet.next()){
      val bitmapBytes: Array[Byte] = resultSet.getBytes("idCodes")
      val parted = resultSet.getString("parted")
      val cardinality = resultSet.getInt("cardinality")

      val bitmap = deserizelize(bitmapBytes, cardinality)
      println(parted + ":" + " \n "
        + bitmap.getCardinality + "\n"
        + cardinality + "\n"
        + bitmapBytes.mkString(",")
      )
      buffer += ((parted,bitmapBytes,cardinality))
    }
  }


  @Test
  def test5(): Unit = {
    val bitmap = new RoaringBitmap()
//    for(i <- 0 until 100){
//      bitmap.add(i)
//    }


    val ckBitMapByteArray = RBMSerializeToClickhouse.serialize(bitmap).array()
    val ckBitMapString = new String(Base64.getEncoder.encode(ckBitMapByteArray))
    val cardinality = bitmap.getCardinality

    println(ckBitMapString)

    val bitmapBase64Bytes = ckBitMapString.getBytes()
    val bitmapSerilizedBytes: Array[Byte] = Base64.getDecoder.decode(bitmapBase64Bytes)

    val deserilizedRBM = new RoaringBitmap()
    if(cardinality <= 32){
      try{
        for(i <- bitmapSerilizedBytes.slice(2, bitmapSerilizedBytes.size)){
          deserilizedRBM.add(i.toInt)
        }
      }catch{
        case e:Exception => throw new Exception("bitmap parse error");
      }
    }else{
      //字节数组第一位为标志位
      //字节数组第二位 位VarInt Code编码 32位整形为 1～5
      //字节数组第三位 roaringbitmap序列化
      for (varIntCodeSize <- 1 to 5) {
        val tmpRBM = new RoaringBitmap()
        breakable{
          try {
            //val array: Array[Byte] = bitmapSerilizedBytes.slice(1, 1 + varIntCodeSize)
            //val varIntValue = VarInt.getVarInt(ByteBuffer.wrap(array))
            val rbmSerializeBytes = bitmapSerilizedBytes.slice(1 + varIntCodeSize, bitmapSerilizedBytes.size)
            tmpRBM.deserialize(ByteBuffer.wrap(rbmSerializeBytes))

            if (VarInt.varIntSize(tmpRBM.serializedSizeInBytes()) == varIntCodeSize) {
              deserilizedRBM.deserialize(ByteBuffer.wrap(rbmSerializeBytes))
            }
          } catch {
            case e: java.nio.BufferUnderflowException => break()
            case e: java.io.IOException => break()
            case e: Exception => throw new Exception("bitmap parse error");
          }
        }
      }
    }

    println(deserilizedRBM.getCardinality)
    println(deserilizedRBM.toArray.mkString(","))
}


  @Test
  def test6(): Unit = {
    val arr: Array[Byte] = Array(0,4,62,13,3,0,-100,-13,10,0,-79,-113,13,0,84,67,36,0)
    val bytes: Array[Byte] = arr.slice(2, arr.size)

    println(arr.mkString(","))

    val bitmap = new RoaringBitmap()

    bitmap.add(199998)
    bitmap.add(717724)
    bitmap.add(888753)
    bitmap.add(2376532)

    val bytebuffer = RBMSerializeToClickhouse.serialize(bitmap)
    println(bytebuffer.array().mkString(","))
  }


   def deserizelize(ckBitmapBase64String:String,ckBitmapCardinality:Int): RoaringBitmap = {
     val bitmapBase64Bytes = ckBitmapBase64String.getBytes()
     val bitmapSerilizedBytes: Array[Byte] = Base64.getDecoder.decode(bitmapBase64Bytes)

     val deserilizedRBM = new RoaringBitmap()
     if (ckBitmapCardinality <= 32) {
       try {
         var begin = 2
         for (i <- 1 to (bitmapSerilizedBytes.size - 2) / 4) {
           val intValue = bytes2uint32(bitmapSerilizedBytes, begin).toInt
           deserilizedRBM.add(intValue)
           begin = begin + 4
         }
       } catch {
         case e: Exception => throw new Exception("bitmap parse error");
       }
     } else {
       //字节数组第一位为标志位
       //字节数组第二位 位VarInt Code编码 32位整形为 1～5
       //字节数组第三位 roaringbitmap序列化
       for (varIntCodeSize <- 1 to 5) {
         val tmpRBM = new RoaringBitmap()
         breakable {
           try {
             //val array: Array[Byte] = bitmapSerilizedBytes.slice(1, 1 + varIntCodeSize)
             //val varIntValue = VarInt.getVarInt(ByteBuffer.wrap(array))
             val rbmSerializeBytes = bitmapSerilizedBytes.slice(1 + varIntCodeSize, bitmapSerilizedBytes.size)
             tmpRBM.deserialize(ByteBuffer.wrap(rbmSerializeBytes))

             if (VarInt.varIntSize(tmpRBM.serializedSizeInBytes()) == varIntCodeSize) {
               deserilizedRBM.deserialize(ByteBuffer.wrap(rbmSerializeBytes))
             }
           } catch {
             case e: java.nio.BufferUnderflowException => break()
             case e: java.io.IOException => break()
             case e: Exception => throw new Exception("bitmap parse error");
           }
         }
       }
     }

     deserilizedRBM
   }

  def deserizelize(ckBitmapBytes: Array[Byte], ckBitmapCardinality: Int): RoaringBitmap = {
    val bitmapSerilizedBytes: Array[Byte] = ckBitmapBytes

    val deserilizedRBM = new RoaringBitmap()
    if (ckBitmapCardinality <= 32) {
      try {
        var begin = 2
        for (i <- 1 to (bitmapSerilizedBytes.size - 2) / 4) {
          val intValue = bytes2uint32(bitmapSerilizedBytes, begin).toInt
          deserilizedRBM.add(intValue)
          begin = begin + 4
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    } else {
      //字节数组第一位为标志位
      //字节数组第二位 位VarInt Code编码 32位整形为 1～5
      //字节数组第三位 roaringbitmap序列化
      for (varIntCodeSize <- 1 to 5) {
        val tmpRBM = new RoaringBitmap()
        breakable {
          try {
            //val array: Array[Byte] = bitmapSerilizedBytes.slice(1, 1 + varIntCodeSize)
            //val varIntValue = VarInt.getVarInt(ByteBuffer.wrap(array))
            val rbmSerializeBytes = bitmapSerilizedBytes.slice(1 + varIntCodeSize, bitmapSerilizedBytes.size)
            tmpRBM.deserialize(ByteBuffer.wrap(rbmSerializeBytes))

            if (VarInt.varIntSize(tmpRBM.serializedSizeInBytes()) == varIntCodeSize) {
              deserilizedRBM.deserialize(ByteBuffer.wrap(rbmSerializeBytes))
            }
          } catch {
            case e: java.nio.BufferUnderflowException => break()
            case e: java.io.IOException => break()
            case e: Exception => throw new Exception("bitmap parse error");
          }
        }
      }
    }

    deserilizedRBM
  }

  def bytes2uint32(_bytes: Array[Byte], _offset: Int): Long = {
    var b0 = _bytes(_offset + 0) & 0xff
    var b1 = _bytes(_offset + 1) & 0xff
    var b2 = _bytes(_offset + 2) & 0xff
    var b3 = _bytes(_offset + 3) & 0xff
    return ((b3 << 24) | (b2 << 16) | (b1 << 8) | b0).toLong & 0xFFFFFFFFL
  }

//  0,4,62,13,3,0,-100,-13,10,0,-79,-113,13,0,84,67,36,0

  @Test
   def test7(): Unit = {
     val arr: Array[Byte] = Array(0, 4, 62, 13, 3, 0, -100, -13, 10, 0, -79, -113, 13, 0, 84, 67, 36, 0)
     val bytes: Array[Byte] = arr.slice(2, arr.size)
     val i1 = bytes2uint32(bytes, 0)
     val i2 = bytes2uint32(bytes, 4)
     val i3 = bytes2uint32(bytes, 8)
     val i4 = bytes2uint32(bytes, 12)
     println(s"$i1,$i2,$i3,$i4")
   }
}
