package com.access.app

import com.access.core.RBMFunctionRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object AccessUserPreferenceSpuApp {
  def main(args: Array[String]): Unit = {

    val BATCH_START_DATE = args(0)
    val BATCH_END_DATE = args(1)


    val spark = SparkSession.builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled","true")
      .config("spark.sql.adaptive.minNumPostShufflePartitions","1")
      .config("spark.sql.adaptive.maxNumPostShufflePartitions","1000")
      .enableHiveSupport()
      .getOrCreate()

    // 为SparkSQL使用注册RBM functions
    RBMFunctionRegistration.registerFunctions(spark)




    spark.sparkContext.setLogLevel("error")

    spark.sql("set hive.exec.dynamic.partition=TRUE")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("ADD JAR hdfs://hacluster/resource/udf/facebook-udfs-1.0.4-SNAPSHOT-uber.jar")
    spark.sql("CREATE TEMPORARY FUNCTION array_exclude_with_value AS 'com.facebook.hive.udf.UDFArrayExcludeWithValue' ")
    spark.sql("CREATE TEMPORARY FUNCTION array_intersect_long AS 'com.facebook.hive.udf.UDFArrayIntersectLong' ")


    if(BATCH_START_DATE != "1970-01-01" && BATCH_END_DATE != "1970-01-01"){
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val start = dateFormat.parse(BATCH_START_DATE)
      val end = dateFormat.parse(BATCH_END_DATE)
      val btDays = (end.getTime - start.getTime)/(1000*3600*24)

      val batchDates = new ArrayBuffer[String]()
      val cal = Calendar.getInstance()
      cal.setTime(dateFormat.parse(BATCH_START_DATE))
//      cal.add(Calendar.HOUR,-1)

      for(i <- Range(0,btDays.toInt+1)){
        cal.add(Calendar.DATE,-1)
        batchDates += dateFormat.format(cal.getTime)
      }

      runSql(batchDates.toArray,spark)
    }else{
      val cal = Calendar.getInstance()
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      cal.add(Calendar.HOUR,-1)
      runSql(Array(dateFormat.format(cal.getTime)),spark)
    }


    spark.stop()
  }

  def runSql(batchDates:Array[String],spark:SparkSession): Unit ={
    import spark.implicits._

    for(batchDate <- batchDates) {
      val batchMonth = batchDate.substring(0, 7)

      //三级类目和spu的映射关系
      val dimCateSpuDF: DataFrame = spark.sql(
        s"""
           |select
           |  cate_name,
           |  rbm_merge(rbm_spu_id) as rbm_spu_list
           |from
           |(select
           |    cate_name,
           |    rbm_init(cast(spu_id as int)) as rbm_spu_id
           |from access_cdm.dim_spu_front_goods_for_bm_dh_f
           |lateral view explode(split(cate_name_list,',')) list as cate_name
           |where dt = '${batchDate}') t
           |group by cate_name
           |""".stripMargin).cache()

      dimCateSpuDF.createOrReplaceTempView("tmp_dim_cate_with_spu")



      val dimCateSpusMap: Map[String, RoaringBitmap] = dimCateSpuDF.rdd.mapPartitions { iter =>
        val lb = new ListBuffer[(String, RoaringBitmap)]
        while (iter.hasNext) {
          val row = iter.next()
          val cateName = row.getAs[String]("cate_name")
          val bytes = row.getAs[Array[Byte]]("rbm_spu_list")
          val rbm: RoaringBitmap = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))
          lb.+=((cateName, rbm))
        }
        lb.iterator
      }.collect().toMap

      val dimCateSpusMapBC = spark.sparkContext.broadcast(dimCateSpusMap)





      //品牌和spu的映射关系
      val dimBrandSpusDF = spark.sql(
        s"""
           |select
           |  brand_id,
           |  rbm_merge(rbm_spu_id) as rbm_spu_list
           |from
           |(select
           |  brand_id,
           |  rbm_init(cast(spu_id as int)) as rbm_spu_id
           |from
           |access_cdm.dim_item_goods_spu_dh_f
           |where dt = '${batchDate}' and spu_status = 1 and is_deleted = 'N') t
           |group by brand_id
           |""".stripMargin).cache()

      dimBrandSpusDF.createOrReplaceTempView("tmp_dim_brand_with_spu")


      val dimBrandSpusMap = dimBrandSpusDF.rdd.mapPartitions { iter =>
        val lb = new ListBuffer[(Int, RoaringBitmap)]
        while (iter.hasNext) {
          val row = iter.next()
          val brandId = row.getAs[Long]("brand_id").toInt
          val bytes = row.getAs[Array[Byte]]("rbm_spu_list")
          val rbm: RoaringBitmap = new RoaringBitmap()
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))
          lb.+=((brandId, rbm))
        }
        lb.iterator
      }.collect().toMap

      val dimBrandSpusMapBC = spark.sparkContext.broadcast(dimBrandSpusMap)

      //订单商品明细数据汇总
      spark.sql(
        s"""
           |--单品
           |select
           |      user_id,
           |    id_code,
           |    to_date(order_paid_at) as event_date,
           |    spu_id,
           |    count_coll as cnt
           |      from access_cdm.dwd_trade_order_goods_dh_f
           |      where dt = '' and order_paid_at is not null and order_closed_at is null and order_status in (1,2,3,4)
           |  and datediff('${batchDate}',to_date(order_paid_at)) between 0 and 30
           |  and is_valid_order = 1
           |  and p_spu_id = 0
           |
           |  union all
           |    --套组
           |    select
           |  user_id,
           |  id_code,
           |  to_date(order_paid_at) as event_date,
           |  p_spu_id as spu_id,
           |  p_goods_cnt as cnt
           |  from access_cdm.dwd_trade_order_p_goods_dh_f
           |  where dt = '${batchDate}' and order_paid_at is not null and order_closed_at is null and  order_status in (1,2,3,4)
           |  and datediff('${batchDate}',to_date(order_paid_at)) between 0 and 365
           |  and is_valid_order = 1 and is_zero_order = 0
           |""".stripMargin).createOrReplaceTempView("tmp_order_spu_detail")


      //每个用户近一个月购买的spu
      spark.sql(
        s"""
           |select
           |   id_code,
           |   rbm_merge(rbm_spu_id) as rbm_spu_list
           |from
           |(select
           |  id_code,
           |  rbm_init(cast(spu_id as int)) as rbm_spu_id
           |from tmp_order_spu_detail
           |where datediff('${batchDate}',event_date) between 0 and 30) t
           |group by id_code
           |""".stripMargin).cache().createOrReplaceTempView("tmp_delete_1")



      //每个等级能购买的spu
      spark.sql(
        s"""
           |select
           |   level,
           |   rbm_merge(rbm_init(cast(spu_id as int))) as rbm_spu_list
           |from access_cdm.dim_item_goods_spu_dh_f
           |lateral view explode(split(support_buy_user_levels,',')) list as level
           |where dt = '${batchDate}' and spu_status = 1 and is_allow_buy = 1
           |group by level
           |""".stripMargin).cache().createOrReplaceTempView("tmp_level_support_spu")




      //需求偏好三级标签映射到商品和得分

      val userCateSpuScoreRdd: RDD[(Long, mutable.HashMap[Int, Int])] = spark.sql(
        s"""
           |select
           |     id_code,
           |     collect_list(concat(cate_name,':',score)) as preference_info
           |from access_ads.ads_bm_user_preference_category_dh_f
           |where dt = '${batchDate}'
           |group by id_code
           |""".stripMargin)
        .rdd
        .repartition(200)
        .mapPartitions { iter =>
          val dimMap = dimCateSpusMapBC.value
          val lb = new ListBuffer[(Long, mutable.HashMap[Int, Int])]

          while (iter.hasNext) {
            val row = iter.next()
            val idCode = row.getAs[Long]("id_code")
            val preferenceInfo = row.getAs[mutable.WrappedArray[String]]("preference_info")

            val preferenceMap = new mutable.HashMap[Int, Int]
            for (info <- preferenceInfo) {
              val arr = info.split(":")
              val cateName = arr(0)
              val score = arr(1).toInt
              val spuIds = dimMap.getOrElse(cateName, new RoaringBitmap()).toArray

              for (spu <- spuIds) {
                if (preferenceMap.contains(spu)) {
                  preferenceMap.+=(spu -> (score + preferenceMap.apply(spu)))
                } else {
                  preferenceMap.+=(spu -> score)
                }
              }
            }
            lb.+=(idCode -> preferenceMap)
          }
          lb.iterator
        }

      //需求偏好对应的spu集合
      userCateSpuScoreRdd.mapPartitions { iter =>
        val lb = new ListBuffer[(Long, Array[Byte])]
        while (iter.hasNext) {
          val rbm: RoaringBitmap = new RoaringBitmap()
          val t = iter.next()
          val idCode = t._1
          for (spu <- t._2.keySet) {
            rbm.add(spu)
          }

          val bos = new ByteArrayOutputStream()
          val stream = new DataOutputStream(bos)
          rbm.serialize(stream)

          val array: Array[Byte] = bos.toByteArray

          lb.+=((idCode, array))
        }
        lb.iterator
      }.toDF("id_code","rbm_spu_list")
        .cache()
        .createOrReplaceTempView("tmp_collection_cate")


      val userBrandSpuScoreRdd = spark.sql(
        s"""
           |select
           |     id_code,
           |     collect_list(concat(brand_id,':',score)) as preference_info
           |from access_ads.ads_bm_user_preference_brand_dh_f
           |where dt = '${batchDate}'
           |group by id_code
           |""".stripMargin)
        .rdd
        .repartition(200)
        .mapPartitions { iter =>
          val dimMap = dimBrandSpusMapBC.value
          val lb = new ListBuffer[(Long, mutable.HashMap[Int, Int])]

          while (iter.hasNext) {
            val row = iter.next()
            val idCode = row.getAs[Long]("id_code")
            val preferenceInfo = row.getAs[mutable.WrappedArray[String]]("preference_info")

            val preferenceMap = new mutable.HashMap[Int, Int]
            for (info <- preferenceInfo) {
              val arr = info.split(":")
              val brandId = arr(0).toInt
              val score = arr(1).toInt
              val spuIds = dimMap.getOrElse(brandId,new RoaringBitmap()).toArray

              for (spu <- spuIds) {
                if (preferenceMap.contains(spu)) {
                  preferenceMap.+=(spu -> (score + preferenceMap.apply(spu)))
                } else {
                  preferenceMap.+=(spu -> score)
                }
              }
            }
            lb.+=(idCode -> preferenceMap)
          }
          lb.iterator
        }


      //品牌偏好对应的spu集合
      userBrandSpuScoreRdd.mapPartitions { iter =>
        val lb = new ListBuffer[(Long, Array[Byte])]
        while (iter.hasNext) {
          val rbm: RoaringBitmap = new RoaringBitmap()
          val t = iter.next()
          val idCode = t._1
          for (spu <- t._2.keySet) {
            rbm.add(spu)
          }
          val bos = new ByteArrayOutputStream()
          val stream = new DataOutputStream(bos)
          rbm.serialize(stream)

          val array: Array[Byte] = bos.toByteArray

          lb.+=((idCode, array))
        }
        lb.iterator
      }.toDF("id_code","rbm_spu_list")
        .cache()
        .createOrReplaceTempView("tmp_collection_brand")


      val userSpuGroupByRDD: RDD[(Long, Iterable[mutable.HashMap[Int, Int]])] = userCateSpuScoreRdd.union(userBrandSpuScoreRdd).groupByKey()

      userSpuGroupByRDD.mapPartitions{iter=>
        val lb = new ListBuffer[(Long, mutable.HashMap[Int, Int])]
        while(iter.hasNext){
          val tuple = iter.next()
          val idCode = tuple._1

          val preferenceMap = new mutable.HashMap[Int, Int]

          for(map <- tuple._2.toList){
            map.foreach{
              case (k,v) => preferenceMap.+=(k -> (v + preferenceMap.getOrElse(k,0)))
            }
          }
          lb.+=((idCode,preferenceMap))
        }
        lb.iterator
      }.toDF("id_code","spu_score_map").createOrReplaceTempView("tmp_dim_user_spu_score")


      //用户10天内加购商品
      spark.sql(
        s"""
           |select
           |  id_code,
           |  rbm_merge(rbm_spu_id) as rbm_spu_list
           |from
           |(select
           |   id_code,
           |   rbm_init(cast(spu_id as int)) as rbm_spu_id
           |from access_cdm.dwd_trade_shopping_cart_record_dh_i
           |where add_cart_type = 1 and datediff('${batchDate}',dt) between 0 and 10
           |and spu_id is not null) t
           |group by id_code
           |""".stripMargin).createOrReplaceTempView("tmp_collection_add_cart")




      //爆款商品
      spark.sql(
        s"""
          |select
          |    1 as key,
          |    rbm_merge(rbm_spu_id) as rbm_hot_goods_list
          |from
          |(select
          |    rbm_init(cast(spu_id as int)) as rbm_spu_id
          |from access_cdm.dim_item_goods_spu_dd_f
          |where dt = date_sub('${batchDate}',1) and is_deleted = 0 and spu_status = 1
          |and is_hot_goods = 1) t
          |group by 1
          |""".stripMargin)
        .cache()
        .createOrReplaceTempView("tmp_collection_hot_goods")



      spark.sql(
        s"""
          |select
          |    1 as key,
          |    rbm_merge(rbm_spu_id) as rbm_normal_goods_list
          |from
          |(select
          |    rbm_init(cast(spu_id as int)) as rbm_spu_id
          |from access_cdm.dim_item_goods_spu_dd_f
          |where dt = date_sub('${batchDate}',1) and is_deleted = 0 and spu_status = 1
          |and is_hot_goods = 0) t
          |group by 1
          |""".stripMargin)
        .cache()
        .createOrReplaceTempView("tmp_collection_normal_goods")




      //商品维度表
      spark.sql(
        s"""
           |select
           |   spu_id,
           |   is_hot_goods
           |from access_cdm.dim_item_goods_spu_dd_f
           |where dt = date_sub('${batchDate}',1) and is_deleted = 0 and spu_status = 1
           |""".stripMargin)
      .cache()
      .createOrReplaceTempView("tmp_dim_spu")




      //用户历史购买过的商品
      spark.sql(
        s"""
           |select
           |   id_code,
           |   rbm_merge(rbm_spu_id) as rbm_spu_list
           |from
           |(select
           |  id_code,
           |  rbm_init(cast(spu_id as int)) as rbm_spu_id
           |from tmp_order_spu_detail
           |where datediff('${batchDate}',event_date) between 30 and 365) t
           |group by id_code
           |""".stripMargin
      ).createOrReplaceTempView("tmp_collection_history")




      //数据合并关联
      spark.sql(
        s"""
           |select
           |     nvl(a.id_code,b.id_code) as id_code,
           |     a.rbm_spu_list as cate_spu_list,
           |     b.rbm_spu_list as brand_spu_list,
           |     c.rbm_spu_list as add_cart_list,
           |     d.rbm_hot_goods_list as hot_goods_list,
           |     h.rbm_normal_goods_list as normal_goods_list,
           |     e.rbm_spu_list as history_spu_list,
           |     f.rbm_spu_list as recent_1m_spu_list,
           |     i.rbm_spu_list as level_support_spu_list,
           |     j.spu_score_map
           |from tmp_collection_cate a
           |full outer join tmp_collection_brand b on a.id_code = b.id_code
           |left join tmp_collection_add_cart c on nvl(a.id_code,b.id_code) = c.id_code
           |join tmp_collection_hot_goods d on true
           |left join tmp_collection_history e on nvl(a.id_code,b.id_code) = e.id_code
           |left join tmp_delete_1 f on nvl(a.id_code,b.id_code) = f.id_code
           |left join bigdata_ods.ods_dealer_center_w_user_brand_provider_dh_f g on g.dt = '${batchDate}' and nvl(a.id_code,b.id_code) = g.id_code
           |left join tmp_collection_normal_goods h on true
           |left join tmp_level_support_spu i on cast(g.brand_provider_level as string) = i.level
           |left join tmp_dim_user_spu_score j on nvl(a.id_code,b.id_code) = j.id_code
           |""".stripMargin).createOrReplaceTempView("tmp_gether")


      spark.read.table("tmp_gether")
        .rdd
        .mapPartitions{iter=>
          val lb = new ListBuffer[(Long, Int, Int, String, Int)]
          while(iter.hasNext){
            val row = iter.next()

            val idCode = row.getAs[Long]("id_code")
            val cateSpuRBM: RoaringBitmap = convertByteArrayToRBM(row.getAs[Array[Byte]]("cate_spu_list"))
            val brandSpuRBM = convertByteArrayToRBM(row.getAs[Array[Byte]]("brand_spu_list"))
            val addCartRBM = convertByteArrayToRBM(row.getAs[Array[Byte]]("add_cart_list"))
            val hotGoodsRBM = convertByteArrayToRBM(row.getAs[Array[Byte]]("hot_goods_list"))
            val normalGoodsRBM = convertByteArrayToRBM(row.getAs[Array[Byte]]("normal_goods_list"))
            val historySpuRBM = convertByteArrayToRBM(row.getAs[Array[Byte]]("history_spu_list"))
            val recent1mSpuRBM = convertByteArrayToRBM(row.getAs[Array[Byte]]("recent_1m_spu_list"))
            val levelSupportSpuRBM = convertByteArrayToRBM(row.getAs[Array[Byte]]("level_support_spu_list"))

            val scoreMap = row.getAs[Map[Int, Int]]("spu_score_map")


            //P0 = 品牌偏好 and  需求偏好 and  且加购的商品 = 品牌偏好 ∩ 需求偏好 ∩ 加购
            val p0RBM = RoaringBitmap.and(RoaringBitmap.andNot(RoaringBitmap.and(RoaringBitmap.and(brandSpuRBM, cateSpuRBM), addCartRBM),recent1mSpuRBM),levelSupportSpuRBM)

            //P1 = 品牌偏好  and  需求偏好 and not in (加购) = (品牌偏好 ∩ 需求偏好) not in (加购)
            val p1RBM = RoaringBitmap.and(RoaringBitmap.andNot(RoaringBitmap.andNot(RoaringBitmap.and(brandSpuRBM,cateSpuRBM),addCartRBM),recent1mSpuRBM),levelSupportSpuRBM)

            //P2 = 需求偏好  and  爆品 and not in (品牌偏好) = (需求偏好 ∩ 爆品) not in (品牌偏好)
            val p2RBM = RoaringBitmap.and(RoaringBitmap.andNot(RoaringBitmap.andNot(RoaringBitmap.and(cateSpuRBM, hotGoodsRBM), brandSpuRBM),recent1mSpuRBM),levelSupportSpuRBM)

            //P3 = 品牌偏好 and 爆品 and not in (需求偏好)   = (品牌偏好 ∩ 爆品) not in (需求偏好)
            val p3RBM = RoaringBitmap.and(RoaringBitmap.andNot(RoaringBitmap.andNot(RoaringBitmap.and(brandSpuRBM, hotGoodsRBM), cateSpuRBM),recent1mSpuRBM),levelSupportSpuRBM)

            //P4 = 需求偏好 and 普通商品 and not in (品牌偏好) = (需求偏好 ∩ 普通商品) not in (品牌偏好)
            val p4RBM = RoaringBitmap.and(RoaringBitmap.andNot(RoaringBitmap.andNot(RoaringBitmap.and(cateSpuRBM,normalGoodsRBM),brandSpuRBM),recent1mSpuRBM),levelSupportSpuRBM)

            //P5 = 品牌偏好 and 普通品牌 and not in (需求偏好) = (品牌偏好 ∩ 普通品牌) not in (需求偏好)
            val p5RBM = RoaringBitmap.and(RoaringBitmap.andNot(RoaringBitmap.andNot(RoaringBitmap.and(brandSpuRBM,normalGoodsRBM),cateSpuRBM),recent1mSpuRBM),levelSupportSpuRBM)

            //P6 = 历史购买过的商品 and not in (品牌偏好) and not in (需求偏好) = (历史购买过的商品 not in 品牌偏好 ) not in (需求偏好)
            val p6RBM = RoaringBitmap.and(RoaringBitmap.andNot(RoaringBitmap.andNot(RoaringBitmap.andNot(historySpuRBM,brandSpuRBM),cateSpuRBM),recent1mSpuRBM),levelSupportSpuRBM)

            orderUserPreference(idCode, p0RBM, scoreMap, hotGoodsRBM, "P0").foreach(lb.+=(_))
            orderUserPreference(idCode, p1RBM, scoreMap, hotGoodsRBM, "P1").foreach(lb.+=(_))
            orderUserPreference(idCode, p2RBM, scoreMap, hotGoodsRBM, "P2").foreach(lb.+=(_))
            orderUserPreference(idCode, p3RBM, scoreMap, hotGoodsRBM, "P3").foreach(lb.+=(_))
            orderUserPreference(idCode, p4RBM, scoreMap, hotGoodsRBM, "P4").foreach(lb.+=(_))
            orderUserPreference(idCode, p5RBM, scoreMap, hotGoodsRBM, "P5").foreach(lb.+=(_))
            orderUserPreference(idCode, p6RBM, scoreMap, hotGoodsRBM, "P6").foreach(lb.+=(_))
          }

          lb.iterator
        }.toDF("id_code","spu_id","rn","priority","score")
        .createOrReplaceTempView("tmp_res")





      spark.sql(
        s"""
          |insert overwrite table access_ads.ads_bm_user_preference_spu_dh_f partition (dt = '${batchDate}',priority)
          |select
          |a.id_code,
          |a.id_code - 200000 as user_id,
          |a.spu_id,
          |b.spu_name,
          |a.rn as rank_no,
          |a.score,
          |a.priority
          |from tmp_res a
          |left join access_cdm.dim_item_goods_spu_dh_f b on b.dt = '${batchDate}' and a.spu_id = b.spu_id
          |""".stripMargin)

    }
  }

  case class Preference(
                       spuID:Int,
                       score:Int
                       )

  def convertByteArrayToRBM(byteArray:Array[Byte]): RoaringBitmap ={
    val rbm = new RoaringBitmap()

    if(null != byteArray){
      rbm.deserialize(new DataInputStream(new ByteArrayInputStream(byteArray)))
    }
    rbm
  }


  def orderUserPreference(idCode:Long,rbm:RoaringBitmap,scoreMap:Map[Int,Int],hotGoodsRBM:RoaringBitmap,priotiry:String): immutable.Seq[(Long, Int, Int, String, Int)] ={

    val dataSet1: Array[(Int, Int, Int)] = RoaringBitmap.and(rbm, hotGoodsRBM).toArray.map { spu =>
       (spu, scoreMap.getOrElse(spu, 0), 1)
    }

    val dataset2: Array[(Int, Int, Int)] = RoaringBitmap.andNot(rbm, hotGoodsRBM).toArray.map { spu =>
       (spu, scoreMap.getOrElse(spu, 0), 0)
    }

    val array = Array.concat(dataSet1, dataset2)

    val sortedArray = array.sortBy { tuple => new UDSortMethod(tuple._2, tuple._3) }

    //id_Code,spu,rank_no,priority,score
    for(i <- sortedArray.indices) yield {
      val t = sortedArray.apply(i)
      (idCode, t._1, i + 1, priotiry,t._2)
    }

  }

  //自定义排序
  class UDSortMethod(val score:Int,val flag:Int) extends Ordered[UDSortMethod] with Serializable{
    override def compare(that: UDSortMethod): Int = {
      if(this.score == that.score){
        that.flag - this.flag
      }else{
        that.score - this.score
      }
    }
  }
}

