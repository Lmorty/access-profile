package com.access.core

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

/**
 * groupingID反解析器
 */
object GetColumnFromGroupingID {


  /**
   * 根据groupingID获取字段的情况
   * @param dimIdsBesidesUser
   * @param columns
   * @param groupingID
   * @return
   */
  def getColumnsFromID(dimIdsBesidesUser:String,columns:String,groupingID:Int): immutable.Seq[String] ={


    //grouping id生成规则
    //spark sql处理方式
    //将 group by 的所有字段 顺序 排列,选中为0 未选中为1,形成一个二进制数字转十进制就是grouping_id

    //hive处理方式
    //将 group by 的所有字段 倒序 排列。
    //对于每个字段，如果该字段出现在了当前粒度中，则该字段位置赋值为1，否则为0。
    //这样就形成了一个二进制数，这个二进制数转为十进制，即为当前粒度对应的 grouping__id。


    val dimSize = if (StringUtils.isBlank(dimIdsBesidesUser)) columns.split(",").length else s"$dimIdsBesidesUser,$columns".split(",").length

    val binaryArray = convertIntToBinaryWithSize(groupingID, dimSize)

    binaryArray.foreach(print)

    val indexs = for (i <- binaryArray.indices if binaryArray.apply(i).equals("0")) yield i

    val selectedColumns = if(StringUtils.isBlank(dimIdsBesidesUser)) {
      val columnsArray = columns.split(",")
      for( i <- indexs) yield columnsArray.apply(i)
    }else{
      val columnsArray = s"$dimIdsBesidesUser,$columns".split(",")
      for( i <- indexs) yield columnsArray.apply(i)
    }


    selectedColumns
  }

  def generateResultFormat(row:Row,dimIdsBesidesUser:String,columns:String): (immutable.Seq[String], String) ={

       //获取groupingID
    val groupingID = row.getAs[Int]("GROUPING__ID")


    val columnSeq = getColumnsFromID(dimIdsBesidesUser, columns, groupingID)


    val values = for (col <- columnSeq) yield row.getAs[String](col)

    val labelID = columnSeq.last

    //判断一下是否有null值
    if(values.contains(null)){
      (null,labelID)
    }else{
      (values,labelID)
    }

  }


  /**
   * 按照长度和十进制数值格式化为对应长度的二进制数值
   * @param groupingID
   * @param length
   * @return
   */
  def convertIntToBinaryWithSize(groupingID:Int,length:Int) ={
    val originBinaryString = Integer.toBinaryString(groupingID)

    val originBinaryArray = originBinaryString.split("|")

    val formatBinaryArrayBuffer = new ArrayBuffer[String]()

    if("0".equals(originBinaryString)){
      for(i <- 0 until length){
        formatBinaryArrayBuffer += "0"
      }
    }else{
      for(i <- 0 until length){
        val reverseOrigin = originBinaryArray.reverse
        if(i <= reverseOrigin.length - 1){
          formatBinaryArrayBuffer += reverseOrigin.apply(i)
        }else{
          formatBinaryArrayBuffer += "0"
        }
      }
    }

    formatBinaryArrayBuffer.reverse.take(length).toArray
  }
}
