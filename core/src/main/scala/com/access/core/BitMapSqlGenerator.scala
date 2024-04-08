package com.access.core

import org.apache.commons.lang3.StringUtils

/**
 * @author huangxianglei
 * sql生成器
 */
object BitMapSqlGenerator {

  def generate(table: String, dimIdsBesidesUser: String, columns: String, batchDate: String): String = {

    val convertToRBMIdClause = generateConvertToRBMIdClause(table, dimIdsBesidesUser, columns, batchDate)

    val aggToRBMClause = generateAggToRBMClause(dimIdsBesidesUser, columns)

    val resultClause = generateResultClause(dimIdsBesidesUser, columns)

    s"""
       |with
       |tmp_conver_into_rbm_id as (
       |$convertToRBMIdClause
       |),
       |tmp_convert_into_rbm as (
       |$aggToRBMClause
       |)
       |$resultClause
       |""".stripMargin

  }


  private def generateConvertToRBMIdClause(table:String,dimIdsBesidesUser:String,columns:String,batchDate:String): String ={

    val dimClause = if(StringUtils.isBlank(dimIdsBesidesUser)) "" else s"${dimIdsBesidesUser.split(",").map(col => s"cast($col as string) as $col").mkString(",")},"
    val columnClause = columns.split(",").map(col => s"cast($col as string) as $col").mkString(",")

    s"""
       |select
       |$dimClause
       |$columnClause,
       |rbm_init(cast(id_code as int)) as rbm_id
       |from $table
       |where dt = '$batchDate'
       |""".stripMargin
  }

  private def generateAggToRBMClause(dimIdsBesidesUser:String,columns:String): String ={

    val dimIdsBesidesUserClause = if(StringUtils.isBlank(dimIdsBesidesUser)) "" else s"${dimIdsBesidesUser},"

    val selectClause =
      s"""
         |select
         |    $dimIdsBesidesUserClause
         |    $columns,
         |    rbm_merge(rbm_id) as rbm_arr,
         |    GROUPING__ID
         |from tmp_conver_into_rbm_id
         |""".stripMargin

    val groupingSetClause = if(StringUtils.isBlank(dimIdsBesidesUser)){
      columns
    }else{
      columns.split(",").map{col => s"($dimIdsBesidesUser,$col)" }.mkString(",")
    }


    val groupByClause = if(StringUtils.isBlank(dimIdsBesidesUser)) {
      s" group by $columns grouping sets ( $groupingSetClause ) "
    }else{
      s" group by $dimIdsBesidesUser,$columns grouping sets ( $groupingSetClause ) "
    }

    s"$selectClause $groupByClause"
  }

  private def generateResultClause(dimIdsBesidesUser:String,columns:String): String ={

    val dimClause = if(StringUtils.isBlank(dimIdsBesidesUser)) "" else s"$dimIdsBesidesUser,"

    s"""
       |select
       |  GROUPING__ID,
       |  $dimClause
       |  $columns,
       |  rbm_arr
       |from tmp_convert_into_rbm
       |""".stripMargin
  }

}



