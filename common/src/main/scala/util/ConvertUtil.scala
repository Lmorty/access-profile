package util

/**
 * @author huangxianglei
 */
object ConvertUtil {

  /**
   * 转为null或者字符串
   * @param value
   * @return
   */
  def convertNullValue(value:Any): String ={
    if(null == value) null else value.toString
  }

}