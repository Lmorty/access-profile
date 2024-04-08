package com.access.core

import org.roaringbitmap.RoaringBitmap

import java.nio.ByteBuffer
import java.nio.ByteOrder.LITTLE_ENDIAN
import java.util.Base64
import scala.util.control.Breaks.{break, breakable}

object RBMSerializeToClickhouse {

  /**
   * RoaringBitmap序列化为clickhouse的序列化格式
   * @param rb
   * @return
   */
  def serialize(rb: RoaringBitmap): ByteBuffer = {
    if (rb.getCardinality <= 32) {
      val bos1 = ByteBuffer.allocate(2 + 4*rb.getCardinality)
      val bos = if (bos1.order eq LITTLE_ENDIAN) bos1 else bos1.slice.order(LITTLE_ENDIAN)
      bos.put(new Integer(0).toByte)
      bos.put(rb.getCardinality.toByte)
      rb.toArray.foreach(i => bos.putInt(i))
      bos
    } else {
      val varIntLen = VarInt.varIntSize(rb.serializedSizeInBytes())
      val bos1 = ByteBuffer.allocate(1 + varIntLen + rb.serializedSizeInBytes()) // 1表示标识位，即是否小于32个值；varIntLen表示后面数据的字节长度；
      val bos = if (bos1.order eq LITTLE_ENDIAN) bos1 else bos1.slice.order(LITTLE_ENDIAN)
      bos.put(new Integer(1).toByte)
      VarInt.putVarInt(rb.serializedSizeInBytes(), bos)
      rb.serialize(bos)
      bos
    }
  }


  def deserialization(ckBitmapBase64String: String, ckBitmapCardinality: Int): RoaringBitmap = {
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

  def deserialization(ckBitmapBytes: Array[Byte], ckBitmapCardinality: Int): RoaringBitmap = {
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
}
