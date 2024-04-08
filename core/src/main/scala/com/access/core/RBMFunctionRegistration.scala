package com.access.core

import org.apache.spark.sql.EncapsulationViolator.createAnalysisException
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, ExpressionInfo, RuntimeReplaceable, UnaryExpression}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, SparkSession}
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait NullableSketchAggregation
  extends TypedImperativeAggregate[Option[RoaringBitmap]] {

  override def createAggregationBuffer(): Option[RoaringBitmap] = None

  override def merge(buffer: Option[RoaringBitmap],
                     other: Option[RoaringBitmap]): Option[RoaringBitmap] =
    (buffer, other) match {
      case (Some(a), Some(b)) =>
        a.or(b)
        Some(a)
      case (a, None) => a
      case (None, b) => b
      case _         => None
    }

  override def eval(buffer: Option[RoaringBitmap]): Any = {
    buffer
      .map(rbm => {
        val bos = new ByteArrayOutputStream
        rbm.serialize(new DataOutputStream(bos))
        bos.toByteArray
      })
      .orNull
  }

  def child: Expression

  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = child.nullable

  override def serialize(buffer: Option[RoaringBitmap]): Array[Byte] = {
    buffer
      .map(rbm => {
        val bos = new ByteArrayOutputStream
        rbm.serialize(new DataOutputStream(bos))
        bos.toByteArray
      })
      .orNull
  }

  override def deserialize(bytes: Array[Byte]): Option[RoaringBitmap] = {
    if (bytes == null) {
      return None
    }

    val rbm = new RoaringBitmap
    rbm.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)))
    Option(rbm)
  }
}

/**
 * This function creates a composable "sketch" for each input row.
 * All expression values are treated as simple values.
 *
 * @param child to estimate the cardinality of.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the composable "sketch" by RoaringBitMap.
  """)
case class RoaringBitMapInit(override val child: Expression)
  extends UnaryExpression
    with CodegenFallback {

  override def dataType: DataType = BinaryType

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case IntegerType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"$prettyName only supports integer input")
    }
  }

  override def nullable: Boolean = child.nullable

  override def nullSafeEval(value: Any): Any = {
    val rbm = new RoaringBitmap
    rbm.add(value.asInstanceOf[Integer])
    val bos = new ByteArrayOutputStream
    rbm.serialize(new DataOutputStream(bos))
    bos.toByteArray
  }

  override def prettyName: String = "rbm_init"
}

/**
 * This version aggregates the "sketches" into a single merged "sketch" that represents the union of the constituents.
 *
 * @param child "sketch" to merge
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the merged RoaringBitMap sketch.
  """)
case class RoaringBitMapMerge(child: Expression,
                              override val mutableAggBufferOffset: Int = 0,
                              override val inputAggBufferOffset: Int = 0)
  extends NullableSketchAggregation {

  def this(child: Expression) = this(child, 0, 0)

  override def update(buffer: Option[RoaringBitmap],
                      inputRow: InternalRow): Option[RoaringBitmap] = {
    val value = child.eval(inputRow)
    if (value != null) {
      val rbm1 = value match {
        case b: Array[Byte] =>
          val rbm = new RoaringBitmap
          rbm.deserialize(new DataInputStream(new ByteArrayInputStream(b)))
          rbm
        case _ =>
          throw new IllegalStateException(
            s"$prettyName only supports Array[Byte]")
      }
      buffer
        .map(buf => {
          buf.or(rbm1)
          buf
        })
        .orElse(Option(rbm1))
    } else {
      buffer
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case BinaryType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          s"$prettyName only supports binary input")
    }
  }

  override def dataType: DataType = BinaryType

  def withNewMutableAggBufferOffset(newOffset: Int): RoaringBitMapMerge =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): RoaringBitMapMerge =
    copy(inputAggBufferOffset = newOffset)

  override def prettyName: String = "rbm_merge"
}

/**
 * Returns the estimated cardinality of an RoaringBitMap "sketch"
 *
 * @param child RoaringBitMap "sketch"
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(sketch) - Returns the estimated cardinality of the binary representation produced by RoaringBitMap.
  """)
case class RoaringBitMapCardinality(override val child: Expression)
  extends UnaryExpression
    with ExpectsInputTypes
    with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Long = {
    val data = input.asInstanceOf[Array[Byte]]
    val rbm = new RoaringBitmap
    rbm.deserialize(new DataInputStream(new ByteArrayInputStream(data)))
    rbm.getLongCardinality
  }

  override def prettyName: String = "rbm_cardinality"
}

trait RBMFunctions {
  def withExpr(expr: Expression): Column = new Column(expr)

  def withAggregateFunction(func: AggregateFunction,
                            isDistinct: Boolean = false): Column = {
    new Column(func.toAggregateExpression(isDistinct))
  }

  def rbm_init(e: Column): Column = withExpr {
    RoaringBitMapInit(e.expr)
  }

  def rbm_init(columnName: String): Column =
    rbm_init(col(columnName))

  def rbm_merge(e: Column): Column = withAggregateFunction {
    RoaringBitMapMerge(e.expr)
  }

  def rbm_merge(columnName: String): Column =
    rbm_merge(col(columnName))

  def rbm_cardinality(e: Column): Column = withExpr {
    RoaringBitMapCardinality(e.expr)
  }

  def rbm_cardinality(columnName: String): Column =
    rbm_cardinality(col(columnName))
}

// based on Spark's FunctionRegistry
object RBMFunctionRegistration {

  type FunctionBuilder = Seq[Expression] => Expression

  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    expression[RoaringBitMapInit]("rbm_init"),
    expression[RoaringBitMapMerge]("rbm_merge"),
    expression[RoaringBitMapCardinality]("rbm_cardinality")
  )

  def registerFunctions(fr: FunctionRegistry): Unit = {
    expressions.foreach {
      case (name, (info, builder)) =>
        fr.registerFunction(FunctionIdentifier(name), info, builder)
    }
  }

  def registerFunctions(spark: SparkSession): Unit = {
    registerFunctions(spark.sessionState.functionRegistry)
  }

  /** See usage above. */
  protected def expression[T <: Expression](name: String)(
    implicit tag: ClassTag[T])
  : (String, (ExpressionInfo, FunctionBuilder)) = {

    // For `RuntimeReplaceable`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `child` and should not be used as function builder.
    val constructors =
    if (classOf[RuntimeReplaceable].isAssignableFrom(tag.runtimeClass)) {
      val all = tag.runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      tag.runtimeClass.getConstructors
    }
    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor =
      constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        Try(varargCtor.get.newInstance(expressions).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw createAnalysisException(e.getCause.getMessage)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f =
          constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
            throw createAnalysisException(
              s"Invalid number of arguments for function $name")
          }
        Try(f.newInstance(expressions: _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw createAnalysisException(e.getCause.getMessage)
        }
      }
    }

    (name, (expressionInfo[T](name), builder))
  }

  /**
   * Creates an [[ExpressionInfo]] for the function as defined by expression T using the given name.
   */
  protected def expressionInfo[T <: Expression: ClassTag](
                                                           name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      new ExpressionInfo(clazz.getCanonicalName,
        null,
        name,
        df.usage(),
        df.extended())
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }
}

