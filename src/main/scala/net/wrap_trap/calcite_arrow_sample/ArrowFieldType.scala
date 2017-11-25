package net.wrap_trap.calcite_arrow_sample

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.linq4j.tree.Primitive
import org.apache.arrow.vector.types.pojo.ArrowType

/**
  * Created by masayuki on 2017/11/18.
  */
sealed abstract class ArrowFieldType()
case object ArrowFieldType {
  val map = List(
    (classOf[ArrowType.Utf8], STRING),
    (classOf[ArrowType.Bool], BOOLEAN),
    (classOf[ArrowType.Int], INT),
    (classOf[ArrowType.FloatingPoint], FLOAT),
    (classOf[ArrowType.Date], DATE),
    (classOf[ArrowType.Time], TIME),
    (classOf[ArrowType.Timestamp], TIMESTAMP))
    .toMap[Class[_ <: ArrowType], ArrowFieldType]

  def toType(arrowFieldType:  ArrowFieldType, typeFactory: JavaTypeFactory): RelDataType = {
    val clazz = arrowFieldType match {
      case STRING => classOf[String]
      case BOOLEAN => Primitive.BOOLEAN.boxClass
      case INT => Primitive.INT.boxClass
      case FLOAT => Primitive.FLOAT.boxClass
      case DATE => classOf[java.sql.Date]
      case TIME => classOf[java.sql.Time]
      case TIMESTAMP => classOf[java.sql.Timestamp]
    }
    typeFactory.createJavaType(clazz)
  }

  def of(arrowType: ArrowType): Option[ArrowFieldType] = {
    map.get(arrowType.getClass)
  }
}
case object STRING extends ArrowFieldType
case object BOOLEAN extends ArrowFieldType
case object INT extends ArrowFieldType
case object FLOAT extends ArrowFieldType
case object DATE extends ArrowFieldType
case object TIME extends ArrowFieldType
case object TIMESTAMP extends ArrowFieldType
