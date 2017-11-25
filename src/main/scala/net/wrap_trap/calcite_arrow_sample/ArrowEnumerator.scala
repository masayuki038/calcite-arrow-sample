package net.wrap_trap.calcite_arrow_sample

import org.apache.calcite.linq4j.Enumerator
import org.slf4j.LoggerFactory

import collection.JavaConversions._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.util.Pair

/**
  * Created by masayuki on 2017/11/18.
  */
object ArrowEnumerator {
  def deduceRowType(vectorSchemaRoot: VectorSchemaRoot, typeFactory: JavaTypeFactory): RelDataType = {
    val arrowTypes = vectorSchemaRoot.getFieldVectors.map(fieldVector => {
      val relDataType = ArrowFieldType.toType(ArrowFieldType.of(fieldVector.getField.getType).get, typeFactory)
      new Pair(fieldVector.getField.getName, relDataType)
    })
    typeFactory.createStructType(arrowTypes)
  }
}

class ArrowEnumerator(vectorSchemaRoot: VectorSchemaRoot) extends Enumerator[Array[Object]] {
  val logger = LoggerFactory.getLogger(classOf[ArrowEnumerator])

  val count = vectorSchemaRoot.getRowCount
  var currentPos = 0

  override def close(): Unit = {}

  override def moveNext(): Boolean = {
    if (this.currentPos < (this.count - 1)) {
      this.currentPos += 1
      return true
    }
    false
  }

  override def current(): Array[Object] = {
    logger.error("count: " + count)
    vectorSchemaRoot.getFieldVectors.map { fieldVector =>
      logger.error("FieldVector: " + fieldVector.getField.getName)
      logger.error("FieldVector.count: " + fieldVector.getAccessor.getValueCount)
      fieldVector.getAccessor.getObject(currentPos)
    }.toArray
  }

  override def reset(): Unit = { this.currentPos = 0 }
}
