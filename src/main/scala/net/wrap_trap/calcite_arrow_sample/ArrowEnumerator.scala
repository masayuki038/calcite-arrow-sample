package net.wrap_trap.calcite_arrow_sample

import org.apache.calcite.linq4j.Enumerator
import org.slf4j.LoggerFactory

import collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.util.Pair

/**
  * Created by masayuki on 2017/11/18.
  */
object ArrowEnumerator {
  def deduceRowType(vectorSchemaRoot: VectorSchemaRoot, typeFactory: JavaTypeFactory): RelDataType = {
    val arrowTypes = vectorSchemaRoot.getFieldVectors.asScala.map(fieldVector => {
      val relDataType = ArrowFieldType.toType(ArrowFieldType.of(fieldVector.getField.getType).get, typeFactory)
      new Pair(fieldVector.getField.getName, relDataType)
    })
    typeFactory.createStructType(arrowTypes.asJava)
  }
}

class ArrowEnumerator(vectorSchemaRoots: Array[VectorSchemaRoot]) extends Enumerator[Array[Object]] {
  val logger = LoggerFactory.getLogger(classOf[ArrowEnumerator])

  var index = 0
  var currentPos = 0

//  logger.error("vectorSchemaRoots.length:" + vectorSchemaRoots.length)
//  vectorSchemaRoots.foreach(v => {
//    logger.error("vectorSchemaRoot: " + v)
//  })

  override def close(): Unit = {}

  override def moveNext(): Boolean = {
    if (this.currentPos < (this.vectorSchemaRoots(this.index).getRowCount - 1)) {
      this.currentPos += 1
      return true
    } else if (this.index < (this.vectorSchemaRoots.length - 1)) {
      this.index += 1
      this.currentPos = 0
      return true
    }
    false
  }

  override def current(): Array[Object] = {
    vectorSchemaRoots(this.index).getFieldVectors.asScala.map { fieldVector =>
      if (fieldVector.getAccessor.getValueCount <= currentPos) {
        "NULL"
      } else {
        fieldVector.getAccessor.getObject(currentPos)
      }
    }.toArray
  }

  override def reset(): Unit = { this.currentPos = 0 }
}
