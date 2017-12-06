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
object EnumeratorUtils {
  def identityList(n: Int): Array[Int] = {
    (0 to n-1).toArray
  }
}

object ArrowEnumerator {
  def deduceRowType(vectorSchemaRoot: VectorSchemaRoot, typeFactory: JavaTypeFactory): RelDataType = {
    val arrowTypes = vectorSchemaRoot.getFieldVectors.asScala.map(fieldVector => {
      val relDataType = ArrowFieldType.toType(ArrowFieldType.of(fieldVector.getField.getType).get, typeFactory)
      new Pair(fieldVector.getField.getName, relDataType)
    })
    typeFactory.createStructType(arrowTypes.asJava)
  }
}

class ArrowEnumerator(vectorSchemaRoots: Array[VectorSchemaRoot], fields: Array[Int]) extends Enumerator[Object] {
  val logger = LoggerFactory.getLogger(classOf[ArrowEnumerator])

  var index = 0
  var currentPos = 0

  def this(vectorSchemaRoots: Array[VectorSchemaRoot]) = {
    this(vectorSchemaRoots, EnumeratorUtils.identityList(vectorSchemaRoots(0).getFieldVectors.size))
  }

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

  override def current(): Object = {
    if (fields.length == 1) {
      return getObject(fields(0))
    }

    fields.map { fieldIndex =>
      getObject(fieldIndex)
    }
  }

  def getObject(fieldIndex: Int): Object = {
    val fieldVector = vectorSchemaRoots(this.index).getFieldVectors.get(fieldIndex)
    if (fieldVector.getAccessor.getValueCount <= this.currentPos) {
      "NULL"
    } else {
      fieldVector.getAccessor.getObject(this.currentPos)
    }
  }

  override def reset(): Unit = { this.currentPos = 0 }
}


class ArrowArrayEnumerator(vectorSchemaRoots: Array[VectorSchemaRoot], fields: Array[Int])
  extends Enumerator[Array[Object]] {
  val logger = LoggerFactory.getLogger(classOf[ArrowEnumerator])

  var index = 0
  var currentPos = 0

  def this(vectorSchemaRoots: Array[VectorSchemaRoot]) = {
    this(vectorSchemaRoots, EnumeratorUtils.identityList(vectorSchemaRoots(0).getFieldVectors.size))
  }

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
    fields.map { fieldIndex =>
      val fieldVector = vectorSchemaRoots(this.index).getFieldVectors.get(fieldIndex)
      if (fieldVector.getAccessor.getValueCount <= this.currentPos) {
        "NULL"
      } else {
        fieldVector.getAccessor.getObject(this.currentPos)
      }
    }
  }

  override def reset(): Unit = { this.currentPos = 0 }
}
