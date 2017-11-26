package net.wrap_trap.calcite_arrow_sample

import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.calcite.DataContext
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelProtoDataType}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.ScannableTable

/**
  * Created by masayuki on 2017/11/18.
  */
class ArrowScannableTable(val vectorSchemaRoots: Array[VectorSchemaRoot], val tProtoRowType: RelProtoDataType)
  extends AbstractTable with ScannableTable {

  override def toString(): String = {
    "ArrowScannableTable"
  }

  def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    if (this.tProtoRowType != null) {
      return this.tProtoRowType.apply(typeFactory)
    }
    ArrowEnumerator.deduceRowType(this.vectorSchemaRoots(0), typeFactory.asInstanceOf[JavaTypeFactory])
  }

  override def scan(root: DataContext): Enumerable[Array[Object]] = {
    new AbstractEnumerable[Array[Object]] {
      override def enumerator(): Enumerator[Array[Object]] = {
        new ArrowEnumerator(vectorSchemaRoots)
      }
    }
  }
}
