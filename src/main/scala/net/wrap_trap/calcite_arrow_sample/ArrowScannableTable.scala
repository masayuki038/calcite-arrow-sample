package net.wrap_trap.calcite_arrow_sample

import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.{AbstractEnumerable, Enumerable, Enumerator}
import org.apache.calcite.rel.`type`.RelProtoDataType
import org.apache.calcite.schema.ScannableTable

/**
  * Created by masayuki on 2017/11/18.
  */
class ArrowScannableTable(val schemaRoots: Array[VectorSchemaRoot], val rowType: RelProtoDataType)
  extends ArrowTable(schemaRoots: Array[VectorSchemaRoot], rowType: RelProtoDataType) with ScannableTable {

  override def toString(): String = {
    "ArrowScannableTable"
  }

  override def scan(root: DataContext): Enumerable[Array[Object]] = {
    new AbstractEnumerable[Array[Object]] {
      override def enumerator(): Enumerator[Array[Object]] = {
        new ArrowEnumerator(schemaRoots)
      }
    }
  }
}
