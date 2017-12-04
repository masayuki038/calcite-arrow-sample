package net.wrap_trap.calcite_arrow_sample

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.`type`.{RelProtoDataType, RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable

/**
  * Created by masayuki on 2017/12/02.
  */
class ArrowTable(val vectorSchemaRoots: Array[VectorSchemaRoot], val tProtoRowType: RelProtoDataType) extends AbstractTable {

  def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    if (this.tProtoRowType != null) {
      return this.tProtoRowType.apply(typeFactory)
    }
    ArrowEnumerator.deduceRowType(this.vectorSchemaRoots(0), typeFactory.asInstanceOf[JavaTypeFactory])
  }
}
