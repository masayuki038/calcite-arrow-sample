package net.wrap_trap.calcite_arrow_sample

import org.apache.arrow.memory.RootAllocator
import org.scalatest.{Matchers, FlatSpec}

class ArrowSchemaTest extends FlatSpec with Matchers {
  "load" should "load arrow file to FieldVector" in {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowSchema = new ArrowSchema(null)
    val vectorSchemaRoots = arrowSchema.load(
      "D:\\development\\repository\\git\\calcite-arrow-sample\\target\\scala-2.12\\classes\\samples\\nationsSF.arrow"
      , allocator)
    vectorSchemaRoots.length should equal(1)
    val nationKeyField = vectorSchemaRoots(0).getFieldVectors.get(0)
    nationKeyField.getField.getName.toUpperCase should equal("N_NATIONKEY")
    nationKeyField.getAccessor.getValueCount should equal(25)
    val nameField = vectorSchemaRoots(0).getFieldVectors.get(1)
    nameField.getField.getName.toUpperCase should equal("N_NAME")
    nameField.getAccessor.getValueCount should equal(25)
    val regionKeyField = vectorSchemaRoots(0).getFieldVectors.get(2)
    regionKeyField.getField.getName.toUpperCase should equal("N_REGIONKEY")
    regionKeyField.getAccessor.getValueCount should equal(25)
    val commentField = vectorSchemaRoots(0).getFieldVectors.get(3)
    commentField.getField.getName.toUpperCase should equal("N_COMMENT")
    commentField.getAccessor.getValueCount should equal(25)
  }
}
