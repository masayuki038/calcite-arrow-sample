package net.wrap_trap.calcite_arrow_sample

import java.lang.reflect.Type

import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j._
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelProtoDataType
import org.apache.calcite.schema.{Schemas, SchemaPlus, TranslatableTable, QueryableTable}

/**
  * Created by masayuki on 2017/12/02.
  */
class ArrowTranslatableTable(val schemaRoots: Array[VectorSchemaRoot], val rowType: RelProtoDataType)
  extends ArrowTable(schemaRoots: Array[VectorSchemaRoot], rowType: RelProtoDataType)
    with QueryableTable with TranslatableTable{

  override def toString(): String = {
    "ArrowTranslatableTable"
  }

  def project(root: DataContext, fields: Array[Int]): Enumerable[Object] = {
    new AbstractEnumerable[Object] {
      override def enumerator(): Enumerator[Object] = {
        new ArrowEnumerator(schemaRoots, fields)
      }
    }
  }

  override def getExpression(schema: SchemaPlus, tableName: String, clazz: Class[_]): Expression = {
    Schemas.tableExpression(schema, getElementType, tableName, clazz)
  }

  def getElementType(): Type = classOf[Array[Any]]

  def asQueryable[T](queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable[T] = {
    throw new UnsupportedOperationException()
  }

  def toRel(context: RelOptTable.ToRelContext, relOptTable: RelOptTable): RelNode = {
    val fieldCount = relOptTable.getRowType.getFieldCount
    val fields = EnumeratorUtils.identityList(fieldCount)
    new ArrowTableScan(context.getCluster, relOptTable, this, fields)
  }
}
