package net.wrap_trap.calcite_arrow_sample

import org.apache.calcite.adapter.enumerable.EnumerableRel.{Prefer, Result}
import org.apache.calcite.adapter.enumerable.{PhysTypeImpl, EnumerableRelImplementor, EnumerableRel, EnumerableConvention}
import org.apache.calcite.linq4j.tree.{Expressions, Blocks, Primitive}
import org.apache.calcite.plan.{RelOptPlanner, RelTraitSet, RelOptTable, RelOptCluster}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelWriter, RelNode}
import org.apache.calcite.rel.core.TableScan

/**
  * Created by masayuki on 2017/11/30.
  */
class ArrowTableScan(val cluster: RelOptCluster,
val myTable: RelOptTable,
val arrowTranslatableTable: ArrowTranslatableTable,
val fields: Array[Int])
extends TableScan(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), myTable) with EnumerableRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new ArrowTableScan(getCluster(), myTable, arrowTranslatableTable, fields)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("fields", Primitive.asList(fields))
  }

  override def deriveRowType(): RelDataType = {
    val fieldList = myTable.getRowType.getFieldList
    val builder = getCluster.getTypeFactory.builder
    fields.foreach(field => builder.add(fieldList.get(field)))
    builder.build
  }

  override def register(planner: RelOptPlanner): Unit = {
    planner.addRule(ArrowProjectTableScanRule.INSTANCE)
  }

  def implement(implementor: EnumerableRelImplementor, pref: Prefer): Result ={
    val physType = PhysTypeImpl.of(implementor.getTypeFactory, getRowType, pref.preferArray)
    implementor.result(physType, Blocks.toBlock(
      Expressions.call(
        table.getExpression(classOf[ArrowTranslatableTable]),
        "project",
        implementor.getRootExpression,
        Expressions.constant(fields))
    ))
  }
}
