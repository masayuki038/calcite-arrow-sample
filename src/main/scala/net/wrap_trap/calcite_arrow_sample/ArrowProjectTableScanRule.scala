package net.wrap_trap.calcite_arrow_sample

import collection.JavaConverters._

import org.apache.calcite.plan.{RelOptRuleCall, RelOptRule}
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.tools.RelBuilderFactory

/**
  * Created by masayuki on 2017/11/29.
  */
object ArrowProjectTableScanRule {
  val INSTANCE = new ArrowProjectTableScanRule(RelFactories.LOGICAL_BUILDER)
}

class ArrowProjectTableScanRule(relBuilderFactory: RelBuilderFactory)
  extends RelOptRule(
    RelOptRule.operand(
      classOf[LogicalProject],
      RelOptRule.operand(classOf[ArrowTableScan], RelOptRule.none())
    ), "ArrowProjectTableScanRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project = call.rel[LogicalProject](0)
    val scan = call.rel[ArrowTableScan](1)
    val fields = getProjectFields(project.getProjects)
    if (fields == null) {
      return
    }

    call.transformTo(
      new ArrowTableScan(
        scan.getCluster,
        scan.getTable,
        scan.arrowTranslatableTable,
        fields
      )
    )
  }

  def getProjectFields(exps: java.util.List[RexNode]): Array[Int] = {
    val fields = new Array[Int](exps.size)
    exps.asScala.zipWithIndex.foreach{ case (exp: RexNode, i: Int) => {
      exp match {
        case e: RexInputRef => fields(i) = e.getIndex
        case _ => return null
      }
    }}
    return fields
  }
}
