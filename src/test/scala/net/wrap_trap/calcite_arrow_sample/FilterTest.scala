package net.wrap_trap.calcite_arrow_sample

import java.lang.reflect.Modifier
import java.util
import java.util.Collections
import java.lang.reflect.Type

import com.google.common.collect.ImmutableList
import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.tree.{MemberDeclaration, Expressions}
import org.apache.calcite.runtime.Bindable
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by masayuki on 2017/12/17.
  */
class FilterTest extends FlatSpec with Matchers {
  "Expressions" should "generate a java file around filter" in {
    val root0_ = Expressions.parameter(Modifier.FINAL, classOf[DataContext], "root0")
    val block = Expressions.block(
      ImmutableList.of(
        Expressions.statement(Expressions.assign(DataContext.ROOT, root0_))
      )
    )
    val memberDeclarations = new util.ArrayList[MemberDeclaration]
    memberDeclarations.add(
      Expressions.methodDecl(
        Modifier.PUBLIC,
        classOf[Filterable],
        "bind",
        Expressions.list(root0_),
        block))
    val classDesc = Expressions.classDecl(
      Modifier.PUBLIC,
      "Baz",
      null,
      Collections.singletonList(classOf[Bindable[Object]]),
      memberDeclarations)
    val s = Expressions.toString(classDesc)
    println(s)
  }
}
