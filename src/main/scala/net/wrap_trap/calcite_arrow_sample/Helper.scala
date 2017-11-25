package net.wrap_trap.calcite_arrow_sample

/**
  * Created by masayuki on 2017/11/20.
  */
object Helper {
  def using[A, R <: {def close()}](r: R)(f : R => A): A = {
    try {
      f(r)
    } finally {
      try { r.close() } catch {case ignore: Exception => {}}
    }
  }

}
