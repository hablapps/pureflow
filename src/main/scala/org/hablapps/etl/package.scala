package org.hablapps

package object etl{

  import org.apache.spark.sql.SQLContext
  import org.apache.spark.SparkContext

  implicit def sqlContextFrom(implicit sc: SparkContext) =
    new SQLContext(sc)

  // Reader transformations

  implicit def tuple4to12[A,B,C,D]: ((A,B,C,D)) => (A,B) = t => (t._1,t._2)
  implicit def tuple4to3[A,B,C,D]: ((A,B,C,D)) => C = t => t._3
  implicit def tuple4to4[A,B,C,D]: ((A,B,C,D)) => D = t => t._4
  implicit def tuple2to1[A,B]: ((A,B)) => A = t => t._1

  // State setters

  implicit def tuple2to1Setter[A,B]: ((A,B)) => A => ((A,B)) = t => a => (a,t._2)

  // Lens -> nat. transf.

  import cats.data.State

  implicit class FromLens[S1,T](p: State[S1,T]){
    def lift[S2](g: S2 => S1, s: S2 => S1 => S2): State[S2,T] = State{ s2 =>
      val (s1,t) = p.run(g(s2)).value
      (s(s2)(s1), t)
    }
  }

  // Const utilities

  import cats.data.Const

  implicit class ConstOps[C,T](c: Const[C,T]){
    def modify[U](f: C => C): Const[C,U] =
      Const(f(c.getConst)).retag[U]
  }
}