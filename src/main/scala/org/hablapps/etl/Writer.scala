package org.hablapps.etl

import org.apache.spark.rdd.RDD

trait Writer[P[_],T]{
  def write(dataset: RDD[T], destination: String): P[Unit]
}

object Writer{

  import cats.data.{Reader => CReader}

  implicit def toReader[E1,E2,T](implicit
    w: Writer[CReader[E1,?],T],
    f: E2 => E1) = new Writer[CReader[E2,?],T]{
    def write(dataset: RDD[T], destination: String) =
      w.write(dataset,destination).local(f)
  }

  implicit def toReaderView[E1,E2,T](
    w: Writer[CReader[E1,?],T])(implicit
    f: E2 => E1) = toReader(w,f)


  import cats.data.State

  implicit def toStateView[E1,E2,T](
    w: Writer[State[E1,?],T])(implicit
    g: E2 => E1, s: E2 => E1 => E2) = new Writer[State[E2,?],T]{
      def write(dataset: RDD[T], destination: String) =
        w.write(dataset,destination).lift(g,s)
    }
}