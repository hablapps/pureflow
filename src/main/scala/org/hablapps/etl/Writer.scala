package org.hablapps.etl

import org.apache.spark.rdd.RDD

abstract class Writer[Collection[_], P[_], T]{
  def write(dataset: Collection[T], destination: String): P[Unit]
}


object Writer{

  import cats.data.{Reader => CReader}

  implicit def toReader[Col[_],E1,E2,T](implicit
    w: Writer[Col,CReader[E1,?],T],
    f: E2 => E1) = new Writer[Col,CReader[E2,?],T]{
    def write(dataset: Col[T], destination: String) =
      w.write(dataset,destination).local(f)
  }

  implicit def toReaderView[Col[_],E1,E2,T](
    w: Writer[Col,CReader[E1,?],T])(implicit
    f: E2 => E1) = toReader(w,f)


  import cats.data.State

  implicit def toStateView[Col[_],E1,E2,T](
    w: Writer[Col,State[E1,?],T])(implicit
    g: E2 => E1, s: E2 => E1 => E2) = new Writer[Col,State[E2,?],T]{
      def write(dataset: Col[T], destination: String) =
        w.write(dataset,destination).lift(g,s)
    }
}
