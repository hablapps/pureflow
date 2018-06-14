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
}