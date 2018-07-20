package org.hablapps.etl

import cats.{Id, Eval}

import org.apache.spark.rdd.RDD

import shapelens.{SurfaceGetter, SurfaceLens}

abstract class Writer[Collection[_], P[_], T]{
  def write(dataset: Collection[T], destination: String): P[Unit]
}

object Writer{

  import cats.~>
  implicit def toQ[Col[_],P[_],Q[_],T](
      w: Writer[Col,P,T])(implicit
      L: P ~> Q) =
    new Writer[Col,Q,T]{
      def write(dataset: Col[T], destination: String) =
        L(w.write(dataset,destination))
    }

  import cats.data.{Reader => CReader, State}

  implicit def toReader[Col[_],E1,E2,T](implicit
      w: Writer[Col,CReader[E1,?],T],
      S: SurfaceGetter[Id, E2, E1]) =
    new Writer[Col,CReader[E2,?],T]{
      def write(dataset: Col[T], destination: String) =
        S.apply(w.write(dataset,destination))
    }

  implicit def toReaderView[Col[_],E1,E2,T](
      w: Writer[Col,CReader[E1,?],T])(implicit
      S: SurfaceGetter[Id, E2, E1]) =
    toReader(w,S)

  implicit def toStateView[Col[_],E1,E2,T](
      w: Writer[Col,State[E1,?],T])(implicit
      L: SurfaceLens[Eval, E2, E1]) =
    new Writer[Col,State[E2,?],T]{
      def write(dataset: Col[T], destination: String) =
        L.apply(w.write(dataset,destination))
    }
}
