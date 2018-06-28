package org.hablapps.etl
package rdd

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.rdd.RDD

import cats.data.{ValidatedNel, Validated}, Validated.{Invalid, Valid}
import cats.Functor, cats.syntax.functor._

abstract class RDDReader[P[_]: Functor, T: ClassTag] extends Reader[RDD,P,T]{

  def parse(data: Data): ValidatedNel[Error, T]

  def apply(from: String): P[RDD[Validated[(Data,List[Error]), T]]] =
    load(from).map(
      _.map( data => parse(data).leftMap(nel => (data,nel.toList))))

  def valid(from: String): P[RDD[T]] =
    apply(from).map(
      _.collect{ case Valid(value) => value })

  def invalid(from: String): P[RDD[(Data, List[Error])]] =
    apply(from).map(
      _.collect{ case Invalid(error) => error })
}

object RDDReader{

  // import cats.~>

  // implicit def fromP[P[_],Q[_]: Functor,T](implicit
  //     R: RDDReader[P,T],
  //     nat: P ~> Q) =
  //   new RDDReader[Q, T]{
  //     type Data = R.Data
  //     type Error = R.Error

  //     val F = implicitly[Functor[Q]]
  //     def parse(data: Data) = R.parse(data)
  //     def load(from: String) = nat(R.load(from))
  //   }

  // implicit def fromReader[E1,E2](implicit f: E1 => E2): CReader[E2,?] ~> CReader[E1, ?] =
  //   new (CReader[E2,?]~>CReader[E1,?]){
  //     def apply[T](r: CReader[E2,T]) = r.local(f)
  //   }

  import cats.data.{Reader => CReader}

  implicit def toReader[E1,E2,T: ClassTag](implicit
      R: RDDReader[CReader[E1,?],T],
      view: E2 => E1) =
    new RDDReader[CReader[E2,?], T]{
      type Data = R.Data
      type Error = R.Error

      val F = implicitly[cats.Functor[CReader[E2,?]]]
      def parse(data: Data) = R.parse(data)
      def load(from: String) = R.load(from).local(view)
    }

  implicit def toReaderView[E1,E2,T: ClassTag](R: RDDReader[CReader[E1,?],T])(implicit
      view: E2 => E1) =
    toReader(classTag[T],R,view)

  import cats.data.State

  implicit def toState[E1,E2,T: ClassTag](implicit
      R: RDDReader[CReader[E1,?],T],
      view: E2 => E1) =
    new RDDReader[State[E2,?], T]{
      type Data = R.Data
      type Error = R.Error

      val F = implicitly[cats.Functor[State[E2,?]]]

      def parse(data: Data) =
        R.parse(data)

      def load(from: String) =
        State{ e2 => (e2, R.load(from)(view(e2))) }
    }

  implicit def toStateView[E1,E2,T: ClassTag](
      R: RDDReader[CReader[E1,?],T])(implicit
      view: E2 => E1) =
    toState(classTag[T],R,view)

}