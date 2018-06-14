package org.hablapps.etl

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import cats.data.{ValidatedNel, Validated}, Validated.{Invalid, Valid}
import cats.Functor, cats.syntax.functor._

trait Reader[P[_],T]{
  implicit val F: Functor[P]
  
  type Data
  type Error

  def parse(data: Data): ValidatedNel[Error, T]

  def load(from: String): P[RDD[Data]]

  def apply(from: String): P[RDD[Validated[(Data,List[Error]), T]]] = 
    load(from).map(
      _.map( data => parse(data).leftMap(nel => (data,nel.toList))))

  def valid(from: String)(implicit
      ct: ClassTag[T]): P[RDD[T]] = 
    apply(from).map(
      _.collect{ case Valid(value) => value })
  
  def invalid(from: String)(implicit 
      ct: ClassTag[T]): P[RDD[(Data, List[Error])]] = 
    apply(from).map(
      _.collect{ case Invalid(error) => error })
}

object Reader{

  // import cats.~>
  
  // implicit def fromP[P[_],Q[_]: Functor,T](implicit 
  //     R: Reader[P,T],
  //     nat: P ~> Q) =
  //   new Reader[Q, T]{
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

  implicit def fromP[E1,E2,T](implicit 
      R: Reader[CReader[E1,?],T],
      view: E2 => E1) =
    new Reader[CReader[E2,?], T]{
      type Data = R.Data
      type Error = R.Error

      val F = implicitly[cats.Functor[CReader[E2,?]]]
      def parse(data: Data) = R.parse(data)
      def load(from: String) = R.load(from).local(view)
    }

}