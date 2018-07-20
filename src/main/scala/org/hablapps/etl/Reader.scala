package org.hablapps.etl

import cats.data.Validated

abstract class Reader[Collection[_], P[_], T] extends java.io.Serializable{
  type Data
  type Error

  def load(from: String): P[Collection[Data]]

  def apply(from: String): P[Collection[Validated[(Data,List[Error]), T]]]

  def valid(from: String): P[Collection[T]]

  def invalid(from: String): P[Collection[(Data, List[Error])]]
}

object Reader{

  implicit class toQC[Col[_],P[_],T](val r: Reader[Col,P,T]){
    def lift[Q[_]](implicit N: shapelens.NatTrans[P,Q]) =
      new Reader[Col, Q, T]{
        type Data = r.Data
        type Error = r.Error

        def load(from: String) = N.nat(r.load(from))
        def apply(from: String) = N.nat(r.apply(from))
        def valid(from: String) = N.nat(r.valid(from))
        def invalid(from: String) = N.nat(r.invalid(from))
      }
  }
}
