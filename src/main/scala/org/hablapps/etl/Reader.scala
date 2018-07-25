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
  
  import cats.data.{Reader => CReader}

  implicit def toReader[Col[_],E1,E2,T](implicit
      R: Reader[Col,CReader[E1,?],T],
      view: E2 => E1) =
    new Reader[Col,CReader[E2,?], T]{
      type Data = R.Data
      type Error = R.Error

      def load(from: String) = R.load(from).local(view)
      def apply(from: String) = R.apply(from).local(view)
      def valid(from: String) = R.valid(from).local(view)
      def invalid(from: String) = R.invalid(from).local(view)
    }

  implicit def toReaderView[Col[_],E1,E2,T](R: Reader[Col,CReader[E1,?],T])(implicit
      view: E2 => E1) = toReader(R,view)

  import cats.data.State

  implicit def toState[Col[_],E1,E2,T](implicit
      R: Reader[Col,CReader[E1,?],T],
      view: E2 => E1) =
    new Reader[Col,State[E2,?], T]{
      type Data = R.Data
      type Error = R.Error

      def load(from: String) =
        State.inspect{ e2 => R.load(from)(view(e2)) }
      def apply(from: String) =
        State.inspect{ e2 => R.apply(from)(view(e2)) }
      def valid(from: String) =
        State.inspect{ e2 => R.valid(from)(view(e2)) }
      def invalid(from: String) =
        State.inspect{ e2 => R.invalid(from)(view(e2)) }
    }

  implicit def toStateView[Col[_],E1,E2,T](
      R: Reader[Col,CReader[E1,?],T])(implicit
      view: E2 => E1) = toState(R,view)
}