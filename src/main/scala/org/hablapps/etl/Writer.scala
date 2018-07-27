package org.hablapps.etl

abstract class Writer[Collection[_], P[_], T]{
  def write(dataset: Collection[T], destination: String): P[Unit]
}

object Writer{

  implicit class toQ[Col[_],P[_],T](val w: Writer[Col,P,T]){
    def lift[Q[_]](implicit L: naturally.NatTrans[P, Q]) =
      new Writer[Col,Q,T]{
        def write(dataset: Col[T], destination: String) =
          L.nat(w.write(dataset,destination))
      }
  }
}
