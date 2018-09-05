package org.hablapps
package etl

import cats.data.Kleisli

trait TransformDefs{

  type Transform[Collection[_], P[_], I, O] =
    Kleisli[P, Collection[I], Collection[O]]

  type TransformB[Collection[_], P[_], I, O] =
    Kleisli[P, Collection[I], Collection[O]]

  type Transform2[Collection[_], P[_], I1, I2, O] =
    Kleisli[P, (Collection[I1], Collection[I2]), Collection[O]]

  type TransformB2[Collection[_], P[_], I1, I2, O] =
    Kleisli[P, (Collection[I1], I2), Collection[O]]

  type Transform3[Collection[_], P[_], I1, I2, I3, O] =
    Kleisli[P, (Collection[I1], Collection[I2], Collection[I3]), Collection[O]]

  // etc.
}
