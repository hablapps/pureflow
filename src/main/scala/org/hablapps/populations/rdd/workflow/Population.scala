package org.hablapps
package populations
package rdd
package workflow

case class Population(name: String, population: Long)

object Population{

  sealed abstract class Error
  case class NegativePopulation(p: Long) extends Error
  case class EmptyName() extends Error

}
