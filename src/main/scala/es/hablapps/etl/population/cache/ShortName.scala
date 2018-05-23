package es.hablapps.etl.population.cache

import scala.util.{Failure, Success, Try}


case class ShortName() {
  def get(name:String): String = Try(ShortName.names(name)) match {
    case Success(v) => v
    case Failure(e) => "NF"
  }
}

object ShortName {

  lazy val names: Map[String, String] = Map[String, String](
    "Madrid" -> "MA",
    "Barcelona" -> "BA",
    "Zamora" -> "ZA"
  )



}
