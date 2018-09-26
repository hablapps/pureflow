package org.hablapps
package translation
package test

import org.apache.spark.sql.{Row, DataFrame}

package object translate {

  case class Person(name: String, age: Int, IO_ID: String)

  case class PersonLU(nameKey: String, nameValue: String, timestamp: String)

  case class PersonTranslated(name: String, age: Int, ioId: String, nameOutput: Option[String])
  object PersonTranslated {
    def fromRow(row: Row): PersonTranslated = row match {
      case Row(name: String, age: Int, ioId: String, nameOutput: String) => PersonTranslated(name, age, ioId, Option(nameOutput))
      case Row(name: String, age: Int, ioId: String, _) => PersonTranslated(name, age, ioId, Option.empty)
    }
  }

  implicit class DataFrameTestUtilities(df: DataFrame) {
    def toList[A](f: Row => A): List[A] = df.collect().toList.map(f)
  }

}
