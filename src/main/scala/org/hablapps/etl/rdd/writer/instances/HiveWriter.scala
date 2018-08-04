package org.hablapps.etl
package rdd

import cats.MonadReader
import cats.syntax.functor._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.hablapps.etl.rdd.writer.Writer

import scala.reflect.runtime.universe.TypeTag

abstract class HiveWriter[
P[_]: MonadReader[?[_], SQLContext],
T <: Product : TypeTag]
  extends Writer[P, T]{

  type K = (String, String)

  def write(rdd: RDD[T], destProp:K): P[Unit] =
    MonadReader[P, SQLContext].ask. map { implicit sQLContext =>

      import sQLContext.implicits._

      val (destination, tableName) = destProp

      val nameTableTmp = s"${tableName}_tmp"
      rdd.toDF().registerTempTable(nameTableTmp)

      val columns = sQLContext.sql(s"describe $nameTableTmp")
        .map(row => s"${row(0)}  ${row(1)}").collect()
        .mkString(", ")

      val dropTable: String = s"DROP TABLE IF EXISTS $nameTableTmp"

      //TODO the hive table properties (snappy compression, parquet format...) should be read from a properties file
      val createExternalTable : String = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName}
                                            |  ($columns)
                                            |  STORED AS PARQUET
                                            |  LOCATION '${destination}'
                                            |  tblproperties('parquet.compress'='SNAPPY')""".stripMargin

      val repairTable: String = s"msck repair table ${tableName}"


      sQLContext.sql(dropTable)
      sQLContext.sql(createExternalTable)
      sQLContext.sql(repairTable)
    }
}