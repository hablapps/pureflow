package org.hablapps.populations
package rdd
package writer

import java.sql.ResultSet

import cats.data.Reader
import main.SaveEnrichedPopulations
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.rdd.RDD

class WritePhoenixPopulation extends TestingSuite with SqlContextProvider {

  "WritePhoenix" should "save some rows into HBase " in {

    type Program[t] = Reader[HBaseContext, t]

    val rdd: RDD[EnrichedPopulation] = sqlContext.sparkContext.parallelize(ExpectedResult.apply)
    val cities: String = ExpectedResult.apply.foldLeft[String]("")(_ + "'" + _.name + "',").dropRight(1)

    SaveEnrichedPopulations[Program].write(rdd, PopulationTable.tableName).run(hc)

    val resultSet: ResultSet = stmt.executeQuery(s"SELECT * from ${PopulationTable.tableName} WHERE ${PopulationTable.city} in ($cities)")
    val resultStream = new Iterator[EnrichedPopulation] {
      def hasNext: Boolean = resultSet.next()
      def next: EnrichedPopulation = EnrichedPopulation(resultSet)
    }.toStream

    resultStream should contain theSameElementsAs ExpectedResult.apply
  }

}
