package org.hablapps.populations.rdd.writer

import java.io.File
import java.util.UUID.randomUUID

import cats.data.Reader
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.hablapps.populations.EnrichedPopulation
import org.hablapps.populations.main.SaveHdfsEnrichedPopulations
import org.hablapps.populations.rdd.SqlContextProvider
import org.hablapps.populations.rdd.main.CreateHiveEnrichedPopulations

class WriteHivePopulation extends SqlContextProvider {

  "WriteHive" should "create an external Hive table over an specific location " in {

    type Program[t] = Reader[SQLContext, t]

    val expectedRDD: RDD[EnrichedPopulation] = sc.parallelize(ExpectedResult.apply)
    val tmpDir: String = s"${System.getProperty("java.io.tmpdir")}${randomUUID().toString}"
    val tableName = "test_etl"

    SaveHdfsEnrichedPopulations[Program].write(expectedRDD,tmpDir).run(sqlContext)
    CreateHiveEnrichedPopulations[Program].write(expectedRDD, (tmpDir, tableName)).run(sqlContext)

    val resultRDD: RDD[EnrichedPopulation] = sqlContext.sql(s"FROM $tableName SELECT *").rdd.map(EnrichedPopulation(_))
    assertRDDEquals(expectedRDD, resultRDD)

    FileUtils.forceDeleteOnExit(new File(tmpDir))

  }

}
