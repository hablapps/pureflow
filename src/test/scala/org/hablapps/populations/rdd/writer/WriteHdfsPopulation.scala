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

class WriteHdfsPopulation extends SqlContextProvider {

  "WriteHdfs" should "save some rows into Hdfs " in {

    type Program[t] = Reader[SQLContext, t]

    val expectedRDD: RDD[EnrichedPopulation] = sc.parallelize(ExpectedResult.apply)
    val tmpDir: String = s"${System.getProperty("java.io.tmpdir")}${randomUUID.toString}"

    SaveHdfsEnrichedPopulations[Program].write(expectedRDD,tmpDir).run(sqlContext)

    val resultRDD = sqlContext.read.parquet(tmpDir).map(r => EnrichedPopulation(r.getString(0), r.getString(1), r.getLong(2)))
    assertRDDEquals(expectedRDD, resultRDD)

    FileUtils.forceDeleteOnExit(new File(tmpDir))

  }

}
