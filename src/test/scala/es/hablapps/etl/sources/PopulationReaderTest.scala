package es.hablapps.etl.sources

import es.hablapps.etl.TestingSuite
import es.hablapps.etl.population.model.{PopulationPK, RawPopulation}
import es.hablapps.etl.population.sources.PopulationReader
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class PopulationReaderTest extends TestingSuite {

  lazy val expectedResult: RawPopulation = RawPopulation("Madrid","2824000")

  "Reader" should "return and DF with four rows " in {
    val df: RDD[(PopulationPK, RawPopulation)] = new PopulationReader().query(sqlContext, StorageLevel.MEMORY_AND_DISK)

    df.count() shouldBe 4
    df.first()._1 shouldBe PopulationPK(expectedResult.name)
    df.first()._2.name shouldBe expectedResult.name
    df.first()._2.population shouldBe expectedResult.population

    df.unpersist()

  }
}
