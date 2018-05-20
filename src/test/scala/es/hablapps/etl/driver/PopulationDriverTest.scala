package es.hablapps.etl.driver

import java.sql.ResultSet

import es.hablapps.etl.TestingSuite
import es.hablapps.etl.population.driver.PopulationDriver
import es.hablapps.etl.population.model.EnrichedPopulation

class PopulationDriverTest extends TestingSuite {

  "Population" should "enrich some rows " in {
    val (raw, enriched) = PopulationDriver.defaultPipeline().runPipeline(sc, hc)

    raw.count() shouldBe 4
    raw.count() shouldBe enriched.count()

    enriched.foreach(p => println(p.toString))
    enriched.filter(p => p.shortName.equalsIgnoreCase("NF")).count() shouldBe 2
    enriched.filter(p => !p.shortName.equalsIgnoreCase("NF")).count() shouldBe 2

    val resultSet: ResultSet = stmt.executeQuery(s"SELECT * FROM population")
    val resultStream = new Iterator[EnrichedPopulation] {
      def hasNext: Boolean = resultSet.next()
      def next: EnrichedPopulation = EnrichedPopulation(resultSet)
    }.toStream

    resultStream.foreach(println)

    (resultStream.count(_=>true)==7) shouldBe true
  }
}
