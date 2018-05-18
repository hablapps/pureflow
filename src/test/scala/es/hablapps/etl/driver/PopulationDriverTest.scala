package es.hablapps.etl.driver

import es.hablapps.etl.TestingSuite
import es.hablapps.etl.population.driver.PopulationDriver
import org.apache.hadoop.hbase.spark.HBaseContext

class PopulationDriverTest extends TestingSuite {

  "Population" should "enrich some rows " in {
    val (raw, enriched) = PopulationDriver.defaultPipeline().runPipeline(sc, new HBaseContext(sc, configuration))

    raw.count() shouldBe 4
    raw.count() shouldBe enriched.count()

    enriched.foreach(p => println(p.toString))
    enriched.filter(p => p.shortName.equalsIgnoreCase("Not Found")).count() shouldBe 2
    enriched.filter(p => !p.shortName.equalsIgnoreCase("Not Found")).count() shouldBe 2
  }
}
