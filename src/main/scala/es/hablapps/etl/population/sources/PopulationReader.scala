package es.hablapps.etl.population.sources

import es.hablapps.etl.common.IAmASparkSQLQueristKey
import es.hablapps.etl.population.model.{PopulationPK, RawPopulation}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

case class PopulationReader() extends IAmASparkSQLQueristKey[PopulationPK, RawPopulation] with Serializable {
  override protected val readPath: String = null

  override def query(sqlContext: SQLContext, storageLevel: StorageLevel): RDD[(PopulationPK, RawPopulation)] = {
    val df = PopulationReader.query(sqlContext)

    df.map(t => {
      val obj = RawPopulation(t)
      (obj.rawPK, obj)
    })
      .persist(storageLevel)
  }
}

object PopulationReader {
  def query(sQLContext: SQLContext): DataFrame = {

    sQLContext.createDataFrame(Seq(
      RawPopulation("Madrid","2824000"),
      RawPopulation("Barcelona","1454000"),
      RawPopulation("Valencia","736000"),
      RawPopulation("Sevilla","695000")))
      .toDF()

  }
}
