package es.hablapps.etl

import java.sql.{Connection, DriverManager, Statement}

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.regionserver.ShutdownHook
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseTestingUtility, HConstants}
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT
import org.apache.phoenix.query.BaseTest
import org.apache.spark.sql.SQLContext
import org.junit.rules.TemporaryFolder
import org.scalatest.{FlatSpec, Matchers, Suite}

trait TestingSuite extends FlatSpec with Matchers with SharedSparkContext  {
  self: Suite =>

  lazy val sqlContext: SQLContext = new SQLContext(sc)
  lazy val hc: HBaseContext = new HBaseContext(sc, hbaseConfiguration)

  lazy val hbaseConfiguration: Configuration = {
    val conf = PhoenixSparkITHelper.getTestClusterConfig
    val quorum = conf.get(HConstants.ZOOKEEPER_QUORUM)
    val clientPort = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT)
    conf.set(HConstants.ZOOKEEPER_QUORUM, s"$quorum:$clientPort")
    conf
  }

  lazy val zookeeperQuorum: String = hbaseConfiguration.get(HConstants.ZOOKEEPER_QUORUM)
  lazy val conn: Connection = {
    val c = DriverManager.getConnection(PhoenixSparkITHelper.getUrl)
    c.setAutoCommit(true)
    c
  }
  lazy val stmt: Statement = conn.createStatement()

  override def beforeAll(): Unit ={

    PhoenixSparkITHelper.doSetup()

    val query = s"CREATE TABLE IF NOT EXISTS ${PopulationTable.tableName} (${PopulationTable.city} VARCHAR NOT NULL, ${PopulationTable.state} CHAR(2), ${PopulationTable.population} BIGINT  CONSTRAINT my_pk PRIMARY KEY (${PopulationTable.city})) DEFAULT_COLUMN_FAMILY='P'"
    println(query)
    stmt.execute(query)
    stmt.execute(s"UPSERT INTO ${PopulationTable.tableName} values ('San Francisco', 'CA', 837442)")
    stmt.execute(s"UPSERT INTO ${PopulationTable.tableName} values ('New York', 'NY', 11231312)")
    stmt.execute(s"UPSERT INTO ${PopulationTable.tableName} values ('Texas', 'TX', 6456465)")

    super.beforeAll()
  }

  override def afterAll() {
    PhoenixSparkITHelper.cleanUpAfterTest()
    PhoenixSparkITHelper.doTeardown()
    conn.close()
    super.afterAll()
  }
}

object PopulationTable {
  val tableName:String  = "population"
  val city:String       = "CITY"
  val state:String      = "P.STATE"
  val population:String = "P.POPULATION"

  val columns: List[String] = List(city, state, population)
}


object PhoenixSparkITHelper extends BaseHBaseManagedTimeIT {
  val baseTest: BaseHBaseManagedTimeIT = new BaseHBaseManagedTimeIT(){}
  def tmpFolder: TemporaryFolder = BaseTest.tmpFolder
  def getTestClusterConfig: Configuration = BaseHBaseManagedTimeIT.getTestClusterConfig
  def doSetup(): Unit = {
    tmpFolder.create()
    BaseHBaseManagedTimeIT.doSetup()
  }
  def doTeardown(): Unit = {
    tmpFolder.delete()
    baseTest.cleanUpAfterTest()
    BaseHBaseManagedTimeIT.doTeardown()
  }
  def getHBaseTestingUtility: HBaseTestingUtility = new HBaseTestingUtility(getTestClusterConfig)
  def getUrl: String = BaseTest.getUrl
}
