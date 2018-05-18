package es.hablapps.phoenix

import java.sql.{Connection, DriverManager, Statement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseTestingUtility, HConstants}
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT
import org.apache.phoenix.query.BaseTest
import org.junit.rules.TemporaryFolder
import org.scalatest.{BeforeAndAfterAll, Suite}

trait PhoenixTest extends BeforeAndAfterAll { self:Suite=>

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

    val query = s"CREATE TABLE IF NOT EXISTS ${PopulationTable.tableName} (${PopulationTable.state} CHAR(2) NOT NULL, ${PopulationTable.city} VARCHAR, ${PopulationTable.population} BIGINT  CONSTRAINT my_pk PRIMARY KEY (${PopulationTable.state})) DEFAULT_COLUMN_FAMILY='P'"
    stmt.execute(query)
    stmt.execute(s"UPSERT INTO ${PopulationTable.tableName} values ('CA', 'San Francisco', 837442)")
    stmt.execute(s"UPSERT INTO ${PopulationTable.tableName} values ('NY', 'New York', 11231312)")
    stmt.execute(s"UPSERT INTO ${PopulationTable.tableName} values ('TX', 'Texas', 6456465)")

  }

  override def afterAll() {
    PhoenixSparkITHelper.cleanUpAfterTest()
    PhoenixSparkITHelper.doTeardown()

    conn.close()
  }
}

object PopulationTable {
  val tableName:String  = "us_population"
  val state:String      = "STATE"
  val city:String       = "P.CITY"
  val population:String = "P.POPULATION"

  val columns: List[String] = List(state, city, population)
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
    BaseHBaseManagedTimeIT.doTeardown()
  }
  def getHBaseTestingUtility: HBaseTestingUtility = new HBaseTestingUtility(getTestClusterConfig)
  def getUrl: String = BaseTest.getUrl
}