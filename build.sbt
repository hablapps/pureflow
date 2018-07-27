val clouderaRelease     = "cdh5.11.2"
val sparkVersion        = "1.6.0-" + clouderaRelease
val hadoopVersion       = "2.6.0-" + clouderaRelease
val hbaseVersion        = "1.2.0-" + clouderaRelease
val slf4jVersion        = "1.7.10"
val holdenKarauVersion  = "1.6.0_0.9.0"
val scalaTestVersion    = "3.0.5"
val phoenixVersion      = "4.13.2-" + clouderaRelease

lazy val root = (project in file("."))
  .settings(
    organization := "es.hablapps",
    name := "spark_etl",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.10.6",

    partialUnificationModule := "com.milessabin" % "si2712fix-plugin" % "1.2.0",

    parallelExecution in Test := false,

    javaOptions ++= Seq("-Xms2G", "-Xmx2G", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),

    resolvers ++= Seq(
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      "Spray Repository" at "http://repo.spray.cc/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
      "Twitter Maven Repo" at "http://maven.twttr.com/",
      "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
      Resolver.sonatypeRepo("public")
    ),

    //Spark
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core",
      "org.apache.spark" %% "spark-sql",
      "org.apache.spark" %% "spark-streaming",
      "org.apache.spark" %% "spark-hive"
    ).map(_ % sparkVersion)
      .map(_ % "compile,test" classifier "" classifier "tests"),

    //Phoenix
    libraryDependencies += "org.apache.phoenix" % "phoenix-spark" % phoenixVersion,
    libraryDependencies += "org.apache.phoenix" % "phoenix-core" % phoenixVersion % "compile,test" classifier "" classifier "tests",

    //HBase
    libraryDependencies ++= Seq(
      "org.apache.hbase" % "hbase-common",
      "org.apache.hbase" % "hbase-server",
      "org.apache.hbase" % "hbase-spark",
      "org.apache.hbase" % "hbase-it",
      "org.apache.hbase" % "hbase-hadoop-compat",
      "org.apache.hbase" % "hbase-hadoop2-compat"
    ).map(_ % hbaseVersion)
    .map(_ % "compile,test" classifier "" classifier "tests"),

    //Hadoop
    libraryDependencies ++= excludeJavaxServlet(Seq(
      "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "compile,test" classifier "" classifier "tests",
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "compile,test" classifier "" classifier "tests" ,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "compile,test" classifier "" classifier "tests" ,
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "compile,test" classifier "" classifier "tests",
      "org.apache.hadoop" % "hadoop-yarn-server-tests" % hadoopVersion % "compile,test" classifier "" classifier "tests",
      "org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion % "compile,test" classifier "" classifier "tests",
      "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % "compile,test")),

    // Hablapps
    libraryDependencies ++= Seq(
      "org.hablapps" %% "naturally" % "0.1-SNAPSHOT",
      compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
    ),

    // Testing
    libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % holdenKarauVersion % "test" excludeAll ExclusionRule(organization = "org.apache.hadoop"),

    libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion,
    "org.eclipse.jetty" % "jetty-util" % "9.3.11.v20160721"),

    libraryDependencies += "org.typelevel" %% "cats" % "0.9.0",

    excludeDependencies ++= Seq(
      "asm",
      "com.amazonaws",
      "io.dropwizard",
      "org.apache.mesos",
      "com.ibm.icu",
      "tomcat"
    ),

    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")
  )

// Based on Hadoop Mini Cluster tests from Alpine's PluginSDK (Apache licensed)
// javax.servlet signing issues can be tricky, we can just exclude the dep
def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) =
  items.map(_.exclude(group, artifact))

def excludeJavaxServlet(items: Seq[ModuleID]) =
  excludeFromAll(items, "javax.servlet", "servlet-api")
