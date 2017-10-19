name := "HBaseSnapshotter"
version := "2.8"
scalaVersion in ThisBuild := "2.10.4"
exportJars := true

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Cloudera repo" at "https://repository.cloudera.com/content/repositories/releases/"
)

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase-spark" % "1.2.0-cdh5.8.2",
  "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.8.2",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0-cdh5.8.2",
  "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.8.2",
  "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.8.2",
  "org.apache.spark" % "spark-core_2.10" % "1.6.0-cdh5.8.2" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.0-cdh5.8.2" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0-cdh5.8.2" % "provided",
  "com.google.code.gson" % "gson" % "2.2.4",
  "com.typesafe" % "config" % "1.2.1",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)

excludeDependencies ++= Seq(
  "org.mortbay.jetty",
  "org.cloudera.logredactor"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
  case "overview.html" => MergeStrategy.rename
  case "plugin.xml" => MergeStrategy.rename
  case "parquet.thrift" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last

  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
