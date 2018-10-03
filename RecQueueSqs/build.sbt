
import sbt.Keys.libraryDependencies

lazy val root = (project in file(".")).

  settings(
    //sbtPlugin := true,
    name := "RecQueueSqs",
    version := "1.0",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("com.spark.sqs.MainReceiver")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.2.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
  "org.apache.spark" %% "spark-catalyst" % "2.2.0" % Test,
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "org.scala-lang" % "scala-library" % "2.11.8",
  "org.apache.hadoop" % "hadoop-common" % "2.6.5" % "provided",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.1.0.jre7" % Test,
  "com.amazonaws" % "aws-java-sdk-sqs" % "1.11.405",
  "com.amazonaws" % "aws-java-sdk" % "1.11.405",
  "com.github.seratch" %% "awscala" % "0.7.+",
  "com.googlecode.json-simple" % "json-simple" % "1.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
