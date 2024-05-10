ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "gcp-bq-spanner"
  )
val sparkVersion = "3.5.1"
val sparkSpanner = "6.45.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "com.google.cloud" % "google-cloud-spanner" % sparkSpanner,

)
