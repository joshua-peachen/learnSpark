ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.5"

lazy val root = (project in file("."))
  .settings(
    name := "learnSpark"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.2.1"
)
