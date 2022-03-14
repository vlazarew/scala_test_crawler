ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

val argsParser = "org.sellmerfud" %% "optparse" % "2.2"
val json4sJackson = "org.json4s" %% "json4s-jackson" % "4.1.0-M1"
val http = "org.scalaj" %% "scalaj-http" % "2.4.2"

libraryDependencies ++= Seq(argsParser, json4sJackson, http)

lazy val root = (project in file("."))
  .settings(
    name := "scala-test-crawler"
  )
