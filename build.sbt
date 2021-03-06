ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

val argsParser = Seq("org.sellmerfud" %% "optparse" % "2.2",
                     "commons-cli" % "commons-cli" % "1.5.0")
val json4sJackson = "org.json4s" %% "json4s-jackson" % "4.1.0-M1"
val http = "org.scalaj" %% "scalaj-http" % "2.4.2"

val akkaActor = "com.typesafe.akka" %% "akka-actor" % "2.6.19"

libraryDependencies ++= Seq(json4sJackson, http, akkaActor) ++ argsParser

lazy val testCrawler = (project in file("."))
  .settings(
    name := "scala-test-crawler"
  )

// set the main class for 'sbt run'
mainClass in(Compile, run) := Some("crawler.TestCrawler")

// set the main class for packaging the main jar
mainClass in(Compile, packageBin) := Some("crawler.TestCrawler")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}