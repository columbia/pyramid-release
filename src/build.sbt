name := "counttable"

version := "0.1"

organization := "edu.columbia.cs.pyramid"

scalaVersion := "2.10.6"

fork in Test := true
parallelExecution in Test := false

publishMavenStyle := true

// Libraries for testing.
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

// The standard library time implementation is broken so we use Joda.
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.6.0"

// Nicer serialization.
libraryDependencies ++= Seq(("com.twitter" %% "chill" % "0.3.6").
                            exclude("com.esotericsoftware.minlog", "minlog"))

// JSON Parsing
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.4.4"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" %  "2.4.4"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.4"

libraryDependencies += "redis.clients" % "jedis" % "2.8.0"

resolvers ++= Seq(
    "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/")
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

testOptions in Test += Tests.Argument("-oDF")

mainClass in  assembly := Some("edu.columbia.cs.pyramid.BuildCountTable")
