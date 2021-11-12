name := "gpubsub"

version := "0.1"

scalaVersion := "3.1.0"


// https://mvnrepository.com/artifact/com.google.cloud/google-cloud-pubsub
libraryDependencies += "com.google.cloud" % "google-cloud-pubsub" % "1.114.7"

val slf4jVersion = "2.0.0-alpha5"
libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % slf4jVersion, "org.slf4j" % "slf4j-simple" % slf4jVersion)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test

libraryDependencies += "org.scalatestplus" %% "scalacheck-1-15" % "3.2.10.0" % Test

libraryDependencies += "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % Test