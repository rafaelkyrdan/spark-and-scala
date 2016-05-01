name := "spark-examples"

version := "1.0"

scalaVersion := "2.11.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "MVN Repo" at "http://mvnrepository.com/artifact"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-repl" % "1.6.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2"

