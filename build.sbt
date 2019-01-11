name := "flinkprojects"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.flink/flink-scala
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.7.1"
// https://mvnrepository.com/artifact/org.apache.flink/flink-core
libraryDependencies += "org.apache.flink" % "flink-core" % "1.7.1"

// https://mvnrepository.com/artifact/org.apache.flink/flink-table
libraryDependencies += "org.apache.flink" %% "flink-table" % "1.7.1" % "provided"
// https://mvnrepository.com/artifact/org.apache.flink/flink-table-common
libraryDependencies += "org.apache.flink" % "flink-table-common" % "1.7.1"

// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.7.1"

// https://mvnrepository.com/artifact/org.apache.flink/flink-jdbc
libraryDependencies += "org.apache.flink" %% "flink-jdbc" % "1.7.1"
// https://mvnrepository.com/artifact/org.apache.flink/flink-clients
libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.7.1"
