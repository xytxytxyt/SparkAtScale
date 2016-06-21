val DSE_HOME = sys.env.getOrElse("DSE_HOME", sys.env("HOME")+"dse")
val sparkClasspathStr = s"dse spark-classpath".!!.trim
val sparkClasspathArr = sparkClasspathStr.split(':')

// Find all Jars on dse spark-classpath
val sparkClasspath = {
  for ( dseJar <- sparkClasspathArr if dseJar.endsWith("jar"))
    yield Attributed.blank(file(dseJar))
}.toSeq 

val globalSettings = Seq(
  version := "0.1",
  scalaVersion := "2.10.5"
)

lazy val feeder = (project in file("feeder"))
                    .settings(name := "feeder")
                    .settings(globalSettings:_*)
                    .settings(libraryDependencies ++= feederDeps)

lazy val streaming = (project in file("streaming"))
                       .settings(name := "streaming")
                       .settings(globalSettings:_*)
                       .settings(libraryDependencies ++= streamingDeps)

val akkaVersion = "2.3.11"

// This needs to match whatever Spark version being used in DSE
val sparkVersion = "1.6.0"
val kafkaVersion = "0.8.2.1"
val scalaTestVersion = "2.2.4"
val jodaVersion = "2.9"

lazy val feederDeps = Seq(
  "joda-time" % "joda-time" % jodaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.apache.kafka" % "kafka_2.10" % kafkaVersion
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
)

// Do not define in streaming deps if we reference them in existing DSE libs
lazy val streamingDeps = Seq(
  "joda-time"         %  "joda-time"             % jodaVersion  % "provided",
  "org.apache.spark"  %% "spark-mllib"           % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-graphx"          % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-sql"             % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-streaming"       % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-streaming-kafka" % sparkVersion exclude("org.spark-project.spark", "unused"),
  "com.databricks"    %% "spark-csv"             % "1.2.0"
)

lazy val printenv = taskKey[Unit]("Prints classpaths and dependencies")
val env = Map("DSE_HOME" -> DSE_HOME, 
              "sparkClasspath" -> sparkClasspath)
              
printenv := println(env)

//Add dse jars to classpath
unmanagedJars in Compile ++= sparkClasspath 
unmanagedJars in Test ++= sparkClasspath 
