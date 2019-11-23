name := "kafka-streams-favourite-color-scala"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "2.3.1",
  "org.slf4j" % "slf4j-simple" % "1.7.29",
)

//javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
//scalacOptions := Seq("-target:jvm-1.8")
//initialize := {
//  val _ = initialize.value
//  if (sys.props("java.specification.version") != "1.8") {
//    sys.error("Java 8 is required for this project")
//  }
//}
