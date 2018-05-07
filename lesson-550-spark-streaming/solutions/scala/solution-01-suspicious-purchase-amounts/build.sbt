val sparkV = "1.5.2"

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.example.spark.streaming",
  scalaVersion := "2.11.7"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "solution-01-suspicious-purchase-amounts",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"      % sparkV /* % "provided" */,
      "org.apache.spark" %% "spark-streaming" % sparkV /* % "provided" */,
      "org.scalatest"    %% "scalatest"       % "2.2.5"   % "test"
    )
  )
