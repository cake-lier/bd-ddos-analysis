ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.8"

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val root = project
  .in(file("."))
  .settings(
    name := "bd-ddos-analysis",
    idePackagePrefix := Some("it.unibo.bd"),
    assembly / mainClass := Some("it.unibo.bd.Main"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.1",
      "org.apache.spark" %% "spark-sql" % "3.2.1",
    ),
  )
