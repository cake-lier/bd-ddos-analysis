ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.16"

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val startupTransition: State => State = { s: State =>
  "conventionalCommits" :: s
}

val localDeploy = taskKey[Unit]("Run application on local machine")

lazy val root = project
  .in(file("."))
  .enablePlugins(RemoteDeployPlugin)
  .settings(
    name := "bd-ddos-analysis",
    idePackagePrefix := Some("it.unibo.bd"),
    scalacOptions ++= Seq(
      "-language:higherKinds",
    ),
    assembly / mainClass := Some("it.unibo.bd.Merge"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.1" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.2.1" % Provided,
    ),
    Global / onLoad := {
      startupTransition compose (Global / onLoad).value
    },
    remoteDeploy := {
      assembly.value
      remoteDeploy.evaluated
    },
    remoteDeployConfFiles := Seq("aws_config.conf"),
    remoteDeployArtifacts := Seq(
      (Compile / packageBin).value.getParentFile / (assembly / assemblyJarName).value -> "main.jar",
    ),
    remoteDeployAfterHooks := Seq(sshClient => {
      sshClient
        .exec(
          "spark-submit "
            + "--class it.unibo.bd.NetProtocol "
//            + "--num-executors 2 "
//            + "--executor-cores 3 "
//            + "--executor-memory 8G "
//            + "--conf spark.dynamicAllocation.enabled=false "
            + "main.jar "
            + "unibo-bd2122-mcastellucci/project",
        )
        .foreach(r => { println(r.stdOutAsString()); println(r.stdErrAsString()) })
    }),
    localDeploy := {
      assembly.value
      import scala.sys.process._
      (
        "spark-submit "
          + "--class it.unibo.bd.Merge "
          + "--master local[*] "
          + s"${(Compile / packageBin).value.getParentFile / (assembly / assemblyJarName).value} "
          + "unibo-bd2122-xxx/yyy"
      ).!<
    },
  )
