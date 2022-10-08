ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.10"

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
    assembly / mainClass := Some("it.unibo.bd.FlowDuration"),
    assembly / assemblyJarName := "main.jar",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.0" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.3.0" % Provided,
      "org.apache.spark" %% "spark-streaming" % "3.3.0" % Provided,
      "io.github.cibotech" %% "evilplot" % "0.8.1",
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
      (assembly / assemblyOutputPath).value -> "main.jar",
    ),
    remoteDeployAfterHooks := Seq(sshClient => {
      sshClient
        .exec(IO.read(new File("remote_command.txt")))
        .foreach(r => {
          import scala.Console
          println(r.stdOutAsString())
          Console.err.println(r.stdErrAsString())
        })
    }),
    localDeploy := {
      import scala.sys.process._
      assembly.value
      IO.read(new File("local_command.txt")).!<
    },
  )
