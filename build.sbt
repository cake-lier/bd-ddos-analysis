ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.8"

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val startupTransition: State => State = { s: State =>
  "conventionalCommits" :: s
}

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
    Global / onLoad := {
      startupTransition compose (Global / onLoad).value
    },
    remoteDeployConfFiles := Seq("aws_config.conf"),
    remoteDeployArtifacts := Seq(
      (Compile / packageBin).value.getParentFile / (assembly / assemblyJarName).value -> "main.jar",
    ),
    remoteDeployAfterHooks := Seq(sshClient => {
      sshClient
        .exec("spark-submit main.jar")
        .foreach(r =>
          println(
            s"""
            |Exit code: ${r.exitCode}
            |Stdout: ${r.stdOutAsString()}
            |Stderr: ${r.stdErrAsString()}
            |""".stripMargin,
          ),
        )
    }),
  )
