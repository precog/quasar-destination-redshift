import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar-destination-redshift"

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-destination-redshift"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-destination-redshift"),
  "scm:git@github.com:precog/quasar-destination-redshift.git"))

val DoobieVersion = "0.7.0"
val ArgonautVersion = "6.2.3"
val AwsSdkVersion = "2.9.1"
val Fs2Version = "2.1.0"
val SpecsVersion = "4.8.3"

val RedshiftRepository = "redshift" at "https://s3.amazonaws.com/redshift-maven-repository/release"

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-destination-redshift")
  .settings(
    resolvers += RedshiftRepository,
    quasarPluginName := "redshift",
    quasarPluginQuasarVersion := managedVersions.value("precog-quasar"),
    quasarPluginDestinationFqcn := Some("quasar.destination.redshift.RedshiftDestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "org.slf4s" %% "slf4s-api" % "1.7.25",
      "io.argonaut"  %% "argonaut" % ArgonautVersion,
      "co.fs2" %% "fs2-core" % Fs2Version,
      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion,
      "com.precog" %% "async-blobstore-core" % managedVersions.value("precog-async-blobstore"),
      "com.precog" %% "async-blobstore-s3" % managedVersions.value("precog-async-blobstore"),
      "com.amazon.redshift" % "redshift-jdbc4-no-awssdk" % "1.2.10.1009"),
    quasarPluginExtraResolvers := Seq(coursier.MavenRepository("https://s3.amazonaws.com/redshift-maven-repository/release")),
    performMavenCentralSync := false,
    publishAsOSSProject := false,
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % SpecsVersion % Test,
      "com.precog" %% "quasar-foundation" % managedVersions.value("precog-quasar"),
      "com.precog" %% "quasar-foundation" % managedVersions.value("precog-quasar") % Test classifier "tests",
      "org.specs2" %% "specs2-scalacheck" % SpecsVersion % Test))
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
