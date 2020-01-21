import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-destination-redshift"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-destination-redshift"),
  "scm:git@github.com:slamdata/quasar-destination-redshift.git"))

val ArgonautVersion = "6.2.3"
val AsyncBlobstoreVersion = "1.1.0-c5fa45c"
val AwsSdkVersion = "2.9.1"
val AwsV1SdkVersion = "1.11.634"
val Fs2Version = "1.0.5"
val SpecsVersion = "4.8.3"

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val QuasarVersion = IO.read(file("./quasar-version")).trim

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-destination-redshift")
  .settings(
    quasarPluginName := "redshift",
    quasarPluginQuasarVersion := QuasarVersion,
    quasarPluginDestinationFqcn := Some("quasar.destination.redshift.RedshiftDestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "io.argonaut"  %% "argonaut" % ArgonautVersion,
      "co.fs2" %% "fs2-core" % Fs2Version,
      "com.slamdata" %% "async-blobstore-core" % AsyncBlobstoreVersion,
      "com.slamdata" %% "async-blobstore-s3" % AsyncBlobstoreVersion),

    performMavenCentralSync := false,
    publishAsOSSProject := false)
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
