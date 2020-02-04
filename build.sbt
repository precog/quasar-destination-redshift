import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-destination-redshift"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-destination-redshift"),
  "scm:git@github.com:slamdata/quasar-destination-redshift.git"))

val DoobieVersion = "0.7.0"
val ArgonautVersion = "6.2.3"
val AsyncBlobstoreVersion = "2.0.1-898e40c"
val AwsSdkVersion = "2.9.1"
val Fs2Version = "2.1.0"
val SpecsVersion = "4.8.3"

val RedshiftRepository = "redshift" at "https://s3.amazonaws.com/redshift-maven-repository/release"

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
    resolvers += RedshiftRepository,
    quasarPluginName := "redshift",
    quasarPluginQuasarVersion := QuasarVersion,
    quasarPluginDestinationFqcn := Some("quasar.destination.redshift.RedshiftDestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "org.slf4s" %% "slf4s-api" % "1.7.25",
      "io.argonaut"  %% "argonaut" % ArgonautVersion,
      "co.fs2" %% "fs2-core" % Fs2Version,
      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion,
      "com.slamdata" %% "async-blobstore-core" % AsyncBlobstoreVersion,
      "com.slamdata" %% "async-blobstore-s3" % AsyncBlobstoreVersion,
      "com.slamdata" %% "fs2-gzip" % "1.1.1",
      "com.amazon.redshift" % "redshift-jdbc4-no-awssdk" % "1.2.10.1009"),
    quasarPluginExtraResolvers := Seq(coursier.MavenRepository("https://s3.amazonaws.com/redshift-maven-repository/release")),
    performMavenCentralSync := false,
    publishAsOSSProject := false,
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % SpecsVersion % Test,
      "com.slamdata" %% "quasar-foundation" % QuasarVersion,
      "com.slamdata" %% "quasar-foundation" % QuasarVersion % Test classifier "tests",
      "org.specs2" %% "specs2-scalacheck" % SpecsVersion % Test))
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
