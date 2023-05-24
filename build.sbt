ThisBuild / scalaVersion     := "2.13.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"
ThisBuild / scalacOptions  := {
  Seq("-feature", "-unchecked", "-deprecation") ++ (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) =>   Seq("-no-indent")
    case _ => Seq("-Ymacro-annotations", "-Ywarn-unused", "-Wunused:imports", "-Wvalue-discard", "-Xsource:3")
  })
}

lazy val root = (project in file("."))
  .settings(
    name := "zio-redis-rezilience",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.13",
      "dev.zio" %% "zio-test" % "2.0.13" % Test,
      "nl.vroste" %% "rezilience" % "0.9.2",
      "dev.zio" %% "zio-redis" % "0.2.0",
      "dev.zio" %% "zio-schema-protobuf" % "0.4.11",
      "dev.zio" %% "zio-schema-json" % "0.4.11",
      "dev.zio" %% "zio-schema-derivation" % "0.4.11",
      "dev.zio" %% "zio-redis-embedded" % "0.1.0"
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
