import scala.scalanative.build.*

val scala3Version = "3.7.4"

ThisBuild / scalaVersion := scala3Version
ThisBuild / organization := "io.github.nicolasfara"
ThisBuild / homepage := Some(
  url(
    "https://github.com/nicolasfara/Template-for-Scala-Multiplatform-Projects"
  )
)
ThisBuild / licenses := List(
  "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")
)
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / developers := List(
  Developer(
    "nicolasfara",
    "Nicolas Farabegoli",
    "nicolas.farabegoli@gmail.com",
    url("https://nicolasfarabegoli.it")
  )
)
ThisBuild / scalacOptions ++= Seq(
  "-Werror",
  "-Wunused:all",
  "-Wvalue-discard",
  "-Wnonunit-statement",
  "-Yexplicit-nulls",
  "-Wsafe-init",
  "-Ycheck-reentrant",
  "-Xcheck-macros",
  "-rewrite",
  "-indent",
  "-unchecked",
  "-explain",
  "-feature",
  "-language:strictEquality",
  "-language:implicitConversions"
)
ThisBuild / coverageEnabled := true
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / wartremoverErrors ++= Warts.allBut(Wart.Any)
ThisBuild / libraryDependencies ++= Seq(
  "org.scalatest" %%% "scalatest" % "3.2.19" % Test
)

lazy val root = crossProject(JSPlatform, JVMPlatform, NativePlatform)
  .crossType(CrossType.Pure)
  .in(file("."))
  .configs()
  .nativeSettings(
    nativeConfig ~= {
      _.withLTO(LTO.default)
        .withMode(Mode.releaseSize)
        .withGC(GC.immix)
    }
  )
  .jsSettings(
    libraryDependencies += "org.scala-js" %%% "scalajs-dom" % "2.8.0",
    scalaJSUseMainModuleInitializer := true,
    scalaJSLinkerConfig ~= { _.withOptimizer(true) }
  )
  .settings(
    name := "Template-for-Scala-Multiplatform-Projects",
  )
