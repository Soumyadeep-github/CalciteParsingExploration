ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"



lazy val root = (project in file("."))
  .settings(
    name := "Calcite-Exploration-fresh",
    libraryDependencies ++= Seq(
      "org.apache.calcite" % "calcite-babel" % "1.34.0",
      "org.apache.calcite" % "calcite-core" % "1.34.0",
      "org.freemarker" % "freemarker" % "2.3.32",
      "org.apache.beam" % "beam-sdks-java-extensions-sql" % "2.46.0",
      "org.apache.beam" % "beam-sdks-java-core" % "2.46.0",
      "org.apache.beam" % "beam-runners-direct-java" % "2.46.0",
      "org.apache.velocity" % "velocity-engine-core" % "2.3",
      "org.apache.velocity.tools" % "velocity-tools-generic" % "3.1"
    )
  )