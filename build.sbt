name := "workbench-scala"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.tupol"      %% "scala-utils-core" % "1.0.0-RC01",
  "org.scalatest"  %% "scalatest"        % "3.0.8",
  "org.scalacheck" %% "scalacheck"       % "1.14.1"
)
