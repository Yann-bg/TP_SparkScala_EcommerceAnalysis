name := "EcommerceAnalysis"
version := "0.1"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.knowm.xchart" % "xchart" % "3.8.2" exclude("de.erichseifert.vectorgraphics2d", "VectorGraphics2D"),
  "de.erichseifert.vectorgraphics2d" % "VectorGraphics2D" % "0.13"
)