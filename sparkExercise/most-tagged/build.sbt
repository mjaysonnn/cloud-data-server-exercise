version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
	  "tagCounter.jar"
}
