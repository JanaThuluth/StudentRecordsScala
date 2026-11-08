
scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "StudentRecordsScala",
    libraryDependencies ++= Seq(
      "org.mongodb" % "mongodb-driver-sync" % "5.6.3",
      "org.apache.spark" %% "spark-sql" % "4.2.0"
    )
  )
