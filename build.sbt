javacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-target", "1.7",
      "-source", "1.7",
      "-Xlint:deprecation"
    )

libraryDependencies ++= Seq(
	"org.mongodb" % "mongo-java-driver" % "2.11.3",
	"org.slf4j" % "slf4j-api" % "1.6.6",
	"junit" % "junit" % "4.10" % "test"
)