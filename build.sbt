import Dependencies._

name := "kafka-streams-scala"

organization := "com.openshine"

scalaVersion := Versions.Scala_2_12_Version

crossScalaVersions := Versions.CrossScalaVersions

scalacOptions := Seq("-Xexperimental",
                     "-unchecked",
                     "-deprecation",
                     "-Ywarn-unused-import")

parallelExecution in Test := false

libraryDependencies ++= Seq(
  kafkaStreams excludeAll (ExclusionRule("org.slf4j", "slf4j-log4j12"), ExclusionRule(
    "org.apache.zookeeper",
    "zookeeper"
  )),
  scalaLogging % "test",
  logback % "test",
  kafka % "test" excludeAll (ExclusionRule("org.slf4j", "slf4j-log4j12"), ExclusionRule(
    "org.apache.zookeeper",
    "zookeeper"
  )),
  curator % "test",
  minitest % "test",
  minitestLaws % "test",
  algebird % "test",
  chill % "test"
)

testFrameworks += new TestFramework("minitest.runner.Framework")

licenses := Seq(
  "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")
)

developers := List(
  Developer("ssaavedra",
            "Santiago Saavedra",
            "@ssaavedra",
            url("https://github.com/ssaavedra"))
)

organizationName := "openshine"

organizationHomepage := Some(url("http://openshine.com/"))

homepage := scmInfo.value map (_.browseUrl)

scmInfo := Some(
  ScmInfo(url("https://github.com/openshine/kafka-streams-scala"),
          "git@github.com:openshine/kafka-streams-scala.git")
)

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishArtifact in Test := false

enablePlugins(GitVersioning)

git.baseVersion := "0.1.git"
git.useGitDescribe := true

bintrayOrganization := Some("openshine")

bintrayRepository := "maven"
