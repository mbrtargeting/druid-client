scalaVersion := "2.12.1"

crossScalaVersions := Seq("2.11.8", "2.12.1")

name := "druid-client"
organization := "eu.m6r"

publishMavenStyle := true
useGpg := true

xjcJvmOpts += "-Duser.language=en"
xjcCommandLine ++= Seq("-p", "eu.m6r.druid.client.models")

ghpages.settings

site.includeScaladoc()

val jacksonDependencyVersion = "2.8.6"

libraryDependencies ++= Seq(
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDependencyVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonDependencyVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonDependencyVersion,
  "org.eclipse.persistence" % "org.eclipse.persistence.moxy" % "2.6.0",
  "com.twitter" %% "util-zk" % "6.39.0",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "joda-time" % "joda-time" % "2.9.6",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

git.remoteRepo := "git@github.com:mbrtargeting/druid-client.git"

pomExtra := <url>https://github.com/mbrtargeting/druid-client</url>
    <licenses>
      <license>
        <name>MIT License</name>
        <url>http://www.opensource.org/licenses/mit-license.php</url>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:mbrtargeting/druid-client.git</url>
      <connection>scm:git:git@github.com:mbrtargeting/druid-client.git</connection>
    </scm>
    <developers>
      <developer>
        <id>gesundkrank</id>
        <name>Jan Graßegger</name>
        <email>jan@mbr-targeting.com</email>
        <url>https://github.com/gesundkrank</url>
      </developer>
    </developers>

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}
