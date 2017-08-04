import sbt.Keys._
import sbt._

object Dependencies {
  val sparkCore = appendExcludes("org.apache.spark" %% "spark-core" % "2.0.1" % "provided")
  val sparkSql = "org.apache.spark" %% "spark-sql" % "2.0.1" % "provided"
  val scopt = "com.github.scopt" %% "scopt" % "3.5.0"

  val transformExamplesDependencies = Seq(sparkCore, sparkSql)


  def appendExcludes(module: ModuleID): ModuleID = {
    module.exclude("org.mortbay.jetty", "servlet-api").
      exclude("commons-beanutils", "commons-beanutils-core").
      exclude("commons-collections", "commons-collections").
      exclude("commons-logging", "commons-logging").
      exclude("com.esotericsoftware.minlog", "minlog")
  }
}