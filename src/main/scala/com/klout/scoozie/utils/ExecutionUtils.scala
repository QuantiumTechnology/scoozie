package com.klout.scoozie.utils

import retry._
import org.apache.oozie.client._

import scala.concurrent.Future
import scala.concurrent.duration._

object ExecutionUtils {
  def toProperty(propertyString: String): (String, String) = {
    val property: Array[String] = propertyString.split("=")
    if (property.length != 2) throw new RuntimeException("error: property file not correctly formatted")
    else property(0) -> property(1)
  }

  def removeCoordinatorJob(appName: String, oozieClient: OozieClient): Unit = {
    import scala.collection.JavaConversions._
    val coordJobsToRemove = oozieClient.getCoordJobsInfo(s"NAME=$appName", 1, 100).filter{
      cj => cj.getAppName == appName && cj.getStatus == Job.Status.RUNNING
    }.map(_.getId).toSeq

    coordJobsToRemove.foreach(oozieClient.kill)
  }

  def run[T <: OozieClient, K](oozieClient: T, properties: Map[String, String])
                              (implicit ev: OozieClientLike[T, K]): Future[K] ={
    val conf = oozieClient.createConfiguration()
    properties.foreach { case (key, value) => conf.setProperty(key, value) }

    implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global

    // Rerun policies
    val retry = Backoff(5, 500.millis)

    // Rerun success condition
    val startJobSuccess = Success[String](!_.isEmpty)

    def startJob: Future[String] = retry(() => Future({
      oozieClient.run(conf)
    }))(startJobSuccess, executionContext)

    for {
      _ <- Future(println("Starting Execution"))
      jobId <- startJob
      _ <- Future(println(s"Started job: $jobId"))
      job <- Future(ev.getJobInfo(oozieClient, jobId))
    } yield job
  }
}

trait OozieClientLike[Client, Job] {
  def getJobInfo(oozieClient: Client, jobId: String): Job
}

object OozieClientLike {
  implicit object OozieClientLikeCoord extends OozieClientLike[OozieClient, Job] {
    def getJobInfo(oozieClient: OozieClient, jobId: String): Job = oozieClient.getCoordJobInfo(jobId)
  }

  implicit object OozieClientLikeWorkflow extends OozieClientLike[OozieClient, WorkflowJob] {
    def getJobInfo(oozieClient: OozieClient, id: String): WorkflowJob = oozieClient.getJobInfo(id)
  }
}