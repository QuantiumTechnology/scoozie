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

  def run[T <: OozieClient, K, J](oozieClient: T, properties: Map[String, String])
                                 (implicit ev: OozieClientLike[T, K, J]): Future[K] ={
    val conf = oozieClient.createConfiguration()
    properties.foreach { case (key, value) => conf.setProperty(key, value) }

    implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global

    // Rerun policies
    val retry = Backoff(5, 500.millis)
    val retryForever = Pause.forever(1.second)

    // Rerun success condition
    val startJobSuccess = Success[String](!_.isEmpty)

    def startJob: Future[String] = retry(() => Future({
      oozieClient.run(conf)
    }))(startJobSuccess, executionContext)


    val retryJobStatusSuccess: Success[J] = Success[J](status => !(status == ev.prep))

    // This polls the oozie client and waits for the predicate above to be met
    // In earlier versions this would poll until the job was successful, now it polls until a job is running
    def retryJobStatus(id: String): Future[J] = retryForever(() =>
      Future({
        ev.getJobStatus(oozieClient, id)
      })
    )(retryJobStatusSuccess, executionContext)

    for {
      _ <- Future(println("Starting Execution"))
      jobId <- startJob
      _ <- Future(println(s"Started job: $jobId"))
      status <- retryJobStatus(jobId)
      _ <- Future(println(s"JOB: $jobId $status"))
      job <- Future(ev.getJobInfo(oozieClient, jobId))
      _ <- if (!(status == ev.succeeded || status == ev.running)) Future.failed(new Exception(s"The job was not successful. Completed with status: $status"))
           else Future.successful(Unit)
    } yield job
  }
}

trait OozieClientLike[Client, Job, JobStatus] {
  val running: JobStatus
  val prep: JobStatus
  val succeeded: JobStatus
  def getJobInfo(oozieClient: Client, jobId: String): Job
  def getJobStatus(oozieClient: Client, jobId: String): JobStatus
}

object OozieClientLike {
  implicit object OozieClientLikeCoord extends OozieClientLike[OozieClient, Job, Job.Status] {
    val running: Job.Status = Job.Status.RUNNING
    val prep: Job.Status = Job.Status.PREP
    val succeeded: Job.Status = Job.Status.SUCCEEDED
    def getJobInfo(oozieClient: OozieClient, jobId: String): Job = oozieClient.getCoordJobInfo(jobId)
    def getJobStatus(oozieClient: OozieClient, jobId: String): Job.Status = getJobInfo(oozieClient, jobId).getStatus
  }

  implicit object OozieClientLikeWorkflow extends OozieClientLike[OozieClient, WorkflowJob, WorkflowJob.Status] {
    val running: WorkflowJob.Status = WorkflowJob.Status.RUNNING
    val prep: WorkflowJob.Status = WorkflowJob.Status.PREP
    val succeeded: WorkflowJob.Status = WorkflowJob.Status.SUCCEEDED
    def getJobInfo(oozieClient: OozieClient, id: String): WorkflowJob = oozieClient.getJobInfo(id)
    def getJobStatus(oozieClient: OozieClient, id: String): WorkflowJob.Status = getJobInfo(oozieClient, id).getStatus
  }
}