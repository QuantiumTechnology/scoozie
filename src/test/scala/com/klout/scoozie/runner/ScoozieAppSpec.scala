package com.klout.scoozie.runner

import com.klout.scoozie.dsl._
import com.klout.scoozie.jobs.{ShellJob, ShellScript}
import com.klout.scoozie.utils.OozieClientLike
import com.klout.scoozie.writer.FileSystemUtils
import org.apache.hadoop.fs.Path
import org.apache.oozie.LocalOozieClient
import org.apache.oozie.client.{Job, WorkflowJob}
import org.joda.time.{DateTime, DateTimeZone}
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll

import scala.concurrent.Await
import scala.util.{Failure, Try}

class ScoozieAppSpec extends Specification with BeforeAfterAll with TestHdfsProvider with TestOozieClientProvider {

  "Scoozie Application" should {
    "run a workflow application successfully" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppWorkflow")

      val scoozieApp = new TestWorkflowApp(
        workflow = Fixtures.workflow("test-workflow"),
        oozieClient = oozieClient,
        appPath = appPath.toString,
        fileSystemUtils = new HdfsFilesystemUtils(fs)
      )

      scoozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Await.result(scoozieApp.executionResult, 30.second)

      result.getStatus must_== WorkflowJob.Status.RUNNING
    }

    "run a coordinator application successfully" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppCoordinator")

      val scoozieApp = new TestCoordinatorApp(
        coordinator = Fixtures.coordinator("test-coordinator"),
        oozieClient = oozieCoordClient,
        appPath = appPath.toString,
        fileSystemUtils = new HdfsFilesystemUtils(fs)
      )

      scoozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Await.result(scoozieApp.executionResult, 90.second)

      result.getStatus must_== Job.Status.RUNNING
    }

    "run a bundle application successfully" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppBundle")

      val scoozieApp = new TestBundleApp(
        bundle = Fixtures.bundle("test-bundle"),
        oozieClient = oozieCoordClient,
        appPath = appPath.toString,
        fileSystemUtils = new HdfsFilesystemUtils(fs)
      )

      scoozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Await.result(scoozieApp.executionResult, 90.seconds)

      result.getStatus must_== Job.Status.RUNNING
    }.pendingUntilFixed("Currently can't because Oozie... LocalTest doesn't support bundles")

    "should catch unexpected errors" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppWorkflow")

      val scoozieApp = new TestWorkflowApp(
        workflow = Fixtures.workflowWithError("test-workflow"),
        oozieClient = oozieClient,
        appPath = appPath.toString,
        fileSystemUtils = new HdfsFilesystemUtils(fs)
      )

      scoozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Try(Await.result(scoozieApp.executionResult, 30.second))

      result.isFailure must_== true
    }

    "should not submit job if writing has failed" in {
      val appPath: Path = new Path(fs.getHomeDirectory, "testAppWorkflow")

      val fileSystemUtils = new FileSystemUtils {
        override def writeTextFile(path: String, text: String): Try[Unit] = Failure(new Exception("a write error"))

        override def makeDirectory(path: String): Try[Unit] = Failure(new Exception("a write error"))
      }

      val scoozieApp = new TestWorkflowApp(
        workflow = Fixtures.workflow("test-workflow"),
        oozieClient = oozieClient,
        appPath = appPath.toString,
        fileSystemUtils = fileSystemUtils
      )

      scoozieApp.main(Array.empty)

      import scala.concurrent.duration._
      val result = Try(Await.result(scoozieApp.executionResult, 30.second))

      result.isFailure must_== true
    }
  }
}

object OozieClientLike {
  implicit object OozieClientLikeLocalCoord extends OozieClientLike[LocalOozieClient, Job, Job.Status] {
    val running: Job.Status = Job.Status.RUNNING
    val prep: Job.Status = Job.Status.PREP
    val succeeded: Job.Status = Job.Status.SUCCEEDED
    def getJobInfo(oozieClient: LocalOozieClient, jobId: String): Job = oozieClient.getCoordJobInfo(jobId)
    def getJobStatus(oozieClient: LocalOozieClient, jobId: String): Job.Status = getJobInfo(oozieClient, jobId).getStatus
  }
}

object Fixtures {
  def workflow(name: String) = {
    val end = End dependsOn ShellJob("error", Right(ShellScript("echo test"))).dependsOn(Start)
    Workflow(name, end)
  }

  def workflowWithError(name: String) = {
    val end = End dependsOn ShellJob("error", Right(ShellScript("echo test"))).dependsOn(Start)
    Workflow(name, end)
  }

  def coordinator(name: String) = {
    Coordinator(
      name = name,
      workflow = workflow(s"${name}_workflow"),
      timezone = DateTimeZone.forID("GMT"),
      start = DateTime.now().toDateTime(DateTimeZone.forID("GMT")),
      end = DateTime.now().plusHours(1).toDateTime(DateTimeZone.forID("GMT")),
      frequency = Hours(1),
      configuration = Nil,
      workflowPath = None
    )
  }

  def bundle(name: String) = {
    Bundle(
      name = name,
      coordinators = List(CoordinatorDescriptor("my-coord", coordinator("my-coord"))),
      kickoffTime = Left[DateTime, String](DateTime.now())
    )
  }
}