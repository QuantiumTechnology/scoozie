package com.klout.scoozie.runner

import com.klout.scoozie.dsl._
import com.klout.scoozie.utils.OozieClientLike
import org.apache.hadoop.fs.Path
import org.apache.oozie.LocalOozieClient
import org.apache.oozie.client.{Job, WorkflowJob}
import org.joda.time.{DateTime, DateTimeZone}
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll

import scala.concurrent.Await

class ScoozieAppSpec extends Specification with BeforeAfterAll with TestOozieClientProvider with TestHdfsProvider {
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

      result.getStatus must_== WorkflowJob.Status.SUCCEEDED
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
      val result = Await.result(scoozieApp.executionResult, 30.second)

      result.getStatus must_== Job.Status.SUCCEEDED
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
      val result = Await.result(scoozieApp.executionResult, 30.second)

      result.getStatus must_== Job.Status.SUCCEEDED
    }.pendingUntilFixed("Currently can't because Oozie... LocalTest doesn't support bundles")
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
    val end = End dependsOn Start
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






