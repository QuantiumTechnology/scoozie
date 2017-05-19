package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Workflow
import com.klout.scoozie.utils.ExecutionUtils
import org.apache.oozie.client.{OozieClient, WorkflowJob}

import scala.concurrent.{ExecutionContext, Future}
import scalaxb.CanWriteXML

abstract class WorkflowAppAbs[W: CanWriteXML] extends ScoozieApp {
  val workflow: Workflow[W]

  type Job = WorkflowJob
  type JobStatus = WorkflowJob.Status

  import com.klout.scoozie.writer.implicits._
  lazy val writeResult = workflow.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)

  import ExecutionContext.Implicits.global
  override lazy val executionResult: Future[Job] = for {
    _ <- Future.fromTry(writeResult)
    _ <- Future(logWriteResult())
    job <- ExecutionUtils.run[OozieClient, Job, JobStatus](oozieClient, workflow.getJobProperties(appPath, jobProperties))
  } yield job
}
