package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Workflow
import org.apache.oozie.client.WorkflowJob

import scala.concurrent.Future
import scalaxb.CanWriteXML

abstract class WorkflowAppAbs[W: CanWriteXML] extends ScoozieApp {
  val workflow: Workflow[W]

  type Job = WorkflowJob
  type JobStatus = WorkflowJob.Status

  import com.klout.scoozie.writer.implicits._
  val writeResult = workflow.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)
  logWriteResult()

  val executionResult: Future[Job]
}
