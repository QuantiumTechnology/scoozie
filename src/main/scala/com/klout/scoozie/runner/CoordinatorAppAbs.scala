package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Coordinator

import scala.concurrent.Future
import scalaxb.CanWriteXML

abstract class CoordinatorAppAbs[C: CanWriteXML, W: CanWriteXML] extends ScoozieApp {
  val coordinator: Coordinator[C, W]

  type Job = org.apache.oozie.client.Job
  type JobStatus = org.apache.oozie.client.Job.Status

  import com.klout.scoozie.writer.implicits._

  lazy val writeResult = coordinator.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)

  logWriteResult()

  val executionResult: Future[Job]
}
