package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Coordinator
import com.klout.scoozie.utils.ExecutionUtils
import org.apache.oozie.client.OozieClient

import scala.concurrent.{ExecutionContext, Future}
import scalaxb.CanWriteXML

abstract class CoordinatorAppAbs[C: CanWriteXML, W: CanWriteXML] extends ScoozieApp {
  val coordinator: Coordinator[C, W]

  type Job = org.apache.oozie.client.Job
  type JobStatus = org.apache.oozie.client.Job.Status

  import com.klout.scoozie.writer.implicits._
  lazy val writeResult = coordinator.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)

  import ExecutionContext.Implicits.global
  override val executionResult: Future[Job] = for {
    _ <- Future.fromTry(writeResult)
    _ <- Future(logWriteResult())
    job <- ExecutionUtils.run[OozieClient, Job, JobStatus](oozieClient, coordinator.getJobProperties(appPath, jobProperties))
  } yield job
}
