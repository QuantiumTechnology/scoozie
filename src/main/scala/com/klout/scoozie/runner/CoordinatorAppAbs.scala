package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Coordinator
import com.klout.scoozie.utils.ExecutionUtils
import org.apache.oozie.client.OozieClient

import scala.concurrent.Future
import scalaxb.CanWriteXML

abstract class CoordinatorAppAbs[C: CanWriteXML, W: CanWriteXML] extends ScoozieApp {
  val coordinator: Coordinator[C, W]

  type Job = org.apache.oozie.client.Job

  import com.klout.scoozie.writer.implicits._
  override lazy val writeResult = coordinator.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)

  override lazy val executionResult: Future[Job] = for{
    _ <- Future.fromTry(writeResult)
    _ <- Future(logWriteResult())
    job <- ExecutionUtils.run[OozieClient, Job](oozieClient, coordinator.getJobProperties(appPath, jobProperties))
  } yield job
}
