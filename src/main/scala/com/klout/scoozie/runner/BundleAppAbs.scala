package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Bundle
import com.klout.scoozie.utils.ExecutionUtils
import org.apache.oozie.client.OozieClient

import scala.concurrent.Future
import scalaxb.CanWriteXML

abstract class BundleAppAbs[B: CanWriteXML,C: CanWriteXML, W: CanWriteXML] extends ScoozieApp {
  val bundle: Bundle[B, C, W]

  type Job = org.apache.oozie.client.Job

  import com.klout.scoozie.writer.implicits._
  override lazy val writeResult = bundle.writeJob(appPath, jobProperties, fileSystemUtils, postProcessing)

  override lazy val executionResult: Future[Job] = for{
    _ <- Future.fromTry(writeResult)
    _ <- Future(logWriteResult())
    job <- ExecutionUtils.run[OozieClient, Job](oozieClient, bundle.getJobProperties(appPath, jobProperties))
  } yield job
}
