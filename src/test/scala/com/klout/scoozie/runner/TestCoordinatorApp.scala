package com.klout.scoozie.runner

import com.klout.scoozie.dsl.Coordinator
import com.klout.scoozie.utils.ExecutionUtils
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scala.concurrent.Future
import scalaxb.CanWriteXML

class TestCoordinatorApp[C: CanWriteXML, W: CanWriteXML](override val coordinator: Coordinator[C, W],
                                                         override val oozieClient: OozieClient,
                                                         override val appPath: String,
                                                         override val fileSystemUtils: FileSystemUtils,
                                                         override val properties: Option[Map[String, String]] = None,
                                                         override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
  extends CoordinatorAppAbs[C, W] {

  import com.klout.scoozie.writer.implicits._

  ExecutionUtils.removeCoordinatorJob(coordinator.name, oozieClient)

  override val executionResult: Future[Job] =
    ExecutionUtils.run[OozieClient, Job, JobStatus](oozieClient, coordinator.getJobProperties(appPath, jobProperties))
}


