package com.klout.scoozie.runner

import com.klout.scoozie.ScoozieConfig
import com.klout.scoozie.dsl.Coordinator
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}
import scalaxb.CanWriteXML

class TestCoordinatorApp[C: CanWriteXML, W: CanWriteXML](override val coordinator: Coordinator[C, W],
                                                         override val oozieClient: OozieClient,
                                                         override val appPath: String,
                                                         override val fileSystemUtils: FileSystemUtils,
                                                         override val properties: Option[Map[String, String]] = None,
                                                         override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
  extends CoordinatorAppAbs[C, W] {

  implicit override val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global

  executionResult.onComplete{
    case Success(_) => println(ScoozieConfig.successMessage)
    case Failure(e) => println(s"Application failed with the following error: ${e.getMessage}")
  }
}


