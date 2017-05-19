package com.klout.scoozie.runner

import com.klout.scoozie.ScoozieConfig
import com.klout.scoozie.dsl.Bundle
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}
import scalaxb.CanWriteXML

class TestBundleApp[B: CanWriteXML, C: CanWriteXML, W: CanWriteXML](override val bundle: Bundle[B, C, W],
                                                                    override val oozieClient: OozieClient,
                                                                    override val appPath: String,
                                                                    override val fileSystemUtils: FileSystemUtils,
                                                                    override val properties: Option[Map[String, String]] = None,
                                                                    override val postProcessing: XmlPostProcessing = XmlPostProcessing.Default)
  extends BundleAppAbs[B, C, W] {

  import ExecutionContext.Implicits.global
  executionResult.onComplete{
    case Success(_) => println(ScoozieConfig.successMessage)
    case Failure(e) => println(s"Application failed with the following error: ${e.getMessage}")
  }
}