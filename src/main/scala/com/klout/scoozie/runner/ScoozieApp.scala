package com.klout.scoozie.runner

import com.klout.scoozie.utils.ExecutionUtils
import com.klout.scoozie.writer.{FileSystemUtils, XmlPostProcessing}
import org.apache.oozie.client.OozieClient

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

abstract class ScoozieApp
    extends App {

  val properties: Option[Map[String, String]]
  val appPath: String
  def oozieClient: OozieClient
  val fileSystemUtils: FileSystemUtils
  val postProcessing: XmlPostProcessing

  val usage = "java -cp <...> com.your.scoozie.app.ObjectName -todayString=foo -yesterdayString=foo ..."

  lazy val argumentProperties: Option[Map[String, String]] = {
    if (args nonEmpty) {
      Some(args.map(arg => {
        if (arg(0) != '-') throw new RuntimeException("error: usage is " + usage)
        ExecutionUtils.toProperty(arg.tail)
      }).toMap)
    } else None
  }

  lazy val jobProperties = (argumentProperties ++ properties).reduceOption(_ ++ _)

  type Job
  type JobStatus

  val writeResult: Try[Unit]

  implicit val executionContext: ExecutionContext
  val executionResult: Future[Job]

  def logWriteResult() = writeResult match {
    case Success(_) => println("Successfully wrote application.")
    case Failure(throwable) =>
      println(throwable.getMessage)
      throwable.printStackTrace()
  }
}