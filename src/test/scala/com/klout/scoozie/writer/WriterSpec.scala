package com.klout.scoozie.writer

import java.io.File

import com.klout.scoozie.dsl._
import com.klout.scoozie.jobs.{ShellScript, ShellJob, MapReduceJob}
import com.klout.scoozie.writer.implicits._
import org.joda.time.{ DateTime, DateTimeZone }
import org.specs2.matcher.TryMatchers
import org.specs2.mutable.Specification

import scala.reflect.io.Path
import scala.util.Try

class WriterSpec extends Specification with TryMatchers {
    val resourcePath = "src/test/resources/"
    val testFolder = s"$resourcePath/writer-tests"

    TestFileSystemUtils.makeDirectory(resourcePath)
    TestFileSystemUtils.deleteRecursively(testFolder)
    TestFileSystemUtils.makeDirectory(testFolder)

    "Workflow writer" should {
        "write a workflow to the correct path" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val currentTestFolder = s"$testFolder/workflow-write-test"
            val workflowFilename = "workflow-test.xml"
            val workflowPath = s"$currentTestFolder/$workflowFilename"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = workflow.write(
                workflowPath,
                TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(currentTestFolder).get.head.getName must_== workflowFilename
        }

        "write a workflow job to the correct path" in {
            val workflow = Workflow(
              name = "test-workflow",
              end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val currentTestFolder = s"$testFolder/workflow-write-job-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = workflow.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(s"$currentTestFolder").get.map(_.getName).toSeq must contain("workflows", "job.properties")
            TestFileSystemUtils.ls(s"$currentTestFolder/workflows").get.map(_.getName).toSeq must contain(s"${workflow.name}.xml")
        }

        "write a workflow job with a shell action to the correct path" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (ShellJob("test-shell", Right(ShellScript("echo test"))) dependsOn Start)
            )

            val currentTestFolder = s"$testFolder/workflow-write-job-with-shell-action-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = workflow.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(s"$currentTestFolder").get.map(_.getName).toSeq must contain("workflows", "job.properties", "bin")
            TestFileSystemUtils.ls(s"$currentTestFolder/workflows").get.map(_.getName).toSeq must contain(s"test-workflow.xml")
            TestFileSystemUtils.ls(s"$currentTestFolder/bin").get.map(_.getName).toSeq must contain(s"test-shell.sh")
            TestFileSystemUtils
              .readTextFile(s"$currentTestFolder/workflows/test-workflow.xml")
              .map(xml =>
                xml.contains("<exec>test-shell.sh</exec>")
                && xml.contains("<file>${test_shell_path}#test-shell.sh</file>"))
              .get must_== true
            TestFileSystemUtils
              .readTextFile(s"$currentTestFolder/job.properties")
              .map(xml => xml.contains("test_shell_path=${rootFolder_path}/bin/test-shell.sh"))
              .get must_== true
        }

        "write a workflow job with multiple shell actions to the correct path" in {
            val scriptOne = ShellJob("test-shell", Right(ShellScript("echo test"))) dependsOn Start
            val scriptTwo = ShellJob("test-shell-2", Right(ShellScript("echo haha"))) dependsOn Start
            val scriptThree = ShellJob("test-shell-3", Right(ShellScript("echo asdfg"))) dependsOn scriptOne
            val scriptFour = ShellJob("test-shell-4", Right(ShellScript("echo testing2"))) dependsOn scriptTwo

            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn Seq(scriptFour, scriptThree)
            )

            val currentTestFolder = s"$testFolder/workflow-write-job-with-multiple-shell-actions-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = workflow.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(s"$currentTestFolder").get.map(_.getName).toSeq must contain("workflows", "job.properties", "bin")
            TestFileSystemUtils.ls(s"$currentTestFolder/workflows").get.map(_.getName).toSeq must contain(s"test-workflow.xml")

            TestFileSystemUtils.ls(s"$currentTestFolder/bin").get.map(_.getName).toSeq must
              contain(s"test-shell.sh", s"test-shell-2.sh", s"test-shell-3.sh", s"test-shell-4.sh")

            TestFileSystemUtils
              .readTextFile(s"$currentTestFolder/workflows/test-workflow.xml")
              .map(xml =>
                  xml.contains("<exec>test-shell.sh</exec>") && xml.contains("<file>${test_shell_path}#test-shell.sh</file>")
                  && xml.contains("<exec>test-shell-2.sh</exec>") && xml.contains("<file>${test_shell_2_path}#test-shell-2.sh</file>")
                  && xml.contains("<exec>test-shell-3.sh</exec>") && xml.contains("<file>${test_shell_3_path}#test-shell-3.sh</file>")
                  && xml.contains("<exec>test-shell-4.sh</exec>") && xml.contains("<file>${test_shell_4_path}#test-shell-4.sh</file>")
              )
              .get must_== true

            TestFileSystemUtils
              .readTextFile(s"$currentTestFolder/job.properties")
              .map(xml =>
                  xml.contains("test_shell_path=${rootFolder_path}/bin/test-shell.sh")
                    && xml.contains("test_shell_2_path=${rootFolder_path}/bin/test-shell-2.sh")
                    && xml.contains("test_shell_3_path=${rootFolder_path}/bin/test-shell-3.sh")
                    && xml.contains("test_shell_4_path=${rootFolder_path}/bin/test-shell-4.sh"))
              .get must_== true
        }
    }

    "Coordinator writer" should {
        val timezone = DateTimeZone.forID("Australia/Sydney")

        "write a coordinator to the correct path" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = timezone,
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = Some("/fake/path")
            )

            val currentTestFolder = s"$testFolder/coordinator-write-test"
            val coordinatorFilename = "coordinator-test.xml"
            val coordinatorPath = s"$currentTestFolder/$coordinatorFilename"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = coordinator.write(
                coordinatorPath,
                TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(currentTestFolder).get.head.getName must_== coordinatorFilename
        }

        "throw an exception if a workflow path is not specified and a write is attempted" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = None
            )

            val currentTestFolder = s"$testFolder/coordinator-write-error-test"
            val coordinatorFilename = "coordinator-test.xml"
            val coordinatorPath = s"$currentTestFolder/$coordinatorFilename"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            coordinator.write(
                coordinatorPath,
                TestFileSystemUtils) must throwA[AssertionError]
        }

        "write a coordinator job to the correct path" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = None
            )

            val currentTestFolder = s"$testFolder/coordinator-write-job-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = coordinator.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(s"$currentTestFolder").get.map(_.getName).toSeq must
                contain("workflows", "coordinators", "job.properties")
            TestFileSystemUtils.ls(s"$currentTestFolder/workflows").get.map(_.getName).toSeq must
                contain("test-workflow.xml")
            TestFileSystemUtils.ls(s"$currentTestFolder/coordinators").get.map(_.getName).toSeq must
                contain("test-coordinator.xml")
            TestFileSystemUtils
                .readTextFile(s"$currentTestFolder/coordinators/test-coordinator.xml")
                .map(xml => xml.contains("<app-path>${test_workflow_path}</app-path>"))
                .get must_== true
        }

        "write a coordinator job with a shell action to the correct path" in {
          val workflow = Workflow(
            name = "test-workflow",
            end = End dependsOn (ShellJob("test-shell", Right(ShellScript("echo test"))) dependsOn Start)
          )

          val coordinator = Coordinator(
            name = "test-coordinator",
            workflow = workflow,
            timezone = DateTimeZone.forID("Australia/Sydney"),
            start = DateTime.now().withZone(timezone),
            end = DateTime.now().plusDays(10).withZone(timezone),
            frequency = Days(24),
            configuration = Nil,
            workflowPath = None
          )

          val currentTestFolder = s"$testFolder/coordinator-write-job-with-shell-action-test"

          TestFileSystemUtils.makeDirectory(currentTestFolder)

          val result: Try[Unit] = coordinator.writeJob(
            path = currentTestFolder,
            fileSystemUtils = TestFileSystemUtils)

          result.isSuccess must_== true
          TestFileSystemUtils.ls(s"$currentTestFolder").get.map(_.getName).toSeq must
            contain("workflows", "coordinators", "job.properties", "bin")
          TestFileSystemUtils.ls(s"$currentTestFolder/workflows").get.map(_.getName).toSeq must
            contain("test-workflow.xml")
          TestFileSystemUtils.ls(s"$currentTestFolder/coordinators").get.map(_.getName).toSeq must
            contain("test-coordinator.xml")
          TestFileSystemUtils
            .readTextFile(s"$currentTestFolder/coordinators/test-coordinator.xml")
            .map(xml => xml.contains("<app-path>${test_workflow_path}</app-path>"))
            .get must_== true
          TestFileSystemUtils
            .readTextFile(s"$currentTestFolder/workflows/test-workflow.xml")
            .map(xml =>
              xml.contains("<exec>test-shell.sh</exec>")
                && xml.contains("<file>${test_shell_path}#test-shell.sh</file>"))
            .get must_== true
          TestFileSystemUtils
            .readTextFile(s"$currentTestFolder/job.properties")
            .map(xml => xml.contains("test_shell_path=${rootFolder_path}/bin/test-shell.sh"))
            .get must_== true
        }

        "throw an exception if the workflow path is specified and a write job is attempted" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = Some("/fake/path")
            )

            val currentTestFolder = s"$testFolder/coordinator-write-job-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            coordinator.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils) must throwA[AssertionError]
        }
    }

    "Bundle writer" should {
        val timezone = DateTimeZone.forID("Australia/Sydney")

        "write a bundle to the correct path" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = None
            )

            val bundle = Bundle(
                name = "test-bundle",
                kickoffTime = Right("${kickOffTime}"),
                coordinators = List(
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle1",
                        path = Some("${appPath}"),
                        coordinator = coordinator
                    )
                )
            )

            val currentTestFolder = s"$testFolder/bundle-write-test"
            val bundleFilename = "bundle-test.xml"
            val bundlePath = s"$currentTestFolder/$bundleFilename"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = bundle.write(
                bundlePath,
                TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(currentTestFolder).get.head.getName must_== bundleFilename
        }

        "throw an exception if the coordinator descriptor path is not specified and a write is attempted" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = None
            )

            val bundle = Bundle(
                name = "test-bundle",
                parameters = List("appPath" -> None, "appPath2" -> Some("hdfs://foo:9000/user/joe/job/job.properties")),
                kickoffTime = Right("${kickOffTime}"),
                coordinators = List(
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle1",
                        path = None,
                        coordinator = coordinator
                    )
                )
            )

            val currentTestFolder = s"$testFolder/bundle-write-error-test"
            val bundleFilename = "bundle-test.xml"
            val bundlePath = s"$currentTestFolder/$bundleFilename"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            bundle.write(
                path = bundlePath,
                fileSystemUtils = TestFileSystemUtils) must throwA[AssertionError]
        }

        "throw an exception if the coordinator descriptor path is specified and a write job is attempted" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = None
            )

            val bundle = Bundle(
                name = "test-bundle",
                parameters = List("appPath" -> None, "appPath2" -> Some("hdfs://foo:9000/user/joe/job/job.properties")),
                kickoffTime = Right("${kickOffTime}"),
                coordinators = List(
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle1",
                        path = Some("/fake/path"),
                        coordinator = coordinator
                    )
                )
            )

            val currentTestFolder = s"$testFolder/bundle-write-job-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            bundle.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils) must throwA[AssertionError]
        }

        "throw an exception if a workflow path is specified and a write job is attempted" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = Some("/fake/path")
            )

            val bundle = Bundle(
                name = "test-bundle",
                parameters = List("appPath" -> None, "appPath2" -> Some("hdfs://foo:9000/user/joe/job/job.properties")),
                kickoffTime = Right("${kickOffTime}"),
                coordinators = List(
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle1",
                        path = None,
                        coordinator = coordinator
                    )
                )
            )

            val currentTestFolder = s"$testFolder/bundle-write-job-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            bundle.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils) must throwA[AssertionError]
        }

        "write a bundle job with one coordinator and one workflow to the correct path" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = None
            )

            val bundle = Bundle(
                name = "test-bundle",
                kickoffTime = Right("${kickOffTime}"),
                coordinators = List(
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle1",
                        path = None,
                        coordinator = coordinator
                    )
                )
            )

            val currentTestFolder = s"$testFolder/bundle-write-job-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = bundle.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(s"$currentTestFolder").get.map(_.getName).toSeq must
                contain("workflows", "coordinators", "bundles", "job.properties")
            TestFileSystemUtils.ls(s"$currentTestFolder/workflows").get.map(_.getName).toSeq must
                contain(s"test-workflow.xml")
            TestFileSystemUtils.ls(s"$currentTestFolder/coordinators").get.map(_.getName).toSeq must
                contain("test-coordinator.xml")
            TestFileSystemUtils.ls(s"$currentTestFolder/bundles").get.map(_.getName).toSeq must contain("test-bundle.xml")
            TestFileSystemUtils
                .readTextFile(s"$currentTestFolder/coordinators/test-coordinator.xml")
                .map(xml => xml.contains("<app-path>${test_workflow_path}</app-path>"))
                .get must_== true
            TestFileSystemUtils
                .readTextFile(s"$currentTestFolder/bundles/test-bundle.xml")
                .map(xml => xml.contains("<app-path>${test_coordinator_path}</app-path>"))
                .get must_== true
        }

        "write a bundle job with one coordinator and one workflow with a shell action to the correct path" in {
          val workflow = Workflow(
            name = "test-workflow",
            end = End dependsOn (ShellJob("test-shell", Right(ShellScript("echo test"))) dependsOn Start)
          )

          val coordinator = Coordinator(
            name = "test-coordinator",
            workflow = workflow,
            timezone = DateTimeZone.forID("Australia/Sydney"),
            start = DateTime.now().withZone(timezone),
            end = DateTime.now().plusDays(10).withZone(timezone),
            frequency = Days(24),
            configuration = Nil,
            workflowPath = None
          )

          val bundle = Bundle(
            name = "test-bundle",
            kickoffTime = Right("${kickOffTime}"),
            coordinators = List(
              CoordinatorDescriptor(
                name = "coordJobFromBundle1",
                path = None,
                coordinator = coordinator
              )
            )
          )

          val currentTestFolder = s"$testFolder/bundle-write-job-with-shell-action-test"

          TestFileSystemUtils.makeDirectory(currentTestFolder)

          val result: Try[Unit] = bundle.writeJob(
            path = currentTestFolder,
            fileSystemUtils = TestFileSystemUtils)

          result.isSuccess must_== true
          TestFileSystemUtils.ls(s"$currentTestFolder").get.map(_.getName).toSeq must
            contain("workflows", "coordinators", "bundles", "job.properties")
          TestFileSystemUtils.ls(s"$currentTestFolder/workflows").get.map(_.getName).toSeq must
            contain(s"test-workflow.xml")
          TestFileSystemUtils.ls(s"$currentTestFolder/coordinators").get.map(_.getName).toSeq must
            contain("test-coordinator.xml")
          TestFileSystemUtils.ls(s"$currentTestFolder/bundles").get.map(_.getName).toSeq must contain("test-bundle.xml")
          TestFileSystemUtils
            .readTextFile(s"$currentTestFolder/coordinators/test-coordinator.xml")
            .map(xml => xml.contains("<app-path>${test_workflow_path}</app-path>"))
            .get must_== true
          TestFileSystemUtils
            .readTextFile(s"$currentTestFolder/bundles/test-bundle.xml")
            .map(xml => xml.contains("<app-path>${test_coordinator_path}</app-path>"))
            .get must_== true
          TestFileSystemUtils
            .readTextFile(s"$currentTestFolder/workflows/test-workflow.xml")
            .map(xml =>
              xml.contains("<exec>test-shell.sh</exec>")
                && xml.contains("<file>${test_shell_path}#test-shell.sh</file>"))
            .get must_== true
          TestFileSystemUtils
            .readTextFile(s"$currentTestFolder/job.properties")
            .map(xml => xml.contains("test_shell_path=${rootFolder_path}/bin/test-shell.sh"))
            .get must_== true
      }

        "write a bundle job with multiple coordinators and multiple workflow to the correct path" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val workflow2 = Workflow(
                name = "test-workflow-2",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = None
            )

            val coordinator2 = Coordinator(
                name = "test-coordinator-2",
                workflow = workflow2,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = None
            )

            val bundle = Bundle(
                name = "test-bundle",
                kickoffTime = Right("${kickOffTime}"),
                coordinators = List(
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle1",
                        path = None,
                        coordinator = coordinator
                    ),
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle2",
                        path = None,
                        coordinator = coordinator2
                    )
                )
            )

            val currentTestFolder = s"$testFolder/bundle-write-job-multiple-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = bundle.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(s"$currentTestFolder").get.map(_.getName).toSeq must
                contain("workflows", "coordinators", "bundles", "job.properties")
            TestFileSystemUtils.ls(s"$currentTestFolder/workflows").get.map(_.getName).toSeq must
                contain("test-workflow.xml", "test-workflow-2.xml")
            TestFileSystemUtils.ls(s"$currentTestFolder/coordinators").get.map(_.getName).toSeq must
                contain("test-coordinator.xml", "test-coordinator-2.xml")
            TestFileSystemUtils.ls(s"$currentTestFolder/bundles").get.map(_.getName).toSeq must
                contain("test-bundle.xml")
            TestFileSystemUtils
                .readTextFile(s"$currentTestFolder/coordinators/test-coordinator.xml")
                .map(xml => xml.contains("<app-path>${test_workflow_path}</app-path>"))
                .get must_== true
            TestFileSystemUtils
                .readTextFile(s"$currentTestFolder/coordinators/test-coordinator-2.xml")
                .map(xml => xml.contains("<app-path>${test_workflow_2_path}</app-path>"))
                .get must_== true
            TestFileSystemUtils
                .readTextFile(s"$currentTestFolder/bundles/test-bundle.xml")
                .map(xml =>
                    xml.contains("<app-path>${test_coordinator_path}</app-path>") &&
                        xml.contains("<app-path>${test_coordinator_2_path}</app-path>"))
                .get must_== true
        }

        "write a bundle job with multiple coordinator descriptors, one coordinator and one workflow to the correct path" in {
            val workflow = Workflow(
                name = "test-workflow",
                end = End dependsOn (MapReduceJob("first") dependsOn Start)
            )

            val coordinator = Coordinator(
                name = "test-coordinator",
                workflow = workflow,
                timezone = DateTimeZone.forID("Australia/Sydney"),
                start = DateTime.now().withZone(timezone),
                end = DateTime.now().plusDays(10).withZone(timezone),
                frequency = Days(24),
                configuration = Nil,
                workflowPath = None
            )

            val bundle = Bundle(
                name = "test-bundle",
                kickoffTime = Right("${kickOffTime}"),
                coordinators = List(
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle1",
                        path = None,
                        coordinator = coordinator
                    ),
                    CoordinatorDescriptor(
                        name = "coordJobFromBundle2",
                        path = None,
                        coordinator = coordinator
                    )
                )
            )

            val currentTestFolder = s"$testFolder/bundle-write-job-multiple-descriptors-test"

            TestFileSystemUtils.makeDirectory(currentTestFolder)

            val result: Try[Unit] = bundle.writeJob(
                path = currentTestFolder,
                fileSystemUtils = TestFileSystemUtils)

            result.isSuccess must_== true
            TestFileSystemUtils.ls(s"$currentTestFolder").get.map(_.getName).toSeq must
                contain("workflows", "coordinators", "bundles", "job.properties")
            TestFileSystemUtils.ls(s"$currentTestFolder/workflows").get.map(_.getName).toSeq must
                contain(s"test-workflow.xml")
            TestFileSystemUtils.ls(s"$currentTestFolder/coordinators").get.map(_.getName).toSeq must
                contain("test-coordinator.xml")
            TestFileSystemUtils.ls(s"$currentTestFolder/bundles").get.map(_.getName).toSeq must contain("test-bundle.xml")
            TestFileSystemUtils
                .readTextFile(s"$currentTestFolder/coordinators/test-coordinator.xml")
                .map(xml => xml.contains("<app-path>${test_workflow_path}</app-path>"))
                .get must_== true
            TestFileSystemUtils
                .readTextFile(s"$currentTestFolder/bundles/test-bundle.xml")
                .map(xml => "<app-path>\\$\\{test_coordinator_path\\}<\\/app-path>".r.findAllIn(xml).length)
                .get must_== 2
        }
    }
}

trait TestFileSystemUtils extends LocalFileSystemUtils {
    def ls(path: String): Try[Array[File]] = {
        Try(new File(path).listFiles())
    }

    def readTextFile(path: String): Try[String] = {
        Try(scala.io.Source.fromFile(path).mkString)
    }

    def deleteRecursively(path: String) = {
        Try(Path(path).deleteRecursively())
    }
}

object TestFileSystemUtils extends TestFileSystemUtils