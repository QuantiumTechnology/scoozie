scoozie
=======

Scala DSL on top of Oozie XML

Why?
----

Scoozie is designed to solve problems concerning developer productivity when creating and running oozie workflow jobs.


Problems that Scoozie solves:
----------------------------

1. Job hierarchy and re-use: No more copy/pasting XML to create a new job
2. Type and syntax checking: No more errors from typos in XML
3. Safety / Verification: Scala provides a type-safe environment.
   Various other verifications and sanity checks are made. For example,
   it is impossible to create a cyclic graph or provide a fork/join
   structure that is not supported by oozie.
4. Developer Overhead: The Scoozie DSL code makes it easy to read and
   write workflows - no more messy XML.
5. Multiple sources of truth: The Scoozie specification of a job will
   be the source of truth for any workflow


Version:
-------------
As of version 0.5.6, scoozie has been upgraded to cdh5.3.3. Please depend on an earlier scoozie version to support lower versions of CDH libraries.


How it works:
-------------

There are three main layers to Scoozie:

1. Specification of job in DSL.  This encapsulates the syntactic sugar
   that makes it easy to read and write Scoozie workflows. The general
   format for a Scoozie workflow is as follows:

    ```scala
    val job1 = JobType(params) dependsOn Start
    val job2 = JobType(params) dependsOn job2
    val job3 = JobType(params) dependsOn job3, etc
    ```

    This is the code that a workflow developer will actually be writing.
    The intent is to make it easy to read and to see the dependency path
    between jobs. Scoozie will automatically figure out forks/joins, etc.
    Specific samples of this can be found in src/main/scala/samples.scala

2. Logic / conversion from DSL spec to workflow graph.  This is the real
   workhorse of Scoozie, as it converts the programmer-specified workflow
   into an intermediate graph of workflow nodes. Forks, joins, and
   decisions are figured out and made ready for the final step - conversion
   to XML.  This step also includes verification on the result graph.

3. Conversion from intermediate graph to XML.  This step makes use of
   scalaxb, an XML data-binding tool for scala. In this step each node in
   the intermediate graph is converted to an oozie-specific type. Finally,
   these nodes are converted to XML.  This XML can be run in oozie by a
   scala command, examples of which can be found in src/main/scala/runSamples.scala


DSL Class Structure:
--------------------

Node: Contains work and a list of dependencies. A node is also a dependency.

Dependency: The parent of everything that a workflow job could depend on. (nodes or decisions)

Work: Any work that a workflow node performs. This could be End, Kill, Jobs, sub workflows, etc.

Job: Extends Work. This is the parent of the typical oozie jobs. (HiveJob, MapReduceJob,
	 DistCpJob, etc). Specific jobs are easy to extend and customize in order to fit the client's
	 needs.

In practice, each line of developer-specified Scoozie code will return a Node, which
other Nodes will depend on (Decisions are a special case of this, which will simply
return a Dependency).

Tutorial:
--------

1. **Hello World!**
	The following workflow will create an Oozie workflow that will create a Hello World directory on HDFS.

	```scala
	def HelloWorldWorkflow = {
	    val mkHelloWorld = FsJob(
	        name = "make-hello-world",
	        tasks = List(
	            MkDir("${nameNode}/users/tmp/oozie-fun/hello-world_${wf:id()}")
	        )
	    ) dependsOn Start
	    val done = End dependsOn mkHelloWorld
	    Workflow("hello-world-wf", done)
	}
	```

    This is an example of the basic structure of every scoozie workflow. Likely every scoozie workflow you will write will begin with a job that depends on Start and ends with an "End" job. The dependency chain makes it possible to specify the entire workflow by only referencing the end node in the Workflow object creation.

2. **Getting more complicated - using forks & joins.**
	Scoozie is smart enough to figure out fork and join structures by looking at the dependencies between workflow nodes. This reduces cognitive load as *for each node the developer only needs to think about what that node depends on.*
For example, consider the following workflow:

    ```scala
    def SimpleForkJoin = {
        val first = MapReduceJob("first") dependsOn Start
        val secondA = MapReduceJob("secondA") dependsOn first
        val secondB = MapReduceJob("secondB") dependsOn first
        val third = MapReduceJob("third") dependsOn (secondA, secondB)
        Workflow("simple-fork-join", third)
    }
    ```

	Scoozie will automatically create a workflow structured as
	first -> fork -> secondA/secondB -> join -> third

	In addition, Scoozie will alert you by throwing an error if the workflow you have specified is not allowed by Oozie (for example, two nodes from different threads being joined)

3. **Making things interesting - Decisions**
	Often a developer needs to specify variable paths in a workflow that are run conditionally. This is allowed in Scoozie via Decision and OneOf.

	```scala
	def DecisionExample = {
		val first = MapReduceJob("first") dependsOn Start
		val decision = Decision(
		    "route1" -> Predicates.BooleanProperty("${doRoute1}")
		) dependsOn first
		val route1Start = MapReduceJob("r1Start") dependsOn (decision option "route1")
		val route1End = MapReduceJob("r1End") dependsOn route1Start
		val route2Start = MapReduceJob("r2Start") dependsOn (decision default)
		val route2End = MapReduceJob("r2End") dependsOn route2Start
		val last = MapReduceJob("last") dependsOn OneOf(route1End, route2End)
		val done = End dependsOn last
		Workflow("simple-decision", done)
	}
	```
	In this example, route1 (route1Start -> route1End) will be run if ${doRoute1} is evaluated to true.  Otherwise, route2 (route2Start -> route2End) will be run. Again, this reduces developer cognitive overhead as the developer only needs to consider the dependencies for one node at a time. Clearly "last" depends on only *one* of route1 and route2.
	More complex decision structures are supported by Oozie as well. Verification will be automatically performed on the decisions to make sure that the developer has included all routes and default routes for each decision.

	1. **Pour some sugar on me - "doIf" and "Optional"**
		A common design pattern for decisions is to have an "optional" route that may or may not be inserted into the workflow.  Scoozie has provided some sugar for making this special case easy to think about and specify in code.

		```scala
		def SugarOption = {
		    val first = MapReduceJob("first") dependsOn Start
		    val option = MapReduceJob("option") dependsOn first doIf "doOption"
		    val second = MapReduceJob("second") dependsOn Optional(option)
		    val done = End dependsOn second
		    Workflow("sugar-option-decision", done)
		}
		```
	This syntax makes the semantics behind the optional node clearer than thinking about a separate decision. In this example, "option" is run if the "${doOption}" argument is evaluated to true (scoozie will figure out the brackets), and "second" must come after "first" and optionally "option".

4. **Scalability, Modularity - Sub Workflows**
	Scoozie allows the developer to control the granularity of his project by using and defining sub-workflows at will. Using the previous SugarOption workflow as an example, we can easily create a new, more complex workflow without worrying about its specifics.

	```scala
	def SubWfExample = {
		val begin = MapReduceJob("begin") dependsOn Start
		val someWork = MapReduceJob("someWork") dependsOn begin
		val subwf = SugarOption dependsOn someWork
		val end = End dependsOn subwf
		Workflow("sub-wf-example", end)
	}
	```

5. **Misc. Cool Things**
	1. Specifying custom error-to paths on nodes.
		Scoozie also makes it easy to provide customizations such as error-to paths on nodes (rather than erroring to "kill" or "fail")

		```scala
		def CustomErrorTo = {
		    val first = MapReduceJob("first") dependsOn Start
		    val errorPath = MapReduceJob("error") dependsOn (first error)
		    val second = MapReduceJob("second") dependsOn first
		    val end = End dependsOn OneOf(second, errorPath)
		}
		```

	This scoozie code will create a workflow containing three nodes. The "first" node will proceed to "second" in the optimal case, but will  now proceed to "errorPath" in the case of an error.  Additionally, the OneOf clause is used again here and has an intuitive meaning.

6. **Creating and Running Workflows in Scoozie**
	Scoozie makes running workflows easy. Simply create an object that extends ScoozieApp, pass in your workflow and any parameters, and you've got a runnable main class that will generate and run an oozie workflow.

	```scala
	object RunExample extends ScoozieApp(SubWfExample)
	```

	Scoozie uses sbt-assembly in order to create an executable jar. Simply type

		> assembly

	from sbt and all of the necessary dependencies and code will be assembled into a single jar: "scoozie-assembly-0.x.jar"
	You can run your workflow by typing

	```
	java -cp scoozie-assembly-0.x.jar RunExample -arg1=foo -arg2=bar ...
	```

    More examples of creating runnable scoozie objects can be found in runSamples.scala


Additional Help
--------------
More example scoozie code can be found in samples.scala. Available job definitions can be found in jobs.scala.  These jobs can be easily extended to fit your specific use cases.

