/*
 * MIT License
 *
 * Copyright (c) 2016 mbr targeting GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package eu.m6r.druid.client

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.net.URLEncoder
import javax.xml.bind.JAXBContext

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps
import scalaj.http.Http
import scalaj.http.HttpResponse
import scalaj.http.HttpStatusException

import com.fasterxml.jackson.databind.node.ObjectNode
import com.twitter.zk.ZkClient
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.HttpStatus
import org.eclipse.persistence.jaxb.MarshallerProperties
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormat
import scopt.OptionParser

import eu.m6r.druid.client.granularities.QueryGranularity
import eu.m6r.druid.client.granularities.SegmentGranularity
import eu.m6r.druid.client.models.DruidHost
import eu.m6r.druid.client.models.IndexTask
import eu.m6r.druid.client.models.IndexTaskBuilder
import eu.m6r.druid.client.models.KillTaskBuilder
import eu.m6r.druid.client.models.RunningTask
import eu.m6r.druid.client.models.TaskStatus

/** Druid API client that implements several endpoint functions.
  *
  * @param zookeeperHosts Comma-separated list of zookeeper hosts (e.g. "host1:2181,host2:2181")
  */
case class DruidHttpClient(zookeeperHosts: String) extends LazyLogging {

  private case class Segment(dataSource: String, segment: String)

  private object DruidRoles extends Enumeration {
    val COORDINATOR = Value("druid:coordinator")
    val OVERLORD = Value("overlord")
  }

  private case class IndexerStatus(id: String, status: String, duration: Int)

  private val jaxbContext = JAXBContext.newInstance(classOf[IndexTask])
  private val marshaller = jaxbContext.createMarshaller()
  marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, "application/json")

  private val zookeeperTimeout = 30 seconds
  private lazy val zookeeperClient = ZkClient(zookeeperHosts, zookeeperTimeout)

  private def discoverDruidRoleHostFromZookeeper(role: DruidRoles.Value): Future[DruidHost] = {
    val coordinatorNode = zookeeperClient(s"/druid/discovery/$role")
    coordinatorNode.getChildren().flatMap(c => {
      val data = c.children.head.getData()
      data.map(data => objectMapper.readValue[DruidHost](data.bytes))
    })
  }

  private def getCoordinator: Future[DruidHost] =
    discoverDruidRoleHostFromZookeeper(DruidRoles.COORDINATOR)

  private def getOverlord: Future[DruidHost] =
    discoverDruidRoleHostFromZookeeper(DruidRoles.OVERLORD)

  private def segmentPath(coordinator: DruidHost, segment: Segment) =
    s"http://${coordinator.address}:${coordinator.port}/druid/coordinator/v1/datasources/" +
        s"${segment.dataSource}/segments/${segment.segment}"

  private def getSegment(coordinator: DruidHost, segment: Segment): HttpResponse[String] =
    Http(segmentPath(coordinator, segment)).asString

  private def getSegmentsForInterval(dataSource: String,
                                     interval: Interval): Future[Seq[Segment]] = {

    val escapedInterval = URLEncoder.encode(interval.toString, "UTF-8")
    getCoordinator.map(coordinator => {
      val requestURL = s"http://${coordinator.address}:${coordinator.port}/druid/coordinator/v1/" +
          s"datasources/$dataSource/intervals/$escapedInterval"
      logger.info("Requesting segments from {}", requestURL)
      val response = Http(requestURL).asString

      response.code match {
        case HttpStatus.SC_OK =>
          objectMapper.readValue[Seq[String]](response.body)
              .map(segment => Segment(dataSource, segment))
        case HttpStatus.SC_NOT_FOUND => Seq.empty
        case _ => throw DruidHttpClient.httpException(response)
      }
    })
  }

  private def deleteSegment(segment: Segment): Future[Segment] = {
    getCoordinator.map(coordinator => {
      val response = Http(segmentPath(coordinator, segment)).method("DELETE").asString
      if (response.isError) {
        throw new IOException(s"Request failed:${response.body}")
      }

      logger.info(s"Requested deletion of druid segment: $segment")

      // Wait until druid has deleted the segment.
      def checkForSegmentDeletion: Segment = {
        val retryPeriod = 2 seconds

        val response = getSegment(coordinator, segment)
        if (response.isError) {
          throw DruidHttpClient.httpException(response)
        }
        else if (response.code == HttpStatus.SC_NO_CONTENT) {
          logger.info(s"Successfully deleted segment $response")
          segment
        }
        else {
          Thread.sleep(retryPeriod.toMillis)
          checkForSegmentDeletion
        }
      }

      checkForSegmentDeletion
    })
  }

  private def waitUntilTaskHasFinished(taskId: String): Future[String] = {
    taskStatus(taskId).map(_.status.status).flatMap {
      case "RUNNING" =>
        Thread.sleep((2 minutes).toMillis)
        waitUntilTaskHasFinished(taskId)
      case "FAILED" => throw new RuntimeException("Indexing task failed")
      case "SUCCESS" => Future.successful(taskId)
    }
  }

  /** Get a list of the status of all running tasks
    *
    * @return Future containing a [[Seq]] of [[eu.m6r.druid.client.models.TaskStatus]].
    */
  def runningTasks(): Future[Seq[RunningTask]] = {
    getCoordinator.map {
      case DruidHost(_, address, port) =>
        val requestURL = s"http://$address:$port/druid/indexer/v1/runningTasks"
        val response = Http(requestURL)
            .header("Content-Type", "application/json")
            .asString
        response.code match {
          case HttpStatus.SC_OK => objectMapper.readValue[Seq[RunningTask]](response.body)
          case _ => throw DruidHttpClient.httpException(response)
        }
    }
  }

  /** Returns the status of a task.
    *
    * @param taskId Druid task id
    * @return Future containing the task status
    */
  def taskStatus(taskId: String): Future[TaskStatus] = {
    getCoordinator.map {
      case DruidHost(_, address, port) =>
        val requestURL = s"http://$address:$port/druid/indexer/v1/task/$taskId/status"
        val response = Http(requestURL)
            .header("Content-Type", "application/json")
            .asString
        response.code match {
          case HttpStatus.SC_OK => objectMapper.readValue[TaskStatus](response.body)
          case _ => throw DruidHttpClient.httpException(response)
        }
    }
  }

  /** Deletes all segments inside a given interval
    *
    * @param dataSource Name of the Druid data source
    * @param interval The interval. All segements inside the intervals time range will be deleted.
    * @return Future containing a list of all deleted segments
    */
  def deleteSegmentsInInterval(dataSource: String, interval: Interval): Future[Seq[String]] =
    getSegmentsForInterval(dataSource, interval)
        .map(_.map(deleteSegment(_).map(_.segment)))
        .flatMap(x => Future.sequence(x))

  /** Kills all segments inside a given interval
    *
    * @param dataSource Name of the Druid data source.
    * @param interval The interval. All segements inside the intervals time range will be killed.
    * @return Future containing id of the kill task.
    */
  def killSegmentsInInterval(dataSource: String, interval: Interval): Future[String] = {
    getCoordinator.map(coordinator => {
      val killSegmentsPath =
        s"http://${coordinator.address}:${coordinator.port}/druid/indexer/v1/task/"

      val killTask = new KillTaskBuilder()
        .withDataSource(dataSource)
        .withInterval(interval)
        .build()

      val taskJson = objectMapper.writeValueAsString(killTask)

      val response = Http(killSegmentsPath)
        .postData(taskJson)
        .header("content-type", "application/json")
        .method("POST")
        .asString

      if (response.isError) {
        throw new IOException(s"Request failed:${response.body}")
      }

      response match {
        case HttpResponse(body: String, HttpStatus.SC_OK, _) =>
          val taskId = objectMapper.readValue[ObjectNode](body).get("task").textValue()
          taskId
        case _ => throw DruidHttpClient.httpException(response)
      }
    })
  }

  /** Closes a running indexing task
    *
    * @param taskId Id of the task that shall be closed.
    * @return Future that calls when the task has been closed.
    */
  def shutdownIndexingTask(taskId: String): Future[Unit] = {
    logger.debug("Shutting down indexing task {}", taskId)

    getOverlord.map(overlord => {
      val url =
        s"http://${overlord.address}:${overlord.port}/druid/indexer/v1/task/$taskId/shutdown"
      val response = Http(url).method("POST").asString
      if (response.isError) throw DruidHttpClient.httpException(response)
    })
  }

  /** Starts a new indexing task.
    *
    * @param task RunningTask to start. Use [[models.IndexTaskBuilder]] to create it.
    * @return Future containing the task id.
    */
  def startIndexTask(task: IndexTask): Future[String] = {
    val byteStream = new ByteArrayOutputStream()
    marshaller.marshal(task, byteStream)

    getOverlord.flatMap {
      case DruidHost(_, address, port) =>
        val requestURL = s"http://$address:$port/druid/indexer/v1/task"
        val response = Http(requestURL)
            .postData(byteStream.toByteArray)
            .header("Content-Type", "application/json")
            .asString

        response match {
          case HttpResponse(body: String, HttpStatus.SC_OK, _) =>
            val taskId = objectMapper.readValue[ObjectNode](body).get("task").textValue()
            waitUntilTaskHasFinished(taskId)
          case _ => throw DruidHttpClient.httpException(response)
        }
    }
  }
}

object DruidHttpClient extends LazyLogging {
  DateTimeZone.setDefault(DateTimeZone.UTC)
  private val DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:00:00'Z'")

  private sealed abstract class Command(commandString: String) {
    def cmd: String = commandString
  }

  private object Command {

    case object CopyDruidToDruid extends Command("copy-druid-to-druid")

    case object CopyFileToDruid extends Command("copy-file-to-druid")

    case object ShutdownIndexingTask extends Command("shutdown-indexing-task")

    case object ListRunningTasks extends Command("list-running-tasks")

    case object TaskStatus extends Command("task-status")

    case object DeleteSegments extends Command("delete-segments")

    case object KillSegments extends Command("kill-segments")

  }

  private case class Conf(cmd: Option[Command] = None, zookeeperHosts: String = "",
                          source: String = "", dimensions: Seq[String] = Seq.empty,
                          metrics: Seq[(String, String, Option[String])] = Seq.empty,
                          configFile: Option[File] = None,
                          targetSource: Option[String] = None, inputPaths: Seq[String] = Seq.empty,
                          segmentGranularity: SegmentGranularity = SegmentGranularity.HOUR,
                          queryGranularity: QueryGranularity = QueryGranularity.HOUR,
                          segmentStart: DateTime = new DateTime(),
                          segmentEnd: Option[DateTime] = None,
                          reducerMemory: Option[Int] = None,
                          targetPartitionSize: Option[Int] = None,
                          taskId: Option[String] = None)


  private val confParser = new OptionParser[Conf]("Druid Http Client") {
    head("druid http client")

    opt[String]('z', "zookeeper-hosts").required()
        .valueName("<host1:port1>,<host2:port2>,...")
        .action((x, c) => c.copy(zookeeperHosts = x))
        .text("list of zookeeper hosts")

    cmd(Command.CopyDruidToDruid.cmd)
        .action((_, c) => c.copy(Some(Command.CopyDruidToDruid)))
        .text("Copy a segment from on source to another")
        .children(
          opt[Seq[String]]('d', "dimensions").valueName("<dimension>,<dimension>...")
              .action((x, c) => c.copy(dimensions = x))
              .text("List of druid dimensions."),
          opt[(String, String, Option[String])]('m', "metric")
              .valueName("<typeName>, <name>[, <fieldName>]")
              .action((x, c) => c.copy(metrics = c.metrics :+ x))
              .text("Add metric"),
          opt[File]('c', "task-config").optional().valueName("<taskConfig>")
              .action((x, c) => c.copy(configFile = Some(x))),
          opt[String]('t', "target-source").required().valueName("<source>")
              .action((x, c) => c.copy(targetSource = Some(x)))
              .text("Source to copy to"),
          opt[String]('g', "segment-granularity").optional().valueName("<segmentGranularity>")
              .action((x, c) => c.copy(segmentGranularity = SegmentGranularity.fromString(x)))
              .text("Granularity of the segment (default=HOUR)"),
          opt[String]('q', "query-granularity").optional().valueName("<queryGranularity>")
              .action((x, c) => c.copy(queryGranularity = QueryGranularity.fromString(x)))
              .text("Queryable granularity (default=HOUR)"),
          opt[String]("segment-start").required().valueName("<segmentStart>")
              .action((x, c) => c.copy(segmentStart = DATE_FORMATTER.parseDateTime(x)))
              .text("Segment start time. Format: yyyy-MM-ddThh:mm:ssZ"),
          opt[String]("segment-end").valueName("<segmentEnd>")
              .action((x, c) => c.copy(segmentEnd = Some(DATE_FORMATTER.parseDateTime(x))))
              .text("Segment end time. Format: yyyy-MM-ddThh:mm:ssZ"),
          opt[Int]("reducer-memory").valueName("<memReducer>")
              .action((x, c) => c.copy(reducerMemory = Some(x)))
              .text("Hadoop reducer memory in MB"),
          opt[Int]("target-partition-size").valueName("<targetPartitionSize>")
              .action((x, c) => c.copy(targetPartitionSize = Some(x)))
              .text("Target number of rows to include in a partition, " +
                  "should be a number that targets segments of 500MB~1GB."),
              opt[String]('s', "source").required().valueName("<source>")
                  .action((x, c) => c.copy(source = x))
                  .text("Druid source to copy from")
        )
    cmd(Command.CopyFileToDruid.cmd)
        .action((_, c) => c.copy(Some(Command.CopyFileToDruid)))
        .text("Reads a segment from HDFS into Druid")
        .children(
          opt[Seq[String]]('d', "dimensions").valueName("<dimension>,<dimension>...")
              .action((x, c) => c.copy(dimensions = x))
              .text("List of druid dimensions."),
          opt[(String, String, Option[String])]('m', "metric")
              .valueName("<typeName>, <name>[, <fieldName>]")
              .action((x, c) => c.copy(metrics = c.metrics :+ x))
              .text("Add metric"),
          opt[File]('c', "task-config").optional().valueName("<taskConfig>")
              .action((x, c) => c.copy(configFile = Some(x))),
          opt[String]('t', "target-source").required().valueName("<source>")
              .action((x, c) => c.copy(targetSource = Some(x)))
              .text("Source to copy to"),
          opt[String]('g', "segment-granularity").optional().valueName("<segmentGranularity>")
              .action((x, c) => c.copy(segmentGranularity = SegmentGranularity.fromString(x)))
              .text("Granularity of the segment (default=HOUR)"),
          opt[String]('q', "query-granularity").optional().valueName("<queryGranularity>")
              .action((x, c) => c.copy(queryGranularity = QueryGranularity.fromString(x)))
              .text("Queryable granularity (default=HOUR)"),
          opt[String]("segment-start").required().valueName("<segmentStart>")
              .action((x, c) => c.copy(segmentStart = DATE_FORMATTER.parseDateTime(x)))
              .text("Segment start time. Format: yyyy-MM-ddThh:mm:ssZ"),
          opt[String]("segment-end").valueName("<segmentEnd>")
              .action((x, c) => c.copy(segmentEnd = Some(DATE_FORMATTER.parseDateTime(x))))
              .text("Segment end time. Format: yyyy-MM-ddThh:mm:ssZ"),
          opt[Int]("reducer-memory").valueName("<memReducer>")
              .action((x, c) => c.copy(reducerMemory = Some(x)))
              .text("Hadoop reducer memory in MB"),
          opt[Int]("target-partition-size").valueName("<targetPartitionSize>")
              .action((x, c) => c.copy(targetPartitionSize = Some(x)))
              .text("Target number of rows to include in a partition, " +
                  "should be a number that targets segments of 500MB~1GB."),
              opt[String]('i', "input-path").required().valueName("<inputPath>")
                  .action((x, c) => c.copy(inputPaths = c.inputPaths :+ x))
        )
    cmd(Command.ShutdownIndexingTask.cmd)
        .action((_, c) => c.copy(Some(Command.ShutdownIndexingTask)))
        .text("Shuts down a running indexing task")
        .children(
          opt[String]('t', "task-id").required().valueName("<taskId>")
              .action((x, c) => c.copy(taskId = Some(x)))
              .text("Id of the running task.")
        )
    cmd(Command.DeleteSegments.cmd)
        .action((_, c) => c.copy(Some(Command.DeleteSegments)))
        .text("Deletes segment(s) from druid source")
        .children(
          opt[String]('s', "source").required().valueName("<source>")
              .action((x, c) => c.copy(source = x))
              .text("Druid source to copy from"),
          opt[String]("segment-start").required().valueName("<segmentStart>")
              .action((x, c) => c.copy(segmentStart = DATE_FORMATTER.parseDateTime(x)))
              .text("Segment start time. Format: yyyy-MM-ddThh:mm:ssZ"),
          opt[String]("segment-end").required().valueName("<segmentEnd>")
              .action((x, c) => c.copy(segmentEnd = Some(DATE_FORMATTER.parseDateTime(x))))
              .text("Segment end time. Format: yyyy-MM-ddThh:mm:ssZ")
        )
    cmd(Command.KillSegments.cmd)
        .action((_, c) => c.copy(Some(Command.KillSegments)))
        .text("Kills segment(s) in a specified data source")
        .children(
          opt[String]('s', "source").required().valueName("<source>")
              .action((x, c) => c.copy(source = x))
              .text("Druid source to kill segments in"),
          opt[String]("segment-start").required().valueName("<segmentStart>")
              .action((x, c) => c.copy(segmentStart = DATE_FORMATTER.parseDateTime(x)))
              .text("Segment start time. Format: yyyy-MM-ddThh:mm:ssZ"),
          opt[String]("segment-end").required().valueName("<segmentEnd>")
              .action((x, c) => c.copy(segmentEnd = Some(DATE_FORMATTER.parseDateTime(x))))
              .text("Segment end time. Format: yyyy-MM-ddThh:mm:ssZ")
        )

    cmd(Command.TaskStatus.cmd)
        .action((_, c) => c.copy(Some(Command.TaskStatus)))
        .text("Shows the status of a task")
        .children(
          opt[String]('t', "task-id").required().valueName("<taskId>")
              .action((x, c) => c.copy(taskId = Some(x)))
              .text("Id of the task.")
        )

    cmd(Command.ListRunningTasks.cmd)
        .action((_, c) => c.copy(Some(Command.ListRunningTasks)))
        .text("Shows the status of all running task")

    checkConfig(c => {
      if (c.cmd.isEmpty) {
        failure("A command has to be specified.")
      } else {
        success
      }
    })
  }

  def httpException(response: HttpResponse[String]): HttpStatusException =
    HttpStatusException(response.code, response.header("Status").getOrElse("UNKNOWN"),
      response.body.toString)

  private def runIndexTask(client: DruidHttpClient, conf: Conf) = {

    var taskBuilder = conf.cmd.get match {
      case Command.CopyDruidToDruid =>
        new IndexTaskBuilder().withSource(conf.source)
      case Command.CopyFileToDruid =>
        new IndexTaskBuilder().withInputPaths(conf.inputPaths)
      case _ => throw new RuntimeException("Task type not supported.")
    }

    val interval = conf.segmentEnd match {
      case None => new Interval(conf.segmentStart, conf.segmentGranularity.period)
      case Some(segmentEnd) => new Interval(conf.segmentStart, segmentEnd)
    }

    taskBuilder = conf.configFile match {
      case None => taskBuilder.withDimensions(conf.dimensions).withMetrics(conf.metrics)
      case Some(configFile) => taskBuilder.withConfigFile(configFile)
    }

    taskBuilder.withDestinationSource(conf.targetSource.get)
        .addInterval(interval)
        .withQueryGranularity(conf.queryGranularity)
        .withSegmentGranularity(conf.segmentGranularity)

        .withReducerMemory(conf.reducerMemory)
        .withTargetPartitionSize(conf.targetPartitionSize)
        .build()

    client.startIndexTask(taskBuilder.build())
  }

  def main(args: Array[String]): Unit = {
    confParser.parse(args, Conf()) match {
      case Some(conf) =>
        val client = new DruidHttpClient(conf.zookeeperHosts)

        val writer = objectMapper.writerWithDefaultPrettyPrinter()

        val future = conf.cmd.get match {
          case Command.CopyDruidToDruid => runIndexTask(client, conf)
          case Command.CopyFileToDruid => runIndexTask(client, conf)
          case Command.ShutdownIndexingTask => client.shutdownIndexingTask(conf.taskId.get)
          case Command.TaskStatus => client.taskStatus(conf.taskId.get)
              .map(writer.writeValue(System.out, _))
          case Command.ListRunningTasks => client.runningTasks()
              .map(writer.writeValue(System.out, _))
          case Command.DeleteSegments =>
            val interval = new Interval(conf.segmentStart, conf.segmentEnd.get)
            client.deleteSegmentsInInterval(conf.source, interval)
          case Command.KillSegments =>
            val interval = new Interval(conf.segmentStart, conf.segmentEnd.get)
            client.killSegmentsInInterval(conf.source, interval)
        }

        Await.result(future, 2 hours)

      case None => System.exit(1)
    }
  }
}

