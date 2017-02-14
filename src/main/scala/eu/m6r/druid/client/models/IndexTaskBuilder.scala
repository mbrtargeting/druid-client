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

package eu.m6r.druid.client.models

import java.io.File
import javax.xml.bind.JAXBContext

import scala.collection.JavaConverters._

import org.eclipse.persistence.jaxb.UnmarshallerProperties
import org.joda.time.DateTime
import org.joda.time.Interval

import eu.m6r.druid.client.granularities.QueryGranularity
import eu.m6r.druid.client.granularities.SegmentGranularity
import eu.m6r.druid.client.models.IndexTaskBuilder.BuilderValidationException

/** Builder class for [[IndexTask]] instances.
  *
  * Creates an [[IndexTask]] object as specified by the
  * [[http://druid.io/docs/latest/ingestion/batch-ingestion.html druid API]].
  */
final class IndexTaskBuilder {
  protected var destinationSource: Option[String] = None
  protected var dimensions: Seq[String] = Seq.empty
  protected var metrics: Seq[Metric] = Seq.empty
  protected var segmentGranularity: Option[SegmentGranularity] = None
  protected var queryGranularity: Option[QueryGranularity] = None
  protected var intervals: Seq[String] = Seq.empty
  protected var reducerMemory: Option[Int] = None
  protected var targetPartitionSize: Option[Int] = None
  protected var inputPaths: Seq[String] = Seq.empty
  protected var source: Option[String] = None

  /** The target druid source.
    *
    * Specifies `"spec" / "dataSchema" / "dataSource"`
    *
    * @param destinationSource Name of source
    * @return Updated builder
    */
  def withDestinationSource(destinationSource: String): IndexTaskBuilder = {
    this.destinationSource = Some(destinationSource)
    this
  }

  /** Sets druid dimensions
    *
    * Overrides old dimension list.
    *
    * Specifies `"spec" / "dataSchema" / "parser" / "dimensionsSpec" / "dimensions"`
    *
    * @param dimensions Sequence of all dimensions
    * @return Updated builder
    */
  def withDimensions(dimensions: Seq[String]): IndexTaskBuilder = {
    this.dimensions = dimensions
    this
  }

  /** Add dimensions to list of dimensions.
    *
    * Specifies `"spec" / "dataSchema" / "parser" / "dimensionsSpec" / "dimensions"`
    *
    * @param dimension Dimension name as String
    * @return Updated builder
    */
  def addDimension(dimension: String): IndexTaskBuilder = {
    this.dimensions :+ dimension
    this
  }

  /** Sets list of metrics from a list of tuples.
    *
    * Tuples have to contain `(type, name, Option(fieldName))`.
    * If `fieldName` is `None`, `name` is used as `fieldName`.
    *
    * Specifies `"spec" / "dataSchema" / "metricsSpec"`
    *
    * @param metrics List of tuples containing the metrics.
    * @return Updated builder
    */
  def withMetrics(metrics: Seq[(String, String, Option[String])]): IndexTaskBuilder = {
    this.metrics = Seq.empty
    metrics.foreach {
      case (typeName, name, fieldName) => addMetric(typeName, name, fieldName.getOrElse(name))
    }
    this
  }

  /** Add metric to the list of metrics.
    *
    * [[http://druid.io/docs/latest/querying/aggregations.html List]] of supported aggregation
    * types.
    *
    * Specifies `"spec" / "dataSchema" / "metricsSpec"`
    *
    * @param typeName Aggregation type name.
    * @param name     Name of the metric. Also used as field name.
    * @return Updated builder
    */
  def addMetric(typeName: String, name: String): IndexTaskBuilder = addMetric(typeName, name, name)

  /** Add metric to the list of metrics.
    *
    * [[http://druid.io/docs/latest/querying/aggregations.html List]] of supported aggregation
    * types.
    *
    * Specifies `"spec" / "dataSchema" / "metricsSpec"`
    *
    * @param typeName  Aggregation type name.
    * @param name      Name of the metric.
    * @param fieldName Name of the field that is used to calculate the metric.
    * @return Updated builder
    */
  def addMetric(typeName: String, name: String, fieldName: String): IndexTaskBuilder = {
    val metric = new Metric
    metric.setType(typeName)
    metric.setName(name)
    metric.setFieldName(fieldName)
    this.metrics :+ metric
    this
  }

  /** Set target segment granularity.
    *
    * Specifies `"spec" / "dataSchema" / "granularitySpec" / "segmentGranularity"`
    *
    * @param segmentGranularity Segment granularity.
    * @return Updated builder
    */
  def withSegmentGranularity(segmentGranularity: SegmentGranularity): IndexTaskBuilder = {
    this.segmentGranularity = Some(segmentGranularity)
    this
  }

  /** Set target query granularity.
    *
    * Specifies `"spec" / "dataSchema" / "granularitySpec" / "queryGranularity"`
    *
    * @param queryGranularity Query granularity.
    * @return Updated builder
    */
  def withQueryGranularity(queryGranularity: QueryGranularity): IndexTaskBuilder = {
    this.queryGranularity = Some(queryGranularity)
    this
  }

  /** Set input intervals.
    *
    * Only events inside these intervals will be stored in the target source.
    *
    * Specifies `"spec" / "dataSchema" / "granularitySpec" / "intervals"`
    * and `"spec" / "dataSchema" / "ioConfig" / "inputSpec" / "ingestionSpec" / "intervals"`
    *
    * @param intervals JodaTime intervals
    * @return Updated builder
    */
  def withIntervals(intervals: Seq[Interval]): IndexTaskBuilder = {
    this.intervals = intervals.map(_.toString)
    this
  }

  /** Add input intervals.
    *
    * Only events inside these intervals will be stored in the target source.
    *
    * Specifies `"spec" / "dataSchema" / "granularitySpec" / "intervals"`
    * and `"spec" / "dataSchema" / "ioConfig" / "inputSpec" / "ingestionSpec" / "intervals"`
    *
    * @param interval JodaTime interval
    * @return Updated builder
    */
  def addInterval(interval: Interval): IndexTaskBuilder = {
    this.intervals = this.intervals :+ interval.toString
    this
  }

  /** Add input intervals.
    *
    * Only events inside these intervals will be stored in the target source.
    *
    * Specifies `"spec" / "dataSchema" / "granularitySpec" / "intervals"`
    * and `"spec" / "dataSchema" / "ioConfig" / "inputSpec" / "ingestionSpec" / "intervals"`
    *
    * @param startTime Start time of the interval
    * @param endTime   End time of the interval
    * @return Updated builder
    */
  def addInterval(startTime: DateTime, endTime: DateTime): IndexTaskBuilder =
    addInterval(new Interval(startTime, endTime))

  /** Set reducer memory.
    *
    * @param reducerMemory Amount of mem used by reducer in MB.
    * @return Updated builder
    */
  def withReducerMemory(reducerMemory: Int): IndexTaskBuilder =
    withReducerMemory(Some(reducerMemory))

  /** Set reducer memory.
    *
    * @param reducerMemory Optional containing the amount of mem used by reducer in MB.
    * @return Updated builder
    */
  def withReducerMemory(reducerMemory: Option[Int]): IndexTaskBuilder = {
    this.reducerMemory = reducerMemory
    this
  }

  /** Target number of rows to include in a partition, should be a number that targets
    * segments of 500MB~1GB.
    *
    * @param targetPartitionSize Max number of rows per partition.
    * @return Updated builder
    */
  def withTargetPartitionSize(targetPartitionSize: Int): IndexTaskBuilder =
    withTargetPartitionSize(Some(targetPartitionSize))

  /** Target number of rows to include in a partition, should be a number that targets
    * segments of 500MB~1GB.
    *
    * @param targetPartitionSize Option containing max number of rows per partition.
    * @return Updated builder
    */
  def withTargetPartitionSize(targetPartitionSize: Option[Int]): IndexTaskBuilder = {
    this.targetPartitionSize = targetPartitionSize
    this
  }

  /** The name of the ingested datasource. Datasources can be thought of as tables.
    *
    * @param source Name of the data source
    * @return Updated builder
    */
  def withSource(source: String): IndexTaskBuilder = {
    this.source = Some(source)
    this
  }

  /** Adds a base path to the list. Input paths indicating where the raw data is located.
    *
    * @param inputPath Input path
    * @return Updated builder
    */
  def addInputPath(inputPath: String): IndexTaskBuilder = {
    this.inputPaths :+ inputPath
    this
  }

  /** Input paths indicating where the raw data is located.
    *
    * @param inputPaths Seq of input paths.
    * @return
    */
  def withInputPaths(inputPaths: Seq[String]): IndexTaskBuilder = {
    this.inputPaths = inputPaths
    this
  }

  /** Reads dimensions and metrics from a config file. Config file follows the schema
    * of [[TaskConfig]].
    *
    * @param configFile Config file as File object.
    * @return Updated builder
    */
  def withConfigFile(configFile: File): IndexTaskBuilder = {
    val taskConfig = IndexTaskBuilder.parseTaskConfig(configFile)
    this.dimensions = taskConfig.getDimensions.asScala
    this.metrics = taskConfig.getMetrics.asScala
    this
  }

  private def validate(): Unit = {
    if (destinationSource.isEmpty) throw new BuilderValidationException("destinationSource")
    if (dimensions.isEmpty) throw new BuilderValidationException("dimensions")
    if (metrics.isEmpty) throw new BuilderValidationException("metrics")
    if (segmentGranularity.isEmpty) throw new BuilderValidationException("segmentGranularity")
    if (queryGranularity.isEmpty) throw new BuilderValidationException("queryGranularity")
    if (intervals.isEmpty) throw new BuilderValidationException("intervals")

    if (inputPaths.isEmpty && source.isEmpty || inputPaths.nonEmpty && source.isDefined) {
      throw new BuilderValidationException("inputPaths", "source")
    }
  }

  /** Builds the [[eu.m6r.druid.client.models.IndexTask]] from the current builder state.
    *
    * @return The built index task.
    */
  def build(): IndexTask = {
    validate()
    val inputSpec = new InputSpec

    if (inputPaths.nonEmpty) {
      inputSpec.setType("static")
      inputSpec.setPaths(inputPaths.mkString(", "))
    } else {
      val ingestionSpec = new IngestionSpec
      ingestionSpec.setDataSource(source.get)
      ingestionSpec.getIntervals.addAll(intervals.asJava)
      inputSpec.setType("dataSource")
      inputSpec.setIngestionSpec(ingestionSpec)
    }

    val dimensionSpec = new DimensionSpec
    dimensionSpec.getDimensions.addAll(dimensions.asJava)

    val timestampSpec = new TimestampSpec
    timestampSpec.setColumn("timestamp")
    timestampSpec.setFormat("iso")
    timestampSpec.setMissingValue("null")

    val parseSpec = new ParseSpec
    parseSpec.setFormat("json")
    parseSpec.setTimestampSpec(timestampSpec)
    parseSpec.setDimensionsSpec(dimensionSpec)

    val parser = new Parser
    parser.setFormat("hadoopyString")
    parser.setParseSpec(parseSpec)

    val granularitySpec = new GranularitySpec
    granularitySpec.setQueryGranularity(queryGranularity.toString)
    granularitySpec.setSegmentGranularity(segmentGranularity.toString)
    granularitySpec.getIntervals.addAll(intervals.asJava)

    val dataSchema = new DataSchema
    dataSchema.setDataSource(destinationSource.get)
    dataSchema.setGranularitySpec(granularitySpec)
    dataSchema.setParser(parser)
    dataSchema.getMetricsSpec.addAll(metrics.asJava)

    val jobProperties = new TuningConfig.JobProperties
    jobProperties.setMapreduceJobUserClasspathFirst("true")
    if (reducerMemory.isDefined) {
      jobProperties.setMapreduceReduceMemoryMb(reducerMemory.get)
    }

    val partitionsSpec = new PartitionsSpec
    partitionsSpec.setType("hashed")
    if (targetPartitionSize.isDefined) {
      partitionsSpec.setTargetPartitionSize(targetPartitionSize.get)
    }

    val tuningConfig = new TuningConfig
    tuningConfig.setType("hadoop")
    tuningConfig.setJobProperties(jobProperties)
    tuningConfig.setPartitionsSpec(partitionsSpec)

    val ioConfig = new IoConfig
    ioConfig.setType("hadoop")
    ioConfig.setInputSpec(inputSpec)

    val spec = new Spec
    spec.setDataSchema(dataSchema)
    spec.setIoConfig(ioConfig)
    spec.setTuningConfig(tuningConfig)

    val indexTask = new IndexTask
    indexTask.setType("index_hadoop")
    indexTask.setSpec(spec)

    indexTask
  }

}

object IndexTaskBuilder {

  private class BuilderValidationException(fields: String*) extends Exception {

    override def getMessage: String = {
      if (fields.isEmpty) {
        "Parameter missing."
      } else if (fields.size == 1) {
        s"Required field '${fields(0)}' is not set."
      } else {
        val formattedFields = fields.map(field => s"'$field'").mkString(", ")
        s"Exactly one of $formattedFields has to be defined."
      }
    }
  }

  private def parseTaskConfig(file: File): TaskConfig = {
    val jaxbContext = JAXBContext.newInstance(classOf[TaskConfig])
    val unmarshaller = jaxbContext.createUnmarshaller()

    if (file.getName.endsWith("json")) {
      unmarshaller.setProperty(UnmarshallerProperties.MEDIA_TYPE, "application/json")
    }

    unmarshaller.unmarshal(file).asInstanceOf[TaskConfig]
  }
}
