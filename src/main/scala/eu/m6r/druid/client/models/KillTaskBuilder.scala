/*
 * MIT License
 *
 * Copyright (c) 2017 mbr targeting GmbH
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

import org.joda.time.Interval

import eu.m6r.druid.client.models.KillTaskBuilder.BuilderValidationException

/** Builder class for [[KillTask]] instances.
  *
  * Creates an [[KillTask]] object as specified by the
  * [[http://druid.io/docs/latest/ingestion/tasks.html druid API]].
  */
final class KillTaskBuilder {

  protected var dataSource: Option[String] = None
  protected var interval: Option[String] = None

  /** Updates builder by setting dataSource.
    *
    * @param dataSource Name of the data source.
    * @return Updated builder.
    */
  def withDataSource(dataSource: String): KillTaskBuilder = {
    this.dataSource = Some(dataSource)
    this
  }

  /** Updates builder by setting the interval.
    *
    * @param interval The interval to kill segments in.
    * @return Updated builder.
    */
  def withInterval(interval: Interval): KillTaskBuilder = {
    this.interval = Some(interval.toString)
    this
  }

  /** Builds the [[eu.m6r.druid.client.models.KillTask]] from the current builder state.
    *
    * @return The built kill task.
    */
  def build(): KillTask = {
    validate()

    val killTask = new KillTask
    killTask.setId(s"kill_${dataSource.get}_${intervalFormatted}")
    killTask.setType("kill")
    killTask.setInterval(interval.get)
    killTask.setDataSource(dataSource.get)

    killTask
  }

  private def intervalFormatted: String = {
    this.interval.get.toString.replaceAll("/", "_")
  }

  private def validate(): Unit = {
    if (dataSource.isEmpty) throw new BuilderValidationException("dataSource")
    if (interval.isEmpty) throw new BuilderValidationException("interval")
  }
}

object KillTaskBuilder {

  private class BuilderValidationException(field: String) extends Exception {

    override def getMessage: String = {
      s"Required field '${field}' is not set."
    }
  }
}
