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

package eu.m6r.druid.client.granularities

import org.scalatest._

class QueryGranularitySpec extends FlatSpec with Matchers {

  val validGranularities =
    Seq("NONE",
      "SECOND",
      "MINUTE",
      "FIVE_MINUTE",
      "TEN_MINUTE",
      "FIFTEEN_MINUTE",
      "HOUR",
      "SIX_HOUR",
      "DAY",
      "WEEK",
      "MONTH",
      "YEAR")

  "Query Granularities" should "return object name from toString" in {
    QueryGranularity.NONE.toString should be("NONE")
    QueryGranularity.SECOND.toString should be("SECOND")
    QueryGranularity.MINUTE.toString should be("MINUTE")
    QueryGranularity.FIVE_MINUTE.toString should be("FIVE_MINUTE")
    QueryGranularity.TEN_MINUTE.toString should be("TEN_MINUTE")
    QueryGranularity.FIFTEEN_MINUTE.toString should be("FIFTEEN_MINUTE")
    QueryGranularity.HOUR.toString should be("HOUR")
    QueryGranularity.SIX_HOUR.toString should be("SIX_HOUR")
    QueryGranularity.DAY.toString should be("DAY")
    QueryGranularity.WEEK.toString should be("WEEK")
    QueryGranularity.MONTH.toString should be("MONTH")
    QueryGranularity.YEAR.toString should be("YEAR")
  }

  it should "return object from string" in {
    validGranularities
        .map(s => (QueryGranularity.fromString(s), s))
        .foreach(t => t._1.toString should be(t._2))
  }
}
