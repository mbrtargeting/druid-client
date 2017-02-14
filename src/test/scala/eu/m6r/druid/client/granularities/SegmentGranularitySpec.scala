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

import org.joda.time.Period
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class SegmentGranularitySpec extends FlatSpec with Matchers {
  val validGranularities =
    Seq("SECOND",
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

  "Segment Granularities" should "return object name from toString" in {
    SegmentGranularity.SECOND.toString should be("SECOND")
    SegmentGranularity.MINUTE.toString should be("MINUTE")
    SegmentGranularity.FIVE_MINUTE.toString should be("FIVE_MINUTE")
    SegmentGranularity.TEN_MINUTE.toString should be("TEN_MINUTE")
    SegmentGranularity.FIFTEEN_MINUTE.toString should be("FIFTEEN_MINUTE")
    SegmentGranularity.HOUR.toString should be("HOUR")
    SegmentGranularity.SIX_HOUR.toString should be("SIX_HOUR")
    SegmentGranularity.DAY.toString should be("DAY")
    SegmentGranularity.WEEK.toString should be("WEEK")
    SegmentGranularity.MONTH.toString should be("MONTH")
    SegmentGranularity.YEAR.toString should be("YEAR")
  }

  it should "return object from string" in {
    validGranularities
        .map(s => (QueryGranularity.fromString(s), s))
        .foreach(t => t._1.toString should be(t._2))
  }

  it should "contain a valid period" in {
    SegmentGranularity.SECOND.period should be (new Period("PT1S"))
    SegmentGranularity.MINUTE.period should be (new Period("PT1M"))
    SegmentGranularity.FIVE_MINUTE.period should be (new Period("PT5M"))
    SegmentGranularity.TEN_MINUTE.period should be (new Period("PT10M"))
    SegmentGranularity.FIFTEEN_MINUTE.period should be (new Period("PT15M"))
    SegmentGranularity.HOUR.period should be (new Period("PT1H"))
    SegmentGranularity.SIX_HOUR.period should be (new Period("PT6H"))
    SegmentGranularity.DAY.period should be (new Period("P1D"))
    SegmentGranularity.WEEK.period should be (new Period("P7D"))
    SegmentGranularity.MONTH.period should be (new Period("P1M"))
    SegmentGranularity.YEAR.period should be (new Period("P1Y"))
  }
}
