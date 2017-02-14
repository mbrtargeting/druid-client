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

sealed abstract class SegmentGranularity(periodString: String) {
  lazy val period = new Period(periodString)

  override def toString: String = this.getClass.toString.split("\\$").last
}

object SegmentGranularity {
  case object SECOND extends SegmentGranularity("PT1S")
  case object MINUTE extends SegmentGranularity("PT1M")
  case object FIVE_MINUTE extends SegmentGranularity("PT5M")
  case object TEN_MINUTE extends SegmentGranularity("PT10M")
  case object FIFTEEN_MINUTE extends SegmentGranularity("PT15M")
  case object HOUR extends SegmentGranularity("PT1H")
  case object SIX_HOUR extends SegmentGranularity("PT6H")
  case object DAY extends SegmentGranularity("P1D")
  case object WEEK extends SegmentGranularity("P7D")
  case object MONTH extends SegmentGranularity("P1M")
  case object YEAR extends SegmentGranularity("P1Y")

  def fromString(string: String): SegmentGranularity = string match {
    case "SECOND" => SECOND
    case "MINUTE" => MINUTE
    case "FIVE_MINUTE" => FIVE_MINUTE
    case "TEN_MINUTE" => TEN_MINUTE
    case "FIFTEEN_MINUTE" => FIFTEEN_MINUTE
    case "HOUR" => HOUR
    case "SIX_HOUR" => SIX_HOUR
    case "DAY" => DAY
    case "WEEK" => WEEK
    case "MONTH" => MONTH
    case "YEAR" => YEAR
  }
}
