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

sealed abstract class QueryGranularity {
  override def toString: String = this.getClass.toString.split("\\$").last
}

object QueryGranularity {
  case object NONE extends QueryGranularity
  case object SECOND extends QueryGranularity
  case object MINUTE extends QueryGranularity
  case object FIVE_MINUTE extends QueryGranularity
  case object TEN_MINUTE extends QueryGranularity
  case object FIFTEEN_MINUTE extends QueryGranularity
  case object HOUR extends QueryGranularity
  case object SIX_HOUR extends QueryGranularity
  case object DAY extends QueryGranularity
  case object WEEK extends QueryGranularity
  case object MONTH extends QueryGranularity
  case object YEAR extends QueryGranularity

  def fromString(string: String): QueryGranularity = string match {
    case "NONE" => NONE
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
