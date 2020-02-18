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

package eu.m6r.druid.client

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.joda.time.DateTime

package object models {

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class DruidHost(name: String, address: String, port: Int)

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class TaskStatus(task: String, status: TaskStatus.Status)

  object TaskStatus {

    @JsonIgnoreProperties(ignoreUnknown = true)
    case class Status(id: String, status: String, duration: Long)

  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class RunningTask(id: String, createdTime: DateTime, queueInsertionTime: DateTime,
                         location: Location)

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class Location(host: String, port: Int)

}
