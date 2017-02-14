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

package eu.m6r.druid

import java.nio.file.Path
import java.nio.file.Paths

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.language.implicitConversions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.twitter
import com.twitter.util.Return
import com.twitter.util.ScheduledThreadPoolTimer
import com.twitter.util.Throw
import scopt.Read

package object client {
  private[druid] lazy val objectMapper = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.registerModule(new JodaModule)
    objectMapper
  }

  implicit val timer = new ScheduledThreadPoolTimer()

  implicit def twitterFutureToScala[T](tf: twitter.util.Future[T]): Future[T] = {
    val promise = Promise[T]

    tf respond {
      case Return(r) => promise success r
      case Throw(e) => promise failure e
    }
    promise.future
  }

  implicit def scalaDurationToTwitter(duration: Duration): com.twitter.util.Duration = {
    com.twitter.util.Duration(duration.length, duration.unit)
  }

  implicit def tripleTupleOptionRead[A1: Read, A2: Read, A3: Read]: Read[(A1, A2, Option[A3])] =
    new Read[(A1, A2, Option[A3])] {
      val arity = 2
      val reads: String => (A1, A2, Option[A3]) = { (s: String) =>
        s.split(',') match {
          case Array(first, second) => (
                implicitly[Read[A1]].reads(first),
                implicitly[Read[A2]].reads(second),
                None)
          case Array(first, second, third) => (
              implicitly[Read[A1]].reads(first),
              implicitly[Read[A2]].reads(second),
              Some(implicitly[Read[A3]].reads(third)))
        }
      }
    }

  implicit def pathOptionRead: Read[Path] =
    new Read[Path] {
      val arity = 1
      val reads: String => Path = { (s: String) =>
        Paths.get(s)
      }
    }
}
