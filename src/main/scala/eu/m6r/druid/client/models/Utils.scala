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

import java.io.File
import javax.xml.bind.JAXBContext

import org.eclipse.persistence.jaxb.UnmarshallerProperties

object Utils {
  def parseTaskConfig(file: File): TaskConfig = {
    val jaxbContext = JAXBContext.newInstance(classOf[TaskConfig])
    val unmarshaller = jaxbContext.createUnmarshaller()

    if (file.getName.endsWith("json")) {
      unmarshaller.setProperty(UnmarshallerProperties.MEDIA_TYPE, "application/json")
    }

    unmarshaller.unmarshal(file).asInstanceOf[TaskConfig]
  }
}
