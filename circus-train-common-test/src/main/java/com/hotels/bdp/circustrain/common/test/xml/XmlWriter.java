/**
 * Copyright (C) 2016-2019 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.circustrain.common.test.xml;

import java.io.Closeable;
import java.io.Writer;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.sun.xml.bind.marshaller.XMLWriter;

public class XmlWriter implements Closeable {
  private static final Attributes EMPTY_ATTS = new AttributesImpl();

  private final XMLWriter xmlWriter;
  private final Stack<String> openedTags;

  public XmlWriter(Writer writer, String encoding) {
    openedTags = new Stack<>();
    xmlWriter = new XMLWriter(writer, encoding);
    try {
      xmlWriter.startDocument();
    } catch (SAXException e) {
      throw new RuntimeException("Unable to start document", e);
    }
  }

  public void dataElement(String name, String content) {
    try {
      xmlWriter.dataElement("", "", name, EMPTY_ATTS, content);
    } catch (SAXException e) {
      throw new RuntimeException("Unable to add data element '" + name + "'", e);
    }
  }

  public void startElement(String name) {
    try {
      xmlWriter.startElement("", "", name, EMPTY_ATTS);
      openedTags.push(name);
    } catch (SAXException e) {
      throw new RuntimeException("Unable to start element '" + name + "'", e);
    }
  }

  public void endElement() {
    if (openedTags.isEmpty()) {
      throw new IllegalStateException("No elements to end");
    }

    String name = null;
    try {
      name = openedTags.pop();
      xmlWriter.endElement("", "", name);
    } catch (SAXException e) {
      throw new RuntimeException("Unable to close element '" + name + "'", e);
    }
  }

  @Override
  public void close() {
    try {
      xmlWriter.endDocument();
    } catch (SAXException e) {
      throw new RuntimeException("Unable to end document", e);
    }
  }

}
