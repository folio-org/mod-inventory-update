package org.folio.inventoryimport.utils;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;

public final class SecureSaxParser {
  private SecureSaxParser() {
  }

  /**
   * A new {@link SAXParser} with external access disabled to prevent
   * XML External Entity (XXE) vulnerabilities. It is not thread-safe.
   */
  public static SAXParser get() throws ParserConfigurationException, SAXException {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    SAXParser saxParser = factory.newSAXParser();
    saxParser.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    return saxParser;
  }
}
