package org.folio.inventoryupdate.importing.utils;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;

public final class SecureSaxParser {
  private SecureSaxParser() {
    throw new UnsupportedOperationException("Utility class");
  }

  /**
   * A new {@link SAXParser} with external access disabled to prevent
   * XML External Entity (XXE) vulnerabilities. It is not thread-safe.
   */
  public static SAXParser get() throws ParserConfigurationException, SAXException {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    SAXParser saxParser = factory.newSAXParser();
    saxParser.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    return saxParser;
  }
}
