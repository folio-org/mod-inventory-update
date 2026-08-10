package org.folio.inventoryupdate.importing.service.delivery.fileimport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.utils.SecureSaxParser;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class SourceXmlCheck extends DefaultHandler {
  public static final Logger logger = LogManager.getLogger("XmlValidator");
  private int elementCount = 0;
  private boolean rootIsCollection = false;
  private boolean isCollectionOfRecords = false;
  private boolean valid = false;
  private String rootName = "";
  private String errorMessage;

  public SourceXmlCheck(String xml) {
    validate(xml);
  }

  private void validate(String payload) {
    try {
      InputStream inputStream = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));
      SecureSaxParser.get().parse(inputStream, this);
      setError();
    } catch (ParserConfigurationException | SAXException | IOException e) {
      valid = false;
      logger.error("SaxParsing error: {}", e.getMessage());
      errorMessage = "XML parsing error when reading source records " + e.getMessage();
    }
  }

  public boolean isInvalid() {
    return !valid;
  }

  public boolean isCollection() {
    return rootIsCollection;
  }

  public boolean isCollectionOfRecords() {
    return isCollectionOfRecords;
  }

  public String rootElement() {
    return rootName;
  }

  @Override
  public void startElement(String uri, String localName, String qualifiedName, Attributes attributes) {
    elementCount++;
    if (elementCount == 1) {
      if (localName.equals("collection")) {
        rootIsCollection = true;
      } else {
        rootName = qualifiedName;
        valid = false;
      }
    } else if (elementCount == 2 && rootIsCollection && localName.equals("record")) {
      isCollectionOfRecords = true;
      valid = true;
    }
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  private void setError() {
    String message = "";
    if (isInvalid()) {
      message = "Invalid XML input. ";
      if (isCollection() && !isCollectionOfRecords()) {
        message += "The XML is a <collection> but must contain one or more <record>s.";
      } else if (!isCollection()) {
        message += "The XML document must be a <collection> of <record>s. Found " + rootElement();
      }
    }
    this.errorMessage = message;
  }
}
