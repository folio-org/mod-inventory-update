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
  private boolean isCollection = false;
  private boolean isCollectionOfRecords = false;
  private boolean isRecord = false;
  private boolean valid = false;
  private String rootName = "";
  private boolean isValidated = false;

  public void validate(String payload) throws ProcessingException {
    try {
      InputStream inputStream = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));
      SecureSaxParser.get().parse(inputStream, this);
    } catch (ParserConfigurationException | SAXException | IOException e) {
      logger.error("SaxParsing error: {}", e.getMessage());
      throw new ProcessingException("XML parsing error when reading source records " + e.getMessage());
    }
  }

  public boolean isValidated() {
    return isValidated;
  }

  public boolean isValid() {
    return valid;
  }

  public boolean isCollection() {
    return isCollection;
  }

  public boolean isCollectionOfRecords() {
    return isCollectionOfRecords;
  }

  public boolean isRecord() {
    return isRecord;
  }

  public String rootElement() {
    return rootName;
  }

  @Override
  public void startElement(String uri, String localName, String qualifiedName, Attributes attributes) {
    elementCount++;
    if (elementCount == 1) {
      if (localName.equals("collection")) {
        isCollection = true;
      } else if (localName.equals("record")) {
        isRecord = true;
        valid = true;
      } else {
        rootName = qualifiedName;
        valid = false;
      }
    } else if (elementCount == 2) {
      if (isCollection && localName.equals("record")) {
        isCollectionOfRecords = true;
        valid = true;
      } else if (!isRecord) {
        valid = false;
      }
    }
    isValidated = true;
  }

  public String error() {
    if (isValidated()) {
      if (!isValid()) {
        String errorMessage = "Invalid XML input. ";
        if (isCollection() && !isCollectionOfRecords()) {
          errorMessage += "The XML is a <collection> but must contain one or more <record>s.";
        } else if (!isCollectionOfRecords() && !isRecord()) {
          errorMessage += "The XML document must be a <collection> of <record>s or a single <record>. Found "
              + rootElement();
        }
        return errorMessage;
      } else {
        return "";
      }
    } else {
      throw new UnsupportedOperationException("Cannot invoke error() before the document is validated");
    }
  }
}
