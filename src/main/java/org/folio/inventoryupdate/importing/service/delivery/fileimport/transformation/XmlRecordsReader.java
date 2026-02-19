package org.folio.inventoryupdate.importing.service.delivery.fileimport.transformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.ProcessingException;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.ProcessingRecord;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.RecordProvider;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.RecordReceiver;
import org.folio.inventoryupdate.importing.utils.SecureSaxParser;
import org.folio.reservoir.util.EncodeXmlText;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class XmlRecordsReader extends DefaultHandler implements RecordProvider, Callable<Void> {

  public static final Logger logger = LogManager.getLogger("XmlRecordsFromFile");
  StringBuilder theRecord = new StringBuilder();
  StringBuilder theCollectionElement = new StringBuilder();
  RecordReceiver target;
  final String xmlCollectionOfRecords;

  public XmlRecordsReader(String recordsSource, RecordReceiver target) {
    this.xmlCollectionOfRecords = recordsSource;
    this.target = target;
  }

  @Override
  public void provideRecords() throws ProcessingException {
    try {
      InputStream inputStream = new ByteArrayInputStream(xmlCollectionOfRecords.getBytes(StandardCharsets.UTF_8));
      SecureSaxParser.get().parse(inputStream, this);
    } catch (ParserConfigurationException | SAXException | IOException e) {
      logger.error("SaxParsing error: {}", e.getMessage());
      throw new ProcessingException("XML parsing error when reading source records " + e.getMessage());
    }
  }

  @Override
  public void startElement(String uri, String localName, String qualifiedName, Attributes attributes) {
    if (localName.equalsIgnoreCase("collection") && theRecord.isEmpty()) {
      theCollectionElement.append("<").append(qualifiedName);
      for (int index = 0; index < attributes.getLength(); index++) {
        theCollectionElement.append(" ")
            .append(attributes.getQName(index)).append("=\"").append(attributes.getValue(index)).append("\"");
      }
      theCollectionElement.append(">");
    } else {
      if (localName.equalsIgnoreCase("record")) {
        theRecord = new StringBuilder();
      }
      theRecord.append("<").append(qualifiedName);
      for (int index = 0; index < attributes.getLength(); index++) {
        theRecord.append(" ")
            .append(attributes.getQName(index)).append("=\"").append(attributes.getValue(index)).append("\"");
      }
      theRecord.append(">");
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) {
    String text = new String(ch, start, length);
    theRecord.append(EncodeXmlText.encodeXmlText(text));
  }

  @Override
  public void endElement(String uri, String localName, String qqName) {
    theRecord.append("</").append(qqName).append(">");
    if (localName.equals("record")) {
      String collectionOfOneRecord =
          theCollectionElement
          + System.lineSeparator()
          + "  " + theRecord
          + System.lineSeparator()
          + "</collection>";
      target.put(new ProcessingRecord(collectionOfOneRecord));
      theRecord = new StringBuilder();
    }
  }

  @Override
  public void endDocument() {
    target.endOfDocument();
  }

  @Override
  public Void call() throws Exception {
    provideRecords();
    return null;
  }
}
