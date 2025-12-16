package org.folio.inventoryupdate.unittests.fixtures;

import io.vertx.core.json.JsonObject;
import org.apache.commons.io.FileUtils;
import org.folio.inventoryupdate.unittests.ImportTests;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Objects;
import java.util.ArrayList;


public class Files {

  public static final String XSLT_EMPTY = getSampleFile("stylesheets/empty.xslt");
  public static final String XSLT_INVALID_XML = getSampleFile("stylesheets/invalidXml.xslt");
  public static final String XSLT_SYNTAX_ERROR = getSampleFile("stylesheets/xsltSyntaxError.xslt");
  public static final String XSLT_COPY_XML_DOC = getSampleFile("stylesheets/copyXmlDoc.xslt");
  private static final String INSTANCE_TYPE_ID = "30fffe0e-e985-4144-b2e2-1e8179bdb41f";

  public static final String XML_INVENTORY_RECORD_SET = getSampleFile("samplesourcefiles/inventoryRecordSet.xml");
  public static final String TWO_XML_INVENTOR_RECORD_SETS = getSampleFile("samplesourcefiles/twoInventoryRecordSets.xml");
  public static final JsonObject JSON_TRANSFORMATION_CONFIG = new JsonObject(Objects.requireNonNull(getSampleFile("configs/transformation.json")));
  public static final JsonObject JSON_CHANNEL = new JsonObject(Objects.requireNonNull(getSampleFile("configs/channel.json")));
  public static final JsonObject JSON_IMPORT_JOB = new JsonObject(Objects.requireNonNull(getSampleFile("jobs/importJob.json")));
  public static final JsonObject JSON_FAILED_RECORDS = new JsonObject(Objects.requireNonNull(getSampleFile("jobs/failed-records.json")));

  private static String getSampleFile(String filename) {
    try {
      return FileUtils.readFileToString(
              new File("src/test/resources/fixtures/" + filename), "UTF-8");
    } catch (IOException fnfe) {
      ImportTests.logger.error(fnfe.getMessage());
      return null;
    }
  }

  /**
    * Creates [numberOfFiles] files (strings of file content), each with [recordsPerFile] records.
    * @param numberOfFiles number of files to generate
    * @param recordsPerFile number of XML records to create in each file
    * @return List of files (strings of file content)
  */
  public static ArrayList<String> filesOfInventoryXmlRecords(int numberOfFiles, int recordsPerFile, String fakedResponseStatus) {
      ArrayList<String> sourceFiles = new ArrayList<>();
      for (int files = 0; files < numberOfFiles; files++) {
          int startRecord = files*recordsPerFile+1;
          sourceFiles.add(createCollectionOfInventoryXmlRecordsWithDeletes(startRecord, startRecord+recordsPerFile-1, fakedResponseStatus));
      }
      return sourceFiles;
  }

   /**
   * Generates an XML document, a `collection` of simple Inventory XML `record`s, each record given a unique instance
   * HRID and title using the numbers in the provided interval
   * @param firstRecord  The number for the first record in the series
   * @param lastRecord  The number of the last record in the series
   * @param fakedResponseStatus The status that fake inventory update should return on PUT
   * @param deletesPositions Insert delete records at given positions in batch
   * @return a number of XML records (total records = lastRecord - firstRecord)
   */
  public static String createCollectionOfInventoryXmlRecordsWithDeletes(int firstRecord, int lastRecord, String fakedResponseStatus, int ... deletesPositions)  {
      CollectionOfXmlRecords collection = new CollectionOfXmlRecords();
      for (int i=firstRecord; i<=lastRecord; i++) {
          if (arrayHasInt(deletesPositions, i)) {
              collection.addDeleteRecord(i);
          } else {
              collection.addUpsertRecord(i, fakedResponseStatus);
          }
      }
      return collection.asXmlString();
  }

  public static boolean arrayHasInt (int[] arr, int i) {
      return Arrays.stream(arr).anyMatch(v -> v == i);
  }

  public static String createCollectionOfOneDeleteRecord(int hrid) {
      CollectionOfXmlRecords collection = new CollectionOfXmlRecords();
      collection.addDeleteRecord(hrid);
      return collection.asXmlString();
  }

  public static class CollectionOfXmlRecords {

      Document collection;
      ArrayList<Element> records = new ArrayList<>();
      public CollectionOfXmlRecords() {
          try {
              DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
              DocumentBuilder builder = factory.newDocumentBuilder();
              collection = builder.newDocument();
              Element root = collection.createElement("collection");
              collection.appendChild(root);
          } catch (ParserConfigurationException pce) {
              throw new RuntimeException(pce);
          }
      }

      public void addUpsertRecord(int recNo, String fakedResponseStatus) {
          Element theRecord = collection.createElement("record");
          theRecord.appendChild(createInstance(recNo, fakedResponseStatus));
          records.add(theRecord);
      }

      public void addDeleteRecord(int hrid) {
          Element theRecord = collection.createElement("record");
          theRecord.appendChild(createDelete(hrid));
          records.add(theRecord);
      }

      public Document asDocument() {
          for (Element theRecord : records) {
              collection.getDocumentElement().appendChild(theRecord);
          }
          return collection;
      }

      public String asXmlString() {
          StringWriter sw = new StringWriter();
          try {
              TransformerFactory transformerFactory = TransformerFactory.newInstance();
              Transformer transformer = transformerFactory.newTransformer();
              transformer.setOutputProperty(OutputKeys.INDENT, "yes");
              DOMSource source = new DOMSource(this.asDocument());
              transformer.transform(source, new StreamResult(sw));
          } catch (TransformerException e) {
              throw new RuntimeException(e);
          }
          return sw.toString();
      }

      private Element createInstance(int recNo, String fakedResponseStatus) {
          Element instance = collection.createElement("instance");
          instance.appendChild(createTextElement("source", "SAMPLES-"+fakedResponseStatus));
          instance.appendChild(createTextElement("hrid", recNo));
          instance.appendChild(createTextElement("title", "Title " + recNo));
          instance.appendChild(createTextElement("instanceTypeId", INSTANCE_TYPE_ID));
          return instance;
      }

      private Element createDelete(int hrid) {
          Element delete = collection.createElement("delete");
          delete.appendChild(createTextElement("hrid", hrid));
          return delete;
      }

      private Element createTextElement(String name, Object value) {
          Element element = collection.createElement(name);
          element.appendChild(collection.createTextNode(value.toString()));
          return element;
      }

  }
}
