package org.folio.inventoryupdate.importing.service.delivery.fileimport.transformation;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventoryupdate.importing.moduledata.Step;
import org.folio.inventoryupdate.importing.moduledata.TransformationStep;
import org.folio.inventoryupdate.importing.moduledata.database.Entity;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.ProcessingRecord;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.RecordReceiver;

/**
 * An XSLT transformation pipeline with an XML to JSON conversion at the end.
 */
public final class XmlTransformationPipeline implements RecordReceiver {

  public static final Logger logger = LogManager.getLogger("TransformationPipeline");
  private final List<Templates> listOfTemplates = new ArrayList<>();
  private RecordReceiver receiverOfTransformedRecord;
  private int records = 0;
  private long transformationTime = 0;
  private boolean convertToJson = true;

  private XmlTransformationPipeline(JsonObject transformation) {
    setTemplates(transformation);
  }

  public XmlTransformationPipeline withTarget(RecordReceiver receiver) {
    this.receiverOfTransformedRecord = receiver;
    records = 0;
    transformationTime = 0;
    return this;
  }

  public XmlTransformationPipeline withXmlToJsonConversion(boolean convert) {
    convertToJson = convert;
    return this;
  }

  public static Future<XmlTransformationPipeline> create(Vertx vertx, String tenant, UUID transformationId) {
    EntityStorage access = new EntityStorage(vertx, tenant);
    TransformationStep tsasDef = new TransformationStep();
    Step stepDef = new Step();
    return access.getEntities("SELECT step.* "
            + " FROM " + stepDef.schemaTable(access.schema()) + " as step,"
            + "      " + tsasDef.schemaTable(access.schema()) + " as tsa "
            + "  WHERE step.id = tsa.step_id "
            + "    AND tsa.transformation_id = '" + transformationId.toString() + "'"
            + "  ORDER BY tsa.position", stepDef)
        .map(steps -> {
          JsonObject json = new JsonObject().put("stepAssociations", new JsonArray());
          for (Entity step : steps) {
            JsonObject o = new JsonObject().put("step", step.asJson());
            o.getJsonObject("step").put("entityType", "xmlTransformationStep");
            json.getJsonArray("stepAssociations").add(o);
          }
          return new XmlTransformationPipeline(json);
        })
        .onFailure(handler -> {
          logger.error("Problem retrieving steps {}", handler.getMessage());
        });
  }

  private String transform(String xmlRecord) {
    String transformedRecord = xmlRecord;
    for (Templates templates : listOfTemplates) {
      transformedRecord = transform(transformedRecord, templates);
    }
    return transformedRecord;
  }

  private String transform(String xmlRecord, Templates templates) {
    try {
      Source sourceXml = new StreamSource(new StringReader(xmlRecord));
      StreamResult resultXmlStream = new StreamResult(new StringWriter());
      Transformer transformer = templates.newTransformer();
      transformer.transform(sourceXml, resultXmlStream);
      return resultXmlStream.getWriter().toString();
    } catch (TransformerException e) {
      logger.error("Error XSLT transforming the XML: {}, passing on original XML", e.getMessage());
      return xmlRecord;
    }
  }

  private JsonObject convertToJson(String xmlRecord) {
    return InventoryXmlToInventoryJson.convert(xmlRecord);
  }

  private void setTemplates(JsonObject transformation) {
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    for (Object o : transformation.getJsonArray("stepAssociations")) {
      JsonObject stepJson = ((JsonObject) o).getJsonObject("step");
      Step step = new Step().fromJson(stepJson);
      try {
        if (stepJson.getString("entityType").equals("xmlTransformationStep")) {
          String script = step.getLineSeparatedXslt();
          Source xslt = new StreamSource(new StringReader(script));
          listOfTemplates.add(transformerFactory.newTemplates(xslt));
        }
      } catch (Exception e) {
        logger.error("Failed to parse the XSLT template sources for step {}: {}",
            step.name(), e.getMessage());
      }
    }
  }

  @Override
  public void put(ProcessingRecord processingRecord) {
    final long transformationStarted = System.currentTimeMillis();
    records++;
    String transformedXmlRecord = transform("<collection>" + processingRecord.getRecordAsString() + "</collection>");
    if (convertToJson) {
      JsonObject jsonRecord = convertToJson(transformedXmlRecord);
      processingRecord.setIsDeletion(jsonRecord.containsKey("delete"));
      processingRecord.update(jsonRecord.encodePrettily());
    } else {
      processingRecord.update(transformedXmlRecord);
    }
    transformationTime += System.currentTimeMillis() - transformationStarted;
    receiverOfTransformedRecord.put(processingRecord);
  }

  @Override
  public void endOfDocument() {
    receiverOfTransformedRecord.endOfDocument();
  }

  @Override
  public long getProcessingTime() {
    return transformationTime;
  }

  @Override
  public int getRecordsProcessed() {
    return records;
  }
}
