package org.folio.inventoryupdate.importing.service.delivery.respond;

import static org.folio.inventoryupdate.importing.service.delivery.respond.Channels.getChannelByTagOrUuid;
import static org.folio.okapi.common.HttpResponse.responseJson;
import static org.folio.okapi.common.HttpResponse.responseText;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.Step;
import org.folio.inventoryupdate.importing.moduledata.Transformation;
import org.folio.inventoryupdate.importing.moduledata.TransformationStep;
import org.folio.inventoryupdate.importing.moduledata.database.EntityStorage;
import org.folio.inventoryupdate.importing.service.ServiceRequest;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.transformation.XmlRecordsReader;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.transformation.XmlTransformationEcho;
import org.folio.inventoryupdate.importing.service.delivery.fileimport.transformation.XmlTransformationPipeline;

public final class Transformations extends EntityResponses {

  private Transformations() {
    throw new UnsupportedOperationException("Static storage utilities");
  }

  public static Future<Void> postStep(ServiceRequest request) {
    Step step = new Step().fromJson(request.bodyAsJson());
    String validationResponse = step.validateStyleSheet();
    if (validationResponse.equals("OK")) {
      return storeEntityRespondWith201(request, step);
    } else {
      return Future.failedFuture(validationResponse);
    }
  }

  public static Future<Void> putStep(ServiceRequest request) {
    Step step = new Step().fromJson(request.bodyAsJson());
    String validationResponse = step.validateStyleSheet();
    if (validationResponse.equals("OK")) {
      UUID id = UUID.fromString(request.requestParam("id"));
      return request.entityStorage().updateEntity(id, step.withUpdatingUser(request.currentUser()))
          .onSuccess(result -> {
            if (result.rowCount() == 1) {
              responseText(request.routingContext(), 204).end();
            } else {
              responseText(request.routingContext(), 404).end("Not found");
            }
          }).mapEmpty();
    } else {
      return Future.failedFuture(validationResponse);
    }
  }

  public static Future<Void> getSteps(ServiceRequest request) {
    return getEntitiesAndRespond(request, new Step());
  }

  public static Future<Void> getStepById(ServiceRequest request) {
    return getEntityAndRespond(request, new Step());
  }

  public static Future<Void> deleteStep(ServiceRequest request) {
    return deleteEntityAndRespond(request, new Step());
  }

  public static Future<Void> getScript(ServiceRequest request) {
    String id = request.requestParam("id");
    return request.entityStorage().getEntity(UUID.fromString(id), new Step()).onComplete(step -> {
      if (step.result() != null) {
        responseText(request.routingContext(), 200).end(((Step) step.result()).getLineSeparatedXslt()).mapEmpty();
      } else {
        responseText(request.routingContext(), 404)
            .end("Could not find step with ID " + id + " to GET script from").mapEmpty();
      }
    }).mapEmpty();
  }

  public static Future<Void> putScript(ServiceRequest request) {
    String id = request.requestParam("id");
    String script = request.bodyAsString();
    String validationResponse = Step.validateStyleSheet(script);
    if (validationResponse.equals("OK")) {
      EntityStorage db = request.entityStorage();
      return db.getEntity(UUID.fromString(id), new Step())
          .onComplete(getStep -> {
            if (getStep.result() != null) {
              Step step = (Step) getStep.result().withUpdatingUser(request.currentUser());
              step.updateScript(script, db)
                  .onComplete(na -> responseText(request.routingContext(), 204).end()).mapEmpty();
            } else {
              responseText(request.routingContext(), 404).end("Update script: Step not found").mapEmpty();
            }
          }).mapEmpty();
    } else {
      return Future.failedFuture(validationResponse + System.lineSeparator() + "Found in stylesheet for step ID " + id);
    }
  }

  public static Future<Void> postTransformation(ServiceRequest request) {
    Transformation transformation = new Transformation().fromJson(request.bodyAsJson());
    return request.entityStorage().storeEntity(transformation.withCreatingUser(request.currentUser()))
        .compose(transformationId ->
            request.entityStorage().storeEntities(transformation.getListOfTransformationSteps()))
        .onSuccess(res -> responseText(request.routingContext(), 201).end(transformation.asJson().encodePrettily()));
  }

  public static Future<Void> getTransformationById(ServiceRequest request) {
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.entityStorage().getEntity(id, new Transformation())
        .compose(transformation -> {
          if (transformation == null) {
            return responseText(request.routingContext(), 404).end("Transformation with ID " + id + " not found.");
          } else {
            return ((Transformation)transformation).fetchTransformationSteps(request.entityStorage().getTenantPool())
                .compose(done ->
                    responseJson(request.routingContext(),200).end(transformation.asJson().encodePrettily()));
          }
        });
  }

  public static Future<Void> getTransformations(ServiceRequest request) {
    return getEntitiesAndRespond(request, new Transformation());
  }

  public static Future<Void> updateTransformation(ServiceRequest request) {
    Transformation transformation = new Transformation().fromJson(request.bodyAsJson());
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.entityStorage().updateEntity(id, transformation.withUpdatingUser(request.currentUser()))
        .onSuccess(result -> {
          if (result.rowCount() == 1) {
            if (transformation.containsListOfSteps()) {
              new TransformationStep().deleteStepsOfTransformation(request, transformation.getRecord().id())
                  .compose(ignore ->
                      request.entityStorage().storeEntities(transformation.getListOfTransformationSteps()))
                  .onSuccess(res -> responseText(request.routingContext(), 204).end());
            } else {
              responseText(request.routingContext(), 204).end();
            }
          } else {
            responseText(request.routingContext(), 404).end("Not found");
          }
        }).mapEmpty();
  }

  public static Future<Void> deleteTransformation(ServiceRequest request) {
    return deleteEntityAndRespond(request, new Transformation());
  }

  public static Future<Void> postTransformationStep(ServiceRequest request) {
    TransformationStep transformationStep = new TransformationStep().fromJson(request.bodyAsJson());
    return transformationStep.createTsaRepositionSteps(request)
        .onSuccess(result -> responseText(request.routingContext(), 201).end());
  }

  public static Future<Void> getTransformationStepById(ServiceRequest request) {
    return getEntityAndRespond(request, new TransformationStep());
  }

  public static Future<Void> getTransformationSteps(ServiceRequest request) {
    return getEntitiesAndRespond(request, new TransformationStep());
  }

  public static Future<Void> putTransformationStep(ServiceRequest request) {
    TransformationStep transformationStep = new TransformationStep().fromJson(request.bodyAsJson());

    UUID id = UUID.fromString(request.requestParam("id"));
    return request.entityStorage().getEntity(id, transformationStep).compose(existingTsa -> {
      if (existingTsa == null) {
        responseText(request.routingContext(), 404).end("Not found");
      } else {
        Integer positionOfExistingTsa = ((TransformationStep) existingTsa).getRecord().position();
        transformationStep.updateTsaRepositionSteps(request, positionOfExistingTsa)
            .onSuccess(result -> responseText(request.routingContext(), 204).end());
      }
      return Future.succeededFuture();
    });
  }

  public static Future<Void> deleteTransformationStep(ServiceRequest request) {
    UUID id = UUID.fromString(request.requestParam("id"));
    EntityStorage db = request.entityStorage();
    return db.getEntity(id, new TransformationStep()).compose(existingTsa -> {
      if (existingTsa == null) {
        responseText(request.routingContext(), 404).end("Not found");
      } else {
        Integer positionOfExistingTsa = ((TransformationStep) existingTsa).getRecord().position();
        ((TransformationStep) existingTsa).deleteTsaRepositionSteps(db.getTenantPool(), positionOfExistingTsa)
            .onSuccess(result -> responseText(request.routingContext(), 200).end());
      }
      return Future.succeededFuture();
    });
  }

  public static Future<Void> tryTransformation(ServiceRequest request)  {
    String channelId = request.requestParam("id");
    String output = request.queryParam("output", "json");
    Buffer xmlContent = Buffer.buffer(request.bodyAsString());
    return getChannelByTagOrUuid(request, channelId).compose(channel -> {
      if (channel == null) {
        return responseText(request.routingContext(), 404)
            .end("Could not find channel with id or tag [" + channelId + "] to try transformation in.").mapEmpty();
      } else {

        return XmlTransformationPipeline
            .create(request.vertx(), request.tenant(), channel.getTransformationId())
            .compose(pipeline -> {
              pipeline.withXmlToJsonConversion(!output.equalsIgnoreCase("xml"));
              pipeline.withTarget(new XmlTransformationEcho(request.routingContext()));
              try {
                new XmlRecordsReader(xmlContent.toString(StandardCharsets.UTF_8), pipeline).provideRecords();
              } catch (Exception e) {
                return Future.failedFuture(e.getMessage());
              }
              return Future.succeededFuture(pipeline);
            }).mapEmpty();
      }
    });
  }
}
