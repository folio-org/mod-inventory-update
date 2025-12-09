package org.folio.inventoryupdate.importing.service.provisioning;

import static org.folio.inventoryupdate.importing.service.provisioning.GenericRequests.deleteEntity;
import static org.folio.inventoryupdate.importing.service.provisioning.GenericRequests.getEntities;
import static org.folio.inventoryupdate.importing.service.provisioning.GenericRequests.getEntity;
import static org.folio.inventoryupdate.importing.service.provisioning.GenericRequests.storeEntityRespondWith201;
import static org.folio.okapi.common.HttpResponse.responseText;

import io.vertx.core.Future;
import java.util.UUID;
import org.folio.inventoryupdate.importing.moduledata.Step;
import org.folio.inventoryupdate.importing.moduledata.Transformation;
import org.folio.inventoryupdate.importing.moduledata.TransformationStep;
import org.folio.inventoryupdate.importing.moduledata.database.ModuleStorageAccess;
import org.folio.inventoryupdate.importing.service.ServiceRequest;

public final class Transformations {

  private Transformations() {
    throw new IllegalStateException("Static storage utilities");
  }

  public static Future<Void> postStep(ServiceRequest request) {
    Step step = new Step().fromJson(request.bodyAsJson());
    String validationResponse = step.validateScriptAsXml();
    if (validationResponse.equals("OK")) {
      return storeEntityRespondWith201(request, step);
    } else {
      return Future.failedFuture(validationResponse);
    }
  }

  public static Future<Void> putStep(ServiceRequest request) {
    Step step = new Step().fromJson(request.bodyAsJson());
    String validationResponse = step.validateScriptAsXml();
    if (validationResponse.equals("OK")) {
      UUID id = UUID.fromString(request.requestParam("id"));
      return request.moduleStorageAccess().updateEntity(id, step.withUpdatingUser(request.currentUser()))
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
    return getEntities(request, new Step());
  }

  public static Future<Void> getStepById(ServiceRequest request) {
    return getEntity(request, new Step());
  }

  public static Future<Void> deleteStep(ServiceRequest request) {
    return deleteEntity(request, new Step());
  }

  public static Future<Void> getScript(ServiceRequest request) {

    return request.moduleStorageAccess().getScript(request)
        .onSuccess(script -> responseText(request.routingContext(), 200).end(script)).mapEmpty();
  }

  public static Future<Void> putScript(ServiceRequest request) {
    String validationResponse = Step.validateScriptAsXml(request.bodyAsString());
    if (validationResponse.equals("OK")) {
      return request.moduleStorageAccess().putScript(request)
          .onSuccess(script -> responseText(request.routingContext(), 204).end()).mapEmpty();
    } else {
      return Future.failedFuture(validationResponse);
    }
  }

  public static Future<Void> postTransformation(ServiceRequest request) {
    Transformation transformation = new Transformation().fromJson(request.bodyAsJson());
    return request.moduleStorageAccess().storeEntity(transformation.withCreatingUser(request.currentUser()))
        .compose(transformationId ->
            request.moduleStorageAccess().storeEntities(transformation.getListOfTransformationSteps()))
        .onSuccess(res -> responseText(request.routingContext(), 201).end(transformation.asJson().encodePrettily()));
  }

  public static Future<Void> getTransformationById(ServiceRequest request) {
    return getEntity(request, new Transformation());
  }

  public static Future<Void> getTransformations(ServiceRequest request) {
    return getEntities(request, new Transformation());
  }

  public static Future<Void> updateTransformation(ServiceRequest request) {
    Transformation transformation = new Transformation().fromJson(request.bodyAsJson());
    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().updateEntity(id, transformation.withUpdatingUser(request.currentUser()))
        .onSuccess(result -> {
          if (result.rowCount() == 1) {
            if (transformation.containsListOfSteps()) {
              new TransformationStep().deleteStepsOfTransformation(request, transformation.getRecord().id())
                  .compose(ignore ->
                      request.moduleStorageAccess().storeEntities(transformation.getListOfTransformationSteps()))
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
    return deleteEntity(request, new Transformation());
  }

  public static Future<Void> postTransformationStep(ServiceRequest request) {
    TransformationStep transformationStep = new TransformationStep().fromJson(request.bodyAsJson());
    return transformationStep.createTsaRepositionSteps(request)
        .onSuccess(result -> responseText(request.routingContext(), 201).end());
  }

  public static Future<Void> getTransformationStepById(ServiceRequest request) {
    return getEntity(request, new TransformationStep());
  }

  public static Future<Void> getTransformationSteps(ServiceRequest request) {
    return getEntities(request, new TransformationStep());
  }

  public static Future<Void> putTransformationStep(ServiceRequest request) {
    TransformationStep transformationStep = new TransformationStep().fromJson(request.bodyAsJson());

    UUID id = UUID.fromString(request.requestParam("id"));
    return request.moduleStorageAccess().getEntity(id, transformationStep).compose(existingTsa -> {
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
    ModuleStorageAccess db = request.moduleStorageAccess();
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
}
