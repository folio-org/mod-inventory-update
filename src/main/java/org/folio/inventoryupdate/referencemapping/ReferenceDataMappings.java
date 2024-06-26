package org.folio.inventoryupdate.referencemapping;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventoryupdate.InventoryStorage;
import org.folio.inventoryupdate.entities.*;
import org.folio.okapi.common.OkapiClient;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ReferenceDataMappings {

  private static final Map<String, Map<ReferenceApi, Map<String, Map<String, String>>>> referenceDataByTenant = new ConcurrentHashMap<>();

  private static Map<ReferenceApi, Map<String, Map<String, String>>> getTenantMappings (String tenant) {
    if (!referenceDataByTenant.containsKey(tenant)) {
      referenceDataByTenant.put(tenant, new ConcurrentHashMap<>());
    }
    return referenceDataByTenant.get(tenant);
  }

  public static boolean hasMappings(String tenant) {
    return referenceDataByTenant.containsKey(tenant);
  }

  private static Map<String,Map<String, String>> getFieldMappingsFromReferenceApi (String tenant, ReferenceApi referenceApi) {
    Map<ReferenceApi, Map<String, Map<String, String>>> referenceApis = getTenantMappings(tenant);
    if (!referenceApis.containsKey(referenceApi)) {
      referenceApis.put(referenceApi, new ConcurrentHashMap<>());
    }
    return referenceApis.get(referenceApi);
  }

  private static Map<String,String> getApiFieldMap (String tenant, ReferenceApi referenceApi) {
    Map<String, Map<String, String>> apiMap = getFieldMappingsFromReferenceApi(tenant, referenceApi);
    if (!apiMap.containsKey(referenceApi.getAlternateKey())) {
      apiMap.put(referenceApi.getAlternateKey(), new ConcurrentHashMap<>());
    }
    return  apiMap.get(referenceApi.getAlternateKey());
  }

  /**
   * Traverses batch of incoming records sets to find occurrences in instance, holdings and items of foreign key references
   * expressed by their unique names (rather than as UUIDs), and constructs Inventory storage queries to look up the
   * corresponding UUIDs by name in the referenced APIs. Caches the mappings by tenant.
   *
   * @return Reference data look-up futures.
   */
  public static Collection<Future<Void>> createReferenceDataLookupFutures(RoutingContext routingContext, List<PairedRecordSets> irsPairs) {
    // Find non-UUID values in foreign key properties across instance, holdings and item records.
    Map<String, AlternateFKValues> accumulatedList = findDistinctAlternateFKValues(irsPairs);
    // Create futures for distinct alternate keys.
    Collection<Future<Void>> refDataUuidFutures = new ArrayList<>();
    for (AlternateFKValues alternateReferences : accumulatedList.values() ) {
      for (String alternateId : alternateReferences.getAlternateIds()) {
        refDataUuidFutures.add(ReferenceDataMappings.cacheAlternateKeyToUuidMapping(routingContext, alternateReferences.getReferenceApi(), alternateId));
      }
    }
    return refDataUuidFutures;
  }

  private static Map<String, AlternateFKValues> findDistinctAlternateFKValues(List<PairedRecordSets> irsPairs) {
    Map<String, AlternateFKValues> accumulatedList = new HashMap<>();
    for (PairedRecordSets pair : irsPairs) {
      InventoryRecordSet irs = pair.getIncomingRecordSet();
      accumulateDistinctAlternateFKValues(accumulatedList,irs.getInstance());
      for (HoldingsRecord holdingsRecord : irs.getHoldingsRecords()) {
        accumulateDistinctAlternateFKValues(accumulatedList, holdingsRecord);
      }
      for (Item item : irs.getItems()) {
        accumulateDistinctAlternateFKValues(accumulatedList, item);
      }
    }
    return accumulatedList;
  }

  private static void accumulateDistinctAlternateFKValues(Map<String, AlternateFKValues> accumulatedList, InventoryRecord entity) {
    for (AlternateFKValues altValues : entity.findAlternateFKValues()) {
      if (accumulatedList.containsKey(altValues.api.getPath())) {
        accumulatedList.get(altValues.api.getPath()).addAlternateIds(altValues.alternateIds);
      } else {
        accumulatedList.put(altValues.api.getPath(), altValues);
      }
    }
  }

  /**
   * If a unique reference data name is not already cached, looks up the reference record by 'name', and caches the mapping
   * of the name to the reference record UUID.
   * @param referenceApi The API referenced by the foreign key value
   * @param value The token (a name or code that is uniquely indexed in the reference data api).
   * @return A future with no result (the result is stored in the mapping cache
   */
  public static Future<Void> cacheAlternateKeyToUuidMapping(RoutingContext routingContext, ReferenceApi referenceApi, String value) {
    Promise<Void> promise = Promise.promise();
    Map<String, String> apiFieldMap = getApiFieldMap(getTenant(routingContext), referenceApi);
    if (!apiFieldMap.containsKey(value)) {
      OkapiClient okapiClient = InventoryStorage.getOkapiClient(routingContext);
      String query = referenceApi.getPath()+"?query="+referenceApi.getAlternateKey()+"=="+ URLEncoder.encode(value, StandardCharsets.UTF_8);
      okapiClient.get(query,
          res -> {
        if (res.succeeded()) {
          JsonArray records = new JsonObject(res.result()).getJsonArray(referenceApi.getArrayName());
          if (records != null) {
            if (records.size() == 1) {
              apiFieldMap.put(value, records.getJsonObject(0).getString("id"));
            }
          }
          promise.complete();
        } else {
          promise.complete();
        }
      });
    } else {
      promise.complete();
    }
    return promise.future();
  }

  public static String getCachedUuidByAlternateKeyValue(String tenant, ReferenceApi referenceApi, String value) {
    return getApiFieldMap(tenant, referenceApi).get(value);
  }

  public static String getTenant(RoutingContext routingContext) {
    String tenant = routingContext.request().getHeader("x-okapi-tenant");
    return tenant == null ? "NONE" : tenant;
  }
}
