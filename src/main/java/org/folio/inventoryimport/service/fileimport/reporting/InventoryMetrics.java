package org.folio.inventoryimport.service.fileimport.reporting;

import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Entity.INSTANCE;
import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Entity.HOLDINGS_RECORD;
import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Entity.ITEM;
import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Transaction.CREATE;
import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Transaction.DELETE;
import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Transaction.UPDATE;
import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Transaction.PROVISIONAL_INSTANCE;
import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Outcome.COMPLETED;
import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Outcome.FAILED;
import static org.folio.inventoryimport.service.fileimport.reporting.InventoryMetrics.Outcome.SKIPPED;


public class InventoryMetrics {


    public final Map<Entity,Map<Transaction, Map<Outcome,Integer>>> metrics = new HashMap<>();
    public enum Entity {
        INSTANCE,
        HOLDINGS_RECORD,
        ITEM,
        INSTANCE_RELATIONSHIP,
        INSTANCE_TITLE_SUCCESSION
    }

    public enum Transaction {
        CREATE,
        UPDATE,
        DELETE,
        PROVISIONAL_INSTANCE
    }

    public enum Outcome {
        PENDING,
        COMPLETED,
        FAILED,
        SKIPPED
    }

    public InventoryMetrics() {
        for (Entity entity : Entity.values()) {
            metrics.put(entity, new HashMap<>());
            for (Transaction transaction : Transaction.values()) {
                metrics.get(entity).put(transaction, new HashMap<>());
                for (Outcome outcome : Outcome.values()) {
                    metrics.get(entity).get(transaction).put(outcome,0);
                }
            }

        }
    }

    public InventoryMetrics(JsonObject metricsJson) {
        for (Entity entity : Entity.values()) {
            metrics.put(entity, new HashMap<>());
            if (Arrays.asList(INSTANCE, HOLDINGS_RECORD, ITEM). contains(entity)) {
                for (Transaction transaction : Arrays.asList(CREATE, UPDATE, DELETE))  {
                    metrics.get(entity).put(transaction, new HashMap<>());
                    for (Outcome outcome : Outcome.values()) {
                        int outcomeCount = metricsJson.getJsonObject(entity.name()).getJsonObject(transaction.name())
                                .getInteger(outcome.name());
                        metrics.get(entity).get(transaction).put(outcome, outcomeCount);
                    }
                }
            } else {
                if (metricsJson.containsKey(entity.name())) {
                    for (Transaction transaction : Arrays.asList(CREATE, DELETE, PROVISIONAL_INSTANCE)) {
                        if (metricsJson.getJsonObject(entity.name()).containsKey(transaction.name())) {
                            metrics.get(entity).put(transaction, new HashMap<>());
                            for (Outcome outcome : Outcome.values()) {
                                int outcomeCount = metricsJson.getJsonObject(entity.name()).getJsonObject(transaction.name())
                                        .getInteger(outcome.name());
                                metrics.get(entity).get(transaction).put(outcome, outcomeCount);
                            }
                        }
                    }
                }
            }
        }
    }

    public void add(InventoryMetrics delta) {
        for (Entity entity : delta.metrics.keySet()) {
            for (Transaction transaction : delta.metrics.get(entity).keySet()) {
                for (Outcome outcome : delta.metrics.get(entity).get(transaction).keySet()) {
                    int outcomeCount = this.metrics.get(entity).get(transaction).get(outcome);
                    int outcomesDelta = delta.metrics.get(entity).get(transaction).get(outcome);
                    this.metrics.get(entity).get(transaction).put(outcome,outcomeCount+outcomesDelta);
                }
            }
        }
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        for (Entity entity : metrics.keySet()) {
            str.append(entity).append(": ");
            for (Transaction transaction : metrics.get(entity).keySet()) {
                for (Outcome outcome : metrics.get(entity).get(transaction).keySet()) {
                    int count = this.metrics.get(entity).get(transaction).get(outcome);
                    str.append(outcome).append(" ").append(transaction).append(": ")
                            .append(count).append(".");
                }
            }
            str.append("\n");
        }
        return str.toString();
    }

    public String report() {
        return "Instance creates: " + metrics.get(INSTANCE).get(CREATE).get(COMPLETED) + ". " +
                " Failed: " + metrics.get(INSTANCE).get(CREATE).get(FAILED) +
                " Skipped: " + metrics.get(INSTANCE).get(CREATE).get(SKIPPED) + "\n" +
                "Instance updates: " + metrics.get(INSTANCE).get(UPDATE).get(COMPLETED) + ". " +
                " Failed: " + metrics.get(INSTANCE).get(UPDATE).get(FAILED) +
                " Skipped: " + metrics.get(INSTANCE).get(UPDATE).get(SKIPPED) + "\n" +
                "Instance deletes: " + metrics.get(INSTANCE).get(DELETE).get(COMPLETED) + ". " +
                " Failed: " + metrics.get(INSTANCE).get(DELETE).get(FAILED) +
                " Skipped: " + metrics.get(INSTANCE).get(DELETE).get(SKIPPED) + "\n" +
                "Holdings records creates: " + metrics.get(HOLDINGS_RECORD).get(CREATE).get(COMPLETED) + ". " +
                " Failed: " + metrics.get(HOLDINGS_RECORD).get(CREATE).get(FAILED) +
                " Skipped: " + metrics.get(HOLDINGS_RECORD).get(CREATE).get(SKIPPED) + "\n" +
                "Holdings records updates: " + metrics.get(HOLDINGS_RECORD).get(UPDATE).get(COMPLETED) + ". " +
                " Failed: " + metrics.get(HOLDINGS_RECORD).get(UPDATE).get(FAILED) +
                " Skipped: " + metrics.get(HOLDINGS_RECORD).get(UPDATE).get(SKIPPED) + "\n" +
                "Holdings records deletes: " + metrics.get(HOLDINGS_RECORD).get(DELETE).get(COMPLETED) + ". " +
                " Failed: " + metrics.get(HOLDINGS_RECORD).get(DELETE).get(FAILED) +
                " Skipped: " + metrics.get(HOLDINGS_RECORD).get(DELETE).get(SKIPPED) + "\n" +
                "Item creates: " + metrics.get(ITEM).get(CREATE).get(COMPLETED) + ". " +
                " Failed: " + metrics.get(ITEM).get(CREATE).get(FAILED) +
                " Skipped: " + metrics.get(ITEM).get(CREATE).get(SKIPPED) + "\n" +
                "Item updates: " + metrics.get(ITEM).get(UPDATE).get(COMPLETED) + ". " +
                " Failed: " + metrics.get(ITEM).get(UPDATE).get(FAILED) +
                " Skipped: " + metrics.get(ITEM).get(UPDATE).get(SKIPPED) + "\n" +
                "Item deletes: " + metrics.get(ITEM).get(DELETE).get(COMPLETED) + ". " +
                " Failed: " + metrics.get(ITEM).get(DELETE).get(FAILED) +
                " Skipped: " + metrics.get(ITEM).get(DELETE).get(SKIPPED);
    }
}


