package org.folio.inventoryupdate;

import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.InventoryRecord;

public class UpdateMetrics {
    InstanceMetrics instance = new InstanceMetrics();
    HoldingsRecordMetrics holdingsRecord = new HoldingsRecordMetrics();
    ItemMetrics item = new ItemMetrics();
    InstanceRelationsMetrics instanceRelationship = new InstanceRelationsMetrics();
    InstanceRelationsMetrics titleSuccession = new InstanceRelationsMetrics();

    public EntityMetrics entity(InventoryRecord.Entity entityType) {
        switch (entityType) {
            case INSTANCE:
                return instance;
            case HOLDINGS_RECORD:
                return holdingsRecord;
            case ITEM:
                return item;
            case INSTANCE_RELATIONSHIP:
                return instanceRelationship;
            case INSTANCE_TITLE_SUCCESSION:
                return titleSuccession;

        }
        return null;
    }

    public JsonObject asJson() {
        JsonObject metrics = new JsonObject();
        metrics.put(InventoryRecord.Entity.INSTANCE.toString(), instance.asJson());
        metrics.put(InventoryRecord.Entity.HOLDINGS_RECORD.toString(), holdingsRecord.asJson());
        metrics.put(InventoryRecord.Entity.ITEM.toString(), item.asJson());
        if (instanceRelationship.touched()) {
            metrics.put(InventoryRecord.Entity.INSTANCE_RELATIONSHIP.toString(), instanceRelationship.asJson());
        }
        if (titleSuccession.touched()) {
            metrics.put(InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION.toString(), titleSuccession.asJson());
        }
        return metrics;
    }

    public abstract class EntityMetrics {
        public TransactionMetrics created = new TransactionMetrics();
        public TransactionMetrics updated = new TransactionMetrics();
        public TransactionMetrics deleted = new TransactionMetrics();

        public TransactionMetrics transaction(InventoryRecord.Transaction transaction) {
            switch (transaction) {
                case CREATE:
                    return created;
                case UPDATE:
                    return updated;
                case DELETE:
                    return deleted;
            }
            return null;
        }

        public boolean touched () {
            return created.touched() || updated.touched() || deleted.touched();
        }

        public JsonObject asJson () {
            JsonObject metrics = new JsonObject();
            metrics.put("CREATED", created.asJson());
            metrics.put("UPDATED", updated.asJson());
            metrics.put("DELETED", deleted.asJson());
            return metrics;
        }
    }

    public class InstanceMetrics extends EntityMetrics {
    }

    public class HoldingsRecordMetrics extends EntityMetrics {
    }

    public class ItemMetrics extends EntityMetrics {
    }

    public class InstanceRelationsMetrics extends EntityMetrics {
        public ProvisionalInstanceMetrics provisionalInstanceMetrics = new ProvisionalInstanceMetrics();

        public JsonObject asJson () {
            JsonObject metrics = new JsonObject();
            metrics.put("CREATED", created.asJson());
            metrics.put("DELETED", deleted.asJson());
            if (provisionalInstanceMetrics.touched) {
                metrics.put("PROVISIONAL_INSTANCE", provisionalInstanceMetrics.asJson());
            }
            return metrics;
        }
    }

    public class ProvisionalInstanceMetrics extends OutcomesMetrics {
    }

    public class OutcomesMetrics {
        public int completed = 0;
        public int failed = 0;
        public int skipped = 0;
        public int pending = 0;
        public boolean touched = false;

        public void increment(InventoryRecord.Outcome outcome) {
            touched = true;
            switch (outcome) {
                case COMPLETED:
                    completed++;
                    break;
                case FAILED:
                    failed++;
                    break;
                case SKIPPED:
                    skipped++;
                    break;
                case PENDING:
                    pending++;
                    break;
            }
        }

        public JsonObject asJson() {
            JsonObject metrics = new JsonObject();
            metrics.put("COMPLETED", completed);
            metrics.put("FAILED", failed);
            metrics.put("SKIPPED", skipped);
            metrics.put("PENDING", pending);
            return metrics;
        }
    }

    public class TransactionMetrics {
        public OutcomesMetrics outcomes = new OutcomesMetrics();

        public boolean touched () {
            return outcomes.touched;
        }

        public JsonObject asJson () {
            return outcomes.asJson();
        }
    }

}
