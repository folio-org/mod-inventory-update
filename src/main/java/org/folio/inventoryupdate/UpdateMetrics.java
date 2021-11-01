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
            default:
                return null;

        }
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
        protected final TransactionMetrics create = new TransactionMetrics();
        protected final TransactionMetrics update = new TransactionMetrics();
        protected final TransactionMetrics delete = new TransactionMetrics();
        protected final TransactionMetrics unknown = new TransactionMetrics();

        public TransactionMetrics transaction(InventoryRecord.Transaction transaction) {
            switch (transaction) {
                case CREATE:
                    return create;
                case UPDATE:
                    return update;
                case DELETE:
                    return delete;
                case UNKNOWN:
                    return unknown;
                default:
                    return null;
            }
        }

        public boolean touched () {
            return create.touched() || update.touched() || delete.touched();
        }

        public JsonObject asJson () {
            JsonObject metrics = new JsonObject();
            metrics.put(InventoryRecord.Transaction.CREATE.name(), create.asJson());
            metrics.put(InventoryRecord.Transaction.UPDATE.name(), update.asJson());
            metrics.put(InventoryRecord.Transaction.DELETE.name(), delete.asJson());
            return metrics;
        }
    }

    private class InstanceMetrics extends EntityMetrics {
    }

    private class HoldingsRecordMetrics extends EntityMetrics {
    }

    private class ItemMetrics extends EntityMetrics {
    }

    public class InstanceRelationsMetrics extends EntityMetrics {
        public final ProvisionalInstanceMetrics provisionalInstanceMetrics = new ProvisionalInstanceMetrics();

        @Override
        public JsonObject asJson () {
            JsonObject metrics = new JsonObject();
            metrics.put(InventoryRecord.Transaction.CREATE.name(), create.asJson());
            metrics.put(InventoryRecord.Transaction.DELETE.name(), delete.asJson());
            if (provisionalInstanceMetrics.touched) {
                metrics.put("PROVISIONAL_INSTANCE", provisionalInstanceMetrics.asJson());
            }
            return metrics;
        }
    }

    public class ProvisionalInstanceMetrics extends OutcomesMetrics {
    }

    public class OutcomesMetrics {
        private int completed = 0;
        private int failed = 0;
        private int skipped = 0;
        private int pending = 0;
        protected boolean touched = false;

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
                default:
                    // no op
            }
        }

        public JsonObject asJson() {
            JsonObject metrics = new JsonObject();
            metrics.put(InventoryRecord.Outcome.COMPLETED.name(), completed);
            metrics.put(InventoryRecord.Outcome.FAILED.name(), failed);
            metrics.put(InventoryRecord.Outcome.SKIPPED.name(), skipped);
            metrics.put(InventoryRecord.Outcome.PENDING.name(), pending);
            return metrics;
        }
    }

    public class TransactionMetrics {
        public final OutcomesMetrics outcomes = new OutcomesMetrics();

        public boolean touched () {
            return outcomes.touched;
        }

        public JsonObject asJson () {
            return outcomes.asJson();
        }
    }

}
