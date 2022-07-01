package org.folio.inventoryupdate;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import org.folio.inventoryupdate.entities.InventoryRecord;
import static org.folio.inventoryupdate.entities.InventoryRecord.*;

import java.util.Arrays;

public class UpdateMetrics {
    protected static final Logger logger = LoggerFactory.getLogger("inventory-update");

    InstanceMetrics instanceMetrics = new InstanceMetrics();
    HoldingsRecordMetrics holdingsRecordMetrics = new HoldingsRecordMetrics();
    ItemMetrics itemMetrics = new ItemMetrics();
    InstanceRelationsMetrics instanceRelationshipMetrics = new InstanceRelationsMetrics();
    InstanceRelationsMetrics titleSuccessionMetrics = new InstanceRelationsMetrics();

    public static UpdateMetrics makeMetricsFromJson (JsonObject updateMetricsJson) {
        UpdateMetrics metrics = new UpdateMetrics();
        for (InventoryRecord.Entity entity :
                Arrays.asList(
                        InventoryRecord.Entity.INSTANCE,
                        InventoryRecord.Entity.HOLDINGS_RECORD,
                        InventoryRecord.Entity.ITEM,
                        InventoryRecord.Entity.INSTANCE_RELATIONSHIP,
                        InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION))
        {
            for (InventoryRecord.Transaction transaction : Arrays.asList(
                    InventoryRecord.Transaction.CREATE,
                    InventoryRecord.Transaction.UPDATE,
                    InventoryRecord.Transaction.DELETE))
            {
                for (InventoryRecord.Outcome outcome : Arrays.asList(
                        Outcome.COMPLETED,
                        Outcome.FAILED,
                        Outcome.SKIPPED,
                        Outcome.PENDING
                ))
                {
                    if (getMetricFromJson(updateMetricsJson, entity, transaction, outcome) != null) {
                        metrics.entity(entity).transaction(transaction).outcomes
                                .increment(outcome,
                                getMetricFromJson(updateMetricsJson, entity, transaction, outcome));
                    }
                }

            }
        }
        return metrics;
    }

    private static Integer getMetricFromJson(JsonObject metrics,
                                         InventoryRecord.Entity entity,
                                         InventoryRecord.Transaction transaction,
                                         InventoryRecord.Outcome outcome) {
        try {
            return metrics
                    .getJsonObject(entity.name())
                    .getJsonObject(transaction.name())
                    .getInteger(outcome.name());
        } catch (NullPointerException npe) {
            return null;
        }
    }
    public EntityMetrics entity(InventoryRecord.Entity entityType) {
        switch (entityType) {
            case INSTANCE:
                return instanceMetrics;
            case HOLDINGS_RECORD:
                return holdingsRecordMetrics;
            case ITEM:
                return itemMetrics;
            case INSTANCE_RELATIONSHIP:
                return instanceRelationshipMetrics;
            case INSTANCE_TITLE_SUCCESSION:
                return titleSuccessionMetrics;
            default:
                return null;
        }
    }

    public JsonObject asJson() {
        JsonObject metrics = new JsonObject();
        metrics.put(InventoryRecord.Entity.INSTANCE.name(), instanceMetrics.asJson());
        metrics.put(InventoryRecord.Entity.HOLDINGS_RECORD.name(), holdingsRecordMetrics.asJson());
        metrics.put(InventoryRecord.Entity.ITEM.name(), itemMetrics.asJson());
        if (instanceRelationshipMetrics.touched()) {
            metrics.put(InventoryRecord.Entity.INSTANCE_RELATIONSHIP.name(), instanceRelationshipMetrics.asJson());
        }
        if (titleSuccessionMetrics.touched()) {
            metrics.put(InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION.name(), titleSuccessionMetrics.asJson());
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

    public static class ProvisionalInstanceMetrics extends OutcomesMetrics {
    }

    public static class OutcomesMetrics {
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

        public int count(InventoryRecord.Outcome outcome) {
            switch (outcome) {
                case COMPLETED:
                    return completed;
                case FAILED:
                    return failed;
                case SKIPPED:
                    return skipped;
                case PENDING:
                    return pending;
                default:
                    return 0;
            }
        }

        public void increment(InventoryRecord.Outcome outcome, Integer i) {
            if (i != null) {
                touched = true;
                switch ( outcome ) {
                    case COMPLETED:
                        completed += i;
                        break;
                    case FAILED:
                        failed += i;
                        break;
                    case SKIPPED:
                        skipped += i;
                        break;
                    case PENDING:
                        pending += i;
                        break;
                    default:
                        // no op
                }
            }
        }

        public JsonObject asJson() {
            JsonObject metrics = new JsonObject();
            metrics.put(Outcome.COMPLETED.name(), completed);
            metrics.put(Outcome.FAILED.name(), failed);
            metrics.put(Outcome.SKIPPED.name(), skipped);
            metrics.put(Outcome.PENDING.name(), pending);
            return metrics;
        }
    }

    public static class TransactionMetrics {
        public final OutcomesMetrics outcomes = new OutcomesMetrics();

        public boolean touched () {
            return outcomes.touched;
        }

        public JsonObject asJson () {
            return outcomes.asJson();
        }
    }

    public UpdateMetrics add (UpdateMetrics metrics) {
        for (InventoryRecord.Entity entity :
                Arrays.asList(
                        InventoryRecord.Entity.INSTANCE,
                        InventoryRecord.Entity.HOLDINGS_RECORD,
                        InventoryRecord.Entity.ITEM,
                        InventoryRecord.Entity.INSTANCE_RELATIONSHIP,
                        InventoryRecord.Entity.INSTANCE_TITLE_SUCCESSION))
        {
            for (InventoryRecord.Transaction transaction : Arrays.asList(
                    InventoryRecord.Transaction.CREATE,
                    InventoryRecord.Transaction.UPDATE,
                    InventoryRecord.Transaction.DELETE))
            {
                for (InventoryRecord.Outcome outcome : Arrays.asList(
                        Outcome.COMPLETED,
                        Outcome.FAILED,
                        Outcome.SKIPPED,
                        Outcome.PENDING
                ))
                {
                    this.entity(entity).transaction(transaction).outcomes.increment(
                          outcome,
                          metrics.entity(entity).transaction(transaction).outcomes.count(outcome));
                }

            }
        }
        return this;
    }

}
