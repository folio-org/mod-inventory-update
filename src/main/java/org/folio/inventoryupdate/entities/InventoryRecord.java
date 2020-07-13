package org.folio.inventoryupdate.entities;

import java.util.UUID;

import io.vertx.core.json.JsonObject;

public abstract class InventoryRecord {

    public enum Transaction {
        UNKNOWN,
        CREATE,
        UPDATE,
        DELETE,
        GET,
        NONE
    }

    public enum Outcome {
        PENDING,
        COMPLETED,
        FAILED,
        SKIPPED
    }

    public enum Entity {
        INSTANCE,
        HOLDINGSRECORD,
        ITEM,
        LOCATION
    }

    protected JsonObject jsonRecord;

    protected Entity type;
    protected Transaction transaction = Transaction.UNKNOWN;
    protected Outcome outcome = Outcome.PENDING;

    public void setTransition (Transaction transaction) {
        this.transaction = transaction;
    }

    public Transaction getTransaction () {
        return transaction;
    }

    public boolean isDeleting () {
        return (transaction == Transaction.DELETE);
    }

    public boolean isUpdating () {
        return (transaction == Transaction.UPDATE);
    }

    public boolean isCreating () {
        return (transaction == Transaction.CREATE);
    }

    public boolean stateUnknown () {
        return (transaction == Transaction.UNKNOWN);
    }

    public String generateUUID () {
        UUID uuid = UUID.randomUUID();
        jsonRecord.put("id", uuid.toString());
        return uuid.toString();
    }

    public void setUUID (String uuid) {
        jsonRecord.put("id", uuid);
    }

    public String UUID () {
        return jsonRecord.getString("id");
    }

    public boolean hasUUID () {
        return (jsonRecord.getString("id") != null);
    }

    public String getHRID () {
        return jsonRecord.getString("hrid");
    }

    public JsonObject asJson() {
        return jsonRecord;
    }

    public String asJsonString() {
        if (jsonRecord != null) {
            return jsonRecord.toString();
        } else {
            return "{}";
        }
    }

    public Entity entityType () {
        return type;
    }

    public void setOutcome (Outcome outcome) {
        this.outcome = outcome;
    }

    public Outcome getOutcome () {
        return this.outcome;
    }

    public void complete() {
        this.outcome = Outcome.COMPLETED;
    }

    public boolean completed() {
        return this.outcome == Outcome.COMPLETED;
    }

    public void fail() {
        this.outcome = Outcome.FAILED;
    }

    public boolean failed() {
        return this.outcome == Outcome.FAILED;
    }

    public void skip() {
        this.outcome = Outcome.SKIPPED;
    }

    public boolean skipped() {
        return this.outcome == Outcome.SKIPPED;
    }

}