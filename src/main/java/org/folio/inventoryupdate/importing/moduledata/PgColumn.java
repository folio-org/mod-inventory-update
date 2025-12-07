package org.folio.inventoryupdate.importing.moduledata;

import org.folio.tlib.postgres.cqlfield.PgCqlFieldBase;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldNumber;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldText;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldTimestamp;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldUuid;

public class PgColumn {

  public final String name;
  public final PgColumn.Type type;
  public final String nullable;
  public final String unique;
  public final Boolean isPrimaryKey;

  public enum Type {
    TEXT,
    INTEGER,
    BIGINT,
    TIMESTAMP,
    UUID,
    BOOLEAN,
    JSONB

  }

  /**
   * Constructor.
   */
  public PgColumn(String name, Type type, Boolean nullable, Boolean isPrimaryKey, Boolean unique) {
    this.name = name;
    this.type = type;
    this.isPrimaryKey = isPrimaryKey;
    this.nullable = Boolean.TRUE.equals(nullable) ? " NULL" : " NOT NULL";
    this.unique = Boolean.TRUE.equals(unique) ? " UNIQUE" : "";
  }

  public String getColumnDdl() {
    return name + " " + type + (Boolean.TRUE.equals(isPrimaryKey) ? " PRIMARY KEY" : nullable + unique);
  }

  /**
   * Selects appropriate PgCql field type for the PG column type.
   */
  public PgCqlFieldBase pgCqlField() {
    switch (type) {
      case INTEGER, BIGINT:
        return new PgCqlFieldNumber();
      case UUID:
        return new PgCqlFieldUuid();
      case TIMESTAMP:
        return new PgCqlFieldTimestamp();
      default:
        return new PgCqlFieldText().withExact().withLikeOps().withFullText();
    }
  }
}
