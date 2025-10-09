package org.folio.inventoryimport.moduledata;

import org.folio.tlib.postgres.cqlfield.PgCqlFieldBase;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldNumber;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldText;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldTimestamp;
import org.folio.tlib.postgres.cqlfield.PgCqlFieldUuid;

public class PgColumn {

  public final String name;
  public final PgColumn.Type type;
  public final String nullable;
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
  public PgColumn(String name, Type type, Boolean nullable, Boolean isPrimaryKey) {
    this.name = name;
    this.type = type;
    this.isPrimaryKey = isPrimaryKey;
    this.nullable = nullable ? "NULL" : "NOT NULL";
  }

  public String getColumnDdl() {
    return name + " " + type + " " + (isPrimaryKey ? " PRIMARY KEY " : nullable);
  }

  /**
   * Selects appropriate PgCql field type for the PG column type.
   */
  public PgCqlFieldBase pgCqlField() {
    switch (type) {
      case INTEGER:
      case BIGINT:
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
