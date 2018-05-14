package io.confluent.connect.jdbc.sink.dialect;

import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.connect.data.*;

import java.util.*;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.copiesToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;


public class DebeziumMySqlDialect extends DbDialect {

  public DebeziumMySqlDialect() {
    super("`", "`");
  }

  public static final HashMap<String, String> SCHEMA_NAME_CASTING_MAP;

  static {
    SCHEMA_NAME_CASTING_MAP = new HashMap<>();
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.data.Bits", "?");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.data.Json", "?");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.data.Enum", "?");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.data.EnumSet", "?");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.Date", "DATE_ADD('1970-01-01', INTERVAL ? DAY)");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.Timestamp", "DATE_ADD('1970-01-01 00:00:00',INTERVAL (? / 1000) SECOND)");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.ZonedTimestamp", "TIMESTAMP(SUBSTRING(?, 1, 19))");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.MicroTimestamp", "DATE_ADD('1970-01-01 00:00:00',INTERVAL FLOOR(? / 1000) SECOND)");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.MicroTime", "SEC_TO_TIME(FLOOR(? / 1000))");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.Time", "SEC_TO_TIME(?)");
    SCHEMA_NAME_CASTING_MAP.put("io.debezium.time.Year", "?");
  }

  public static final HashMap<String, String> SCHEMA_NAME_DATATYPE_MAP;

  static {
    // mappings were taken from the documentation at http://debezium.io/docs/connectors/mysql/
    SCHEMA_NAME_DATATYPE_MAP = new HashMap<>();
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.data.Bits", "VARBINARY(1024)");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.data.Json", "TEXT");
    // constraint for enums is enforced by the source data
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.data.Enum", "TEXT");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.data.EnumSet", "TEXT");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.Date", "DATE");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.Timestamp", "DATETIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.ZonedTimestamp", "DATETIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.MicroTimestamp", "DATETIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.MicroTime", "TIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.Time", "TIME");
    SCHEMA_NAME_DATATYPE_MAP.put("io.debezium.time.Year", "YEAR");
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    String schemaName = field.schemaName();
    Map<String, String> parameters = field.schemaParameters();
    Schema.Type type = field.schemaType();

    String sqlType;

    if (schemaName != null && schemaName.startsWith("io.debezium")) {
      sqlType = SCHEMA_NAME_DATATYPE_MAP.get(schemaName);
    } else {
      sqlType = new MySqlDialect().getSqlType(schemaName, parameters, type);
    }

    // handle the "default" case nicely: just return text
    if (sqlType == null) sqlType = "TEXT";
    // special case: MySQL can't deal with TEXT in primary key fields
    if (sqlType.equals("TEXT") && field.isPrimaryKey()) sqlType = "VARCHAR(255)";
    return sqlType;
  }

  private String makeUpsertPlaceholders(
      final Collection<String> keyCols,
      final Collection<String> cols,
      final FieldsMetadata fieldsMetadata
  ) {
    final StringBuilder builder = new StringBuilder();
    this.addUpsertPlaceholders(keyCols, fieldsMetadata, builder);
    this.addUpsertPlaceholders(cols, fieldsMetadata, builder);
    builder.setLength(builder.length() - 2); // remove trailing ", "
    return builder.toString();
  }

  public void addUpsertPlaceholders(
      final Collection<String> cols,
      final FieldsMetadata fieldsMetadata,
      StringBuilder builder
  ) {
    for (String col : cols) {
      String schemaName = fieldsMetadata.allFields.get(col).schemaName();
      if (schemaName == null) {
        builder.append("?, ");
        continue;
      }
      String placeholder = SCHEMA_NAME_CASTING_MAP.get(schemaName);
      // Depending on the settings, debezium will use some native kafka datatypes
      // See http://debezium.io/docs/connectors/mysql/#decimal-values
      // and http://debezium.io/docs/connectors/mysql/#temporal-values
      if (placeholder == null) placeholder = "?";

      builder.append(placeholder);
      builder.append(", ");
    }
  }


  @Override
  public String getUpsertQuery(
      final String table,
      final Collection<String> keyCols,
      final Collection<String> cols,
      final FieldsMetadata fieldsMetadata

  ) {
    //MySql doesn't support SQL 2003:merge so here how the upsert is handled

    final StringBuilder builder = new StringBuilder();
    builder.append("INSERT INTO ");
    builder.append(escaped(table));
    builder.append("(");
    joinToBuilder(builder, ",", keyCols, cols, escaper());
    builder.append(") VALUES (");

    builder.append(this.makeUpsertPlaceholders(keyCols, cols, fieldsMetadata));

    builder.append(") ON DUPLICATE KEY UPDATE ");
     // TODO FIXME: Do we also need to apply conversions to the ON DUPLICATE KEY statements or will they be taken from
    // the initial columns we provide?
    joinToBuilder(
        builder,
        ",",
        cols.isEmpty() ? keyCols : cols,
        new StringBuilderUtil.Transform<String>() {
          @Override
          public void apply(StringBuilder builder, String col) {
            builder.append(escaped(col)).append("=VALUES(").append(escaped(col)).append(")");
          }
        }
    );
    return builder.toString();
  }

  /**
   * Since Debezium doesn't send default values correctly yet (see https://github.com/debezium/debezium/pull/500),
   * all new columns must be nullable and default to null or else it will crash.
   *
   * This also requires the DbStructure.amendIfNecessary() to NOT crash
   * if a new column is neither optional nor has a default value.
   */
  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final boolean newlines = fields.size() > 1;

    final StringBuilder builder = new StringBuilder("ALTER TABLE ");
    builder.append(escaped(tableName));
    builder.append(" ");
    joinToBuilder(builder, ",", fields, new StringBuilderUtil.Transform<SinkRecordField>() {
      @Override
      public void apply(StringBuilder builder, SinkRecordField f) {
        if (newlines) {
          builder.append(System.lineSeparator());
        }
        builder.append("ADD ");
        writeAlterTableColumnSpec(builder, f);
      }
    });
    return Collections.singletonList(builder.toString());
  }

  private void writeAlterTableColumnSpec(StringBuilder builder, SinkRecordField f) {
    builder.append(escaped(f.name()));
    builder.append(" ");
    builder.append(getSqlType(f));
    builder.append(" NULL DEFAULT NULL");

  }

}
