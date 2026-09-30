/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.oracle;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.integrations.destination.StandardNameTransformer;
import io.airbyte.cdk.integrations.destination.jdbc.SqlOperations;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleOperations implements SqlOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleOperations.class);
  private static final int INSERT_BATCH_SIZE = 5_000;

  private final String tablespace;

  public OracleOperations(final String tablespace) {
    this.tablespace = tablespace;
  }

  @Override
  public void createSchemaIfNotExists(final JdbcDatabase database, final String schemaName) throws Exception {
    if (database.queryInt("select count(*) from all_users where upper(username) = upper(?)", schemaName) == 0) {
      LOGGER.warn("Schema " + schemaName + " is not found! Trying to create a new one.");
      // Oracle 18c+ schema-only account: no password (passwords are limited to 30 bytes) and no login.
      // The connector user creates the tables in this schema, so the schema needs no privileges.
      database.execute(String.format("CREATE USER %s NO AUTHENTICATION QUOTA UNLIMITED ON %s", schemaName, tablespace));
    }
  }

  @Override
  public void createTableIfNotExists(final JdbcDatabase database, final String schemaName, final String tableName) throws Exception {
    try {
      if (!tableExists(database, schemaName, tableName)) {
        database.execute(createTableQuery(database, schemaName, tableName));
      }
    } catch (final Exception e) {
      LOGGER.error("Error while creating table.", e);
      throw e;
    }
  }

  @Override
  public String createTableQuery(final JdbcDatabase database, final String schemaName, final String tableName) {
    // No primary key: direct-path inserts merge the random UUID index after every batch.
    // CACHE writes out-of-line LOBs through the buffer cache instead of synchronous direct writes.
    // The storage clause omits SECUREFILE so Oracle picks the LOB type the tablespace supports.
    return String.format(
        "CREATE TABLE %s.%s ( \n"
            + "%s VARCHAR(64) NOT NULL,\n"
            + "%s NCLOB,\n"
            + "%s TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP\n"
            + ") LOB (%s) STORE AS (CACHE)",
        schemaName, tableName,
        OracleDestination.COLUMN_NAME_AB_ID, OracleDestination.COLUMN_NAME_DATA, OracleDestination.COLUMN_NAME_EMITTED_AT,
        OracleDestination.COLUMN_NAME_DATA);
  }

  private boolean tableExists(final JdbcDatabase database, final String schemaName, final String tableName) throws Exception {
    final Integer count = database.queryInt("select count(*) \n from all_tables\n where upper(owner) = upper(?) and upper(table_name) = upper(?)",
        schemaName, tableName);
    return count == 1;
  }

  @Override
  public void dropTableIfExists(final JdbcDatabase database, final String schemaName, final String tableName) throws Exception {
    if (tableExists(database, schemaName, tableName)) {
      try {
        final String query = String.format("DROP TABLE %s.%s", schemaName, tableName);
        database.execute(query);
      } catch (final Exception e) {
        LOGGER.error(String.format("Error dropping table %s.%s", schemaName, tableName), e);
        throw e;
      }
    }
  }

  @Override
  public String truncateTableQuery(final JdbcDatabase database, final String schemaName, final String tableName) {
    // TRUNCATE avoids row-by-row undo and redo. Use DELETE when the user cannot truncate another schema's table.
    return String.format("""
                         BEGIN
                           EXECUTE IMMEDIATE 'TRUNCATE TABLE %1$s.%2$s';
                         EXCEPTION
                           WHEN OTHERS THEN
                             IF SQLCODE = -1031 THEN
                               EXECUTE IMMEDIATE 'DELETE FROM %1$s.%2$s';
                             ELSE
                               RAISE;
                             END IF;
                         END""", schemaName, tableName);
  }

  @Override
  public void insertRecords(final JdbcDatabase database,
                            final List<AirbyteRecordMessage> records,
                            final String schemaName,
                            final String tempTableName)
      throws Exception {
    final String tableName = String.format("%s.%s", schemaName, tempTableName);
    final String columns = String.format("(%s, %s, %s)",
        OracleDestination.COLUMN_NAME_AB_ID, OracleDestination.COLUMN_NAME_DATA, OracleDestination.COLUMN_NAME_EMITTED_AT);
    insertRawRecordsInBatches(tableName, columns, database, records, UUID::randomUUID);
  }

  // A single-row INSERT with JDBC batches is parsed once and avoids the large INSERT ALL statement.
  // APPEND_VALUES uses direct-path inserts. Oracle requires a commit after each direct-path batch.
  // Hikari connections use autocommit by default; commit explicitly only when autocommit is off.
  private static void insertRawRecordsInBatches(final String tableName,
                                                final String columns,
                                                final JdbcDatabase jdbcDatabase,
                                                final List<AirbyteRecordMessage> records,
                                                final Supplier<UUID> uuidSupplier)
      throws SQLException {
    if (records.isEmpty()) {
      return;
    }

    final String query = String.format("INSERT /*+ APPEND_VALUES */ INTO %s %s VALUES (?, ?, ?)", tableName, columns);

    jdbcDatabase.execute(connection -> {
      final boolean commitEachBatch = !connection.getAutoCommit();
      try (final PreparedStatement statement = connection.prepareStatement(query)) {
        int batchCount = 0;
        for (final AirbyteRecordMessage message : records) {
          final JsonNode formattedData = StandardNameTransformer.formatJsonPath(message.getData());
          statement.setString(1, uuidSupplier.get().toString());
          statement.setString(2, Jsons.serialize(formattedData));
          statement.setTimestamp(3, Timestamp.from(Instant.ofEpochMilli(message.getEmittedAt())));
          statement.addBatch();

          if (++batchCount == INSERT_BATCH_SIZE) {
            executeBatch(connection, statement, commitEachBatch);
            batchCount = 0;
          }
        }

        if (batchCount > 0) {
          executeBatch(connection, statement, commitEachBatch);
        }
      }
    });
  }

  private static void executeBatch(final Connection connection, final PreparedStatement statement, final boolean commit)
      throws SQLException {
    statement.executeBatch();
    if (commit) {
      connection.commit();
    }
  }

  @Override
  public String insertTableQuery(final JdbcDatabase database,
                                 final String schemaName,
                                 final String sourceTableName,
                                 final String destinationTableName) {
    return String.format("INSERT INTO %s.%s SELECT * FROM %s.%s\n", schemaName, destinationTableName, schemaName, sourceTableName);
  }

  @Override
  public void executeTransaction(final JdbcDatabase database, final List<String> queries) throws Exception {
    // Append mode has no start queries. An empty PL/SQL block fails with PLS-00103.
    if (queries.isEmpty()) {
      return;
    }
    final String SQL = "BEGIN\n COMMIT;\n" + String.join(";\n", queries) + "; \nCOMMIT; \nEND;";
    database.execute(SQL);
  }

  @Override
  public boolean isValidData(final JsonNode data) {
    return true;
  }

  @Override
  public boolean isSchemaRequired() {
    return true;
  }

}
