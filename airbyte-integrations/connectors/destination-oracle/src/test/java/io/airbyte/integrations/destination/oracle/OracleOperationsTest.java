/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OracleOperationsTest {

  private static final String EXPECTED_SQL =
      "INSERT /*+ APPEND_VALUES */ INTO TEST_SCHEMA.TEST_TABLE (\"_AIRBYTE_AB_ID\", \"_AIRBYTE_DATA\", \"_AIRBYTE_EMITTED_AT\") VALUES (?, ?, ?)";
  private static final long EMITTED_AT = 1_700_000_000_000L;

  static Stream<Arguments> batchCases() {
    return Stream.of(
        Arguments.of("no records do not open a connection", 0, true, 0, 0),
        Arguments.of("one record uses one batch", 1, true, 1, 0),
        Arguments.of("exactly one full batch", 5_000, true, 1, 0),
        Arguments.of("one record over a full batch uses two batches", 5_001, true, 2, 0),
        Arguments.of("autocommit off commits each batch", 5_001, false, 2, 2));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("batchCases")
  void insertRecordsUsesJdbcBatches(final String name,
                                    final int recordCount,
                                    final boolean autoCommit,
                                    final int expectedBatches,
                                    final int expectedCommits)
      throws Exception {
    final JdbcDatabase database = mock(JdbcDatabase.class);
    final Connection connection = mock(Connection.class);
    final PreparedStatement statement = mock(PreparedStatement.class);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(connection.getAutoCommit()).thenReturn(autoCommit);
    doAnswer(invocation -> {
      final CheckedConsumer<Connection, ?> query = invocation.getArgument(0);
      query.accept(connection);
      return null;
    }).when(database).execute(any(CheckedConsumer.class));

    final List<AirbyteRecordMessage> records = IntStream.range(0, recordCount)
        .mapToObj(i -> new AirbyteRecordMessage()
            .withData(Jsons.jsonNode(Map.of("id", i)))
            .withEmittedAt(EMITTED_AT))
        .toList();

    new OracleOperations("users").insertRecords(database, records, "TEST_SCHEMA", "TEST_TABLE");

    if (recordCount == 0) {
      verify(database, never()).execute(any(CheckedConsumer.class));
      return;
    }

    verify(connection, times(1)).prepareStatement(EXPECTED_SQL);
    verify(statement, times(recordCount)).addBatch();
    verify(statement, times(expectedBatches)).executeBatch();
    verify(statement, never()).execute();
    verify(statement, times(recordCount)).setString(eq(1), anyString());
    verify(statement, times(1)).setString(2, "{\"id\":0}");
    verify(statement, times(recordCount)).setTimestamp(eq(3), eq(Timestamp.from(Instant.ofEpochMilli(EMITTED_AT))));
    verify(statement, never()).setString(eq(4), anyString());
    verify(connection, times(expectedCommits)).commit();
  }

  @Test
  void truncateTableQueryUsesTruncateWithDeleteFallback() {
    final String expected = """
                            BEGIN
                              EXECUTE IMMEDIATE 'TRUNCATE TABLE TEST_SCHEMA.TEST_TABLE';
                            EXCEPTION
                              WHEN OTHERS THEN
                                IF SQLCODE = -1031 THEN
                                  EXECUTE IMMEDIATE 'DELETE FROM TEST_SCHEMA.TEST_TABLE';
                                ELSE
                                  RAISE;
                                END IF;
                            END""";

    assertEquals(expected, new OracleOperations("users").truncateTableQuery(null, "TEST_SCHEMA", "TEST_TABLE"));
  }

  static Stream<Arguments> transactionCases() {
    return Stream.of(
        Arguments.of("append mode has no start queries and runs nothing", List.of(), null),
        Arguments.of("one query runs inside one PL/SQL block", List.of("Q1"), "BEGIN\n COMMIT;\nQ1; \nCOMMIT; \nEND;"),
        Arguments.of("two queries run inside one PL/SQL block", List.of("Q1", "Q2"), "BEGIN\n COMMIT;\nQ1;\nQ2; \nCOMMIT; \nEND;"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("transactionCases")
  void executeTransactionRunsOnlyNonEmptyBlocks(final String name, final List<String> queries, final String expectedSql) throws Exception {
    final JdbcDatabase database = mock(JdbcDatabase.class);

    new OracleOperations("users").executeTransaction(database, queries);

    if (expectedSql == null) {
      verify(database, never()).execute(anyString());
    } else {
      verify(database, times(1)).execute(expectedSql);
    }
  }

  static Stream<Arguments> schemaCases() {
    return Stream.of(
        Arguments.of("existing schema is not created", 1, null),
        Arguments.of("long schema name creates a schema-only user without a password", 0,
            "CREATE USER source_namespace_test_20260930_vrhbg NO AUTHENTICATION QUOTA UNLIMITED ON users"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("schemaCases")
  void createSchemaIfNotExistsCreatesSchemaOnlyUser(final String name, final int existingUsers, final String expectedSql) throws Exception {
    final JdbcDatabase database = mock(JdbcDatabase.class);
    when(database.queryInt(anyString(), anyString())).thenReturn(existingUsers);

    new OracleOperations("users").createSchemaIfNotExists(database, "source_namespace_test_20260930_vrhbg");

    if (expectedSql == null) {
      verify(database, never()).execute(anyString());
    } else {
      verify(database, times(1)).execute(anyString());
      verify(database, times(1)).execute(expectedSql);
    }
  }

  @Test
  void createTableQueryUsesCachedNclobWithoutPrimaryKey() {
    final String expected = """
                            CREATE TABLE TEST_SCHEMA.TEST_TABLE (\s
                            "_AIRBYTE_AB_ID" VARCHAR(64) NOT NULL,
                            "_AIRBYTE_DATA" NCLOB,
                            "_AIRBYTE_EMITTED_AT" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                            ) LOB ("_AIRBYTE_DATA") STORE AS (CACHE)""";

    assertEquals(expected, new OracleOperations("users").createTableQuery(null, "TEST_SCHEMA", "TEST_TABLE"));
  }

}
