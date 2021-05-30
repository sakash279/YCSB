/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved. Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License. See accompanying LICENSE file. Submitted by Chrisjan Matser on 10/11/2010.
 */

package site.ycsb.db;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

/**
 * Cassandra 2.x CQL client using DataStax Java Driver 4 with Cosmos Cassandra extensions.
 * <p>
 * See {@code cassandra2/README.md} for details.
 *
 * @author David-Noble-at-work
 */
public class CassandraCQLClientExt extends DB {

  public static final String APPLICATION_CONFIGURATION_FILE_DEFAULT = "application.conf";
  public static final String APPLICATION_CONFIGURATION_FILE_PROPERTY = "config-file";

  public static final String EXECUTION_TRACING_DEFAULT = "false";
  public static final String EXECUTION_TRACING_PROPERTY = "execution-tracing";

  public static final String READ_CONSISTENCY_LEVEL_DEFAULT = ConsistencyLevel.QUORUM.name();
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "read-consistency-level";

  public static final String WRITE_CONSISTENCY_LEVEL_DEFAULT = ConsistencyLevel.QUORUM.name();
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "write-consistency-level";

  public static final String YCSB_KEY = "y_id";

  static final Logger LOG = LoggerFactory.getLogger(CassandraCQLClientExt.class);


  /**
   * Count the number of times initialized to teardown on the last {@link #cleanup()}.
   */

  private static final AtomicReference<PreparedStatement> DELETE_STATEMENT = new AtomicReference<>();
  private static final AtomicInteger INIT_COUNT = new AtomicInteger();
  private static final ConcurrentMap<Set<String>, PreparedStatement> INSERT_STATEMENTS = new ConcurrentHashMap<>();
  private static final AtomicReference<PreparedStatement> READ_ALL_STATEMENT = new AtomicReference<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> READ_STATEMENTS = new ConcurrentHashMap<>();
  private static final AtomicReference<PreparedStatement> SCAN_ALL_STATEMENT = new AtomicReference<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> SCAN_STATEMENTS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> UPDATE_STATEMENTS = new ConcurrentHashMap<>();

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.QUORUM;
  private static boolean tracing = false;
  private static CqlSession session = null;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.QUORUM;

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int currentInitCount = INIT_COUNT.decrementAndGet();
      if (currentInitCount <= 0) {
        READ_STATEMENTS.clear();
        SCAN_STATEMENTS.clear();
        INSERT_STATEMENTS.clear();
        UPDATE_STATEMENTS.clear();
        READ_ALL_STATEMENT.set(null);
        SCAN_ALL_STATEMENT.set(null);
        DELETE_STATEMENT.set(null);
        session.close();
        session = null;
      }
      if (currentInitCount < 0) {
        // This should never happen.
        throw new DBException("initCount is negative: " + currentInitCount);
      }
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   *
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(final String table, final String key) {

    try {

      final PreparedStatement delete = DELETE_STATEMENT.updateAndGet(prior -> prior == null
          ? session.prepare(QueryBuilder.deleteFrom(table).whereColumn(YCSB_KEY).isEqualTo(bindMarker()).build()
              .setConsistencyLevel(writeConsistencyLevel)
              .setTracing(tracing))
          : prior);

      LOG.debug("statement: {}}, key: {}", delete.getQuery(), key);
      session.execute(delete.bind(key));

      return Status.OK;

    } catch (final Exception error) {
      LOG.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), error);
    }

    return Status.ERROR;
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      if (session != null) {
        return;  // because we're initialized
      }

      try {

        final File applicationConfigurationFile = new File(this.getProperties().getProperty(
            APPLICATION_CONFIGURATION_FILE_PROPERTY,
            APPLICATION_CONFIGURATION_FILE_DEFAULT));

        tracing = Boolean.getBoolean(this.getProperties().getProperty(
            EXECUTION_TRACING_PROPERTY,
            EXECUTION_TRACING_DEFAULT));

        readConsistencyLevel = DefaultConsistencyLevel.valueOf(this.getProperties().getProperty(
            READ_CONSISTENCY_LEVEL_PROPERTY,
            READ_CONSISTENCY_LEVEL_DEFAULT));

        writeConsistencyLevel = DefaultConsistencyLevel.valueOf(this.getProperties().getProperty(
            WRITE_CONSISTENCY_LEVEL_PROPERTY,
            WRITE_CONSISTENCY_LEVEL_DEFAULT));

        session = CqlSession.builder()
            .withClassLoader(ClassLoader.getSystemClassLoader())
            .withConfigLoader(DriverConfigLoader.fromFile(applicationConfigurationFile))
            .build();

        final Metadata metadata = session.getMetadata();
        LOG.info("Connected to session: {}", metadata.getClusterName());

        for (final Node node : metadata.getNodes().values()) {
          LOG.info("Datacenter: {}; Host: {}; Rack: {}", node.getDatacenter(), node.getEndPoint(), node.getRack());
        }

      } catch (final Throwable error) {
        throw new DBException(error);
      }
    } // synchronized
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param namedValues A HashMap of field/value pairs to insert in the record
   *
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> namedValues) {

    try {

      final Set<String> fields = namedValues.keySet();

      final PreparedStatement statement = INSERT_STATEMENTS.compute(fields, (ignored, prior) -> {
          if (prior != null) {
            return prior;
          }
          final Insert insert = QueryBuilder.insertInto(table)
              .value(YCSB_KEY, bindMarker(YCSB_KEY))
              .values(fields.stream().collect(Collectors.toMap(Function.identity(), name -> bindMarker())));
          return session.prepare(insert.build().setConsistencyLevel(writeConsistencyLevel).setTracing(tracing));
        });

      if (LOG.isDebugEnabled()) {
        LOG.debug("statement: {}, key: {}, values: {}", statement.getQuery(), key, namedValues);
      }

      // TODO (DANOBLE) verify that the order of namedValues::keySet and namedValues::values are the same

      session.execute(statement.boundStatementBuilder(namedValues.values()).setString(YCSB_KEY, key).build());
      return Status.OK;

    } catch (final Exception error) {
      LOG.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), error);
    }

    return Status.ERROR;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   *
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(
      final String table,
      final String key,
      final Set<String> fields,
      final Map<String, ByteIterator> result) {

    try {

      final PreparedStatement statement = fields == null
          ? READ_ALL_STATEMENT.updateAndGet(prior -> {
              if (prior != null) {
                return prior;
              }
              final SimpleStatement select = selectFrom(table).all()
                  .where(Relation.column(YCSB_KEY).isEqualTo(bindMarker()))
                  .limit(1)
                  .build();
              return session.prepare(select.setConsistencyLevel(readConsistencyLevel).setTracing(tracing));
            })
          : READ_STATEMENTS.compute(new HashSet<>(fields), (names, prior) -> {
              if (prior != null) {
                return prior;
              }
              final SimpleStatement select = selectFrom(table).columns(names)
                  .where(Relation.column(YCSB_KEY).isEqualTo(bindMarker()))
                  .limit(1)
                  .build();
              return session.prepare(select.setConsistencyLevel(readConsistencyLevel).setTracing(tracing));
            });

      if (LOG.isDebugEnabled()) {
        LOG.debug("statement: {}, key: {}, fields: {}", statement.getQuery(), key, fields);
      }

      final ResultSet resultSet = session.execute(statement.bind(key));
      final Row row = resultSet.one();

      if (row == null) {
        return Status.NOT_FOUND;
      }

      final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();

      for (final ColumnDefinition definition : columnDefinitions) {
        final ByteBuffer value = row.getBytesUnsafe(definition.getName());
        final String name = definition.getName().toString();
        result.put(name, value == null ? null : new ByteArrayByteIterator(value.array()));
      }

      return Status.OK;

    } catch (final Exception e) {
      LOG.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database.
   * <p>
   * Each field/value pair from the result will be stored in a HashMap. Cassandra CQL uses "token" method for range
   * scan which doesn't always yield intuitive results.
   *
   * @param table       The name of the table
   * @param startKey    The record key of the first record to read.
   * @param recordCount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   *
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(
      final String table,
      final String startKey,
      final int recordCount,
      final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {

    try {

      // TODO (DANOBLE) The statement builder in driver-3 (?) is not set up for tokens. Here's the query fragment we
      //  expect: "where token(YCSB_KEY) >= token(<bind-marker>) limit <bind-marker>". If we don't get that from
      //  select::asCql, then code it manually

      final PreparedStatement statement = fields == null
          ? SCAN_ALL_STATEMENT.updateAndGet(prior -> {
              if (prior != null) {
                return prior;
              }
              final SimpleStatement select = selectFrom(table).all()
                  .whereToken(YCSB_KEY).isGreaterThanOrEqualTo(bindMarker())
                  .limit(bindMarker())
                  .build();
              return session.prepare(select.setConsistencyLevel(readConsistencyLevel).setTracing(tracing));
            })
          : SCAN_STATEMENTS.compute(new HashSet<>(fields), (names, prior) -> {
              if (prior != null) {
                return prior;
              }
              final SimpleStatement select = selectFrom(table).columns(names)
                  .whereToken(YCSB_KEY).isGreaterThanOrEqualTo(bindMarker())
                  .limit(bindMarker())
                  .build();
              return session.prepare(select.setConsistencyLevel(readConsistencyLevel).setTracing(tracing));
            });

      if (LOG.isDebugEnabled()) {
        LOG.debug("statement: {}, startKey: {}, recordCount: {}, fields: {}",
            statement.getQuery(),
            startKey,
            recordCount,
            fields);
      }

      final ResultSet resultSet = session.execute(statement.bind(startKey, recordCount));

      for (final Row row : resultSet) {

        final HashMap<String, ByteIterator> tuple = new HashMap<>(row.size());

        for (final ColumnDefinition columnDefinition : row.getColumnDefinitions()) {
          final String name = columnDefinition.getName().toString();
          final ByteBuffer value = row.getBytesUnsafe(name);
          tuple.put(name, value == null ? null : new ByteArrayByteIterator(value.array()));
        }

        result.add(tuple);
      }

      return Status.OK;

    } catch (final Exception error) {
      LOG.error("Failed scanning with startKey '{}' due to: ", startKey, error);
      return Status.ERROR;
    }

  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param namedValues A HashMap of field/value pairs to update in the record
   *
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> namedValues) {

    try {
      final Set<String> fields = new HashSet<>(namedValues.keySet());

      final PreparedStatement statement = UPDATE_STATEMENTS.compute(fields, (names, prior) -> {
          if (prior != null) {
            return prior;
          }
          final SimpleStatement update = QueryBuilder.update(table)
              .set(names.stream()
                  .map(name -> Assignment.setColumn(name, bindMarker()))
                  .toArray(Assignment[]::new))
              .whereColumn(YCSB_KEY).isEqualTo(bindMarker(YCSB_KEY))
              .build();
          return session.prepare(update.setConsistencyLevel(writeConsistencyLevel).setTracing(tracing));
        });

      if (LOG.isDebugEnabled()) {
        LOG.debug("statement: {}, key: {}, values: {}", statement.getQuery(), key, namedValues);
      }

      // TODO (DANOBLE) verify that the order of namedValues::keySet and namedValues::values are the same

      session.execute(statement.boundStatementBuilder(namedValues.values()).setString(YCSB_KEY, key).build());
      return Status.OK;

    } catch (final Exception e) {
      LOG.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }
}
