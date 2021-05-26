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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
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

/**
 * Cassandra 2.x CQL client using DataStax Java Driver 4 with Cosmos Cassandra extensions.
 * <p>
 * See {@code cassandra2/README.md} for details.
 *
 * @author David-Noble-at-work
 */
public class CassandraCQLClientExt extends DB {

  public static final String APPLICATION_CONFIGURATION_FILE_DEFAULT = "application.conf";
  public static final String APPLICATION_CONFIGURATION_FILE_PROPERTY = "cassandra.driver-4.configuration-file";

  public static final String YCSB_KEY = "y_id";

  static final Logger LOG = LoggerFactory.getLogger(CassandraCQLClientExt.class);
  private static final AtomicReference<PreparedStatement> DELETE_STMT = new AtomicReference<>();
  /**
   * Count the number of times initialized to teardown on the last {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger();
  private static final ConcurrentMap<Set<String>, PreparedStatement> INSERT_STMTS = new ConcurrentHashMap<>();
  private static final AtomicReference<PreparedStatement> READ_ALL_STMT = new AtomicReference<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> READ_STMTS = new ConcurrentHashMap<>();
  private static final AtomicReference<PreparedStatement> SCAN_ALL_STMT = new AtomicReference<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> SCAN_STMTS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> UPDATE_STMTS = new ConcurrentHashMap<>();

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.QUORUM;
  private static Session session = null;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.QUORUM;

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int currentInitCount = INIT_COUNT.decrementAndGet();
      if (currentInitCount <= 0) {
        READ_STMTS.clear();
        SCAN_STMTS.clear();
        INSERT_STMTS.clear();
        UPDATE_STMTS.clear();
        READ_ALL_STMT.set(null);
        SCAN_ALL_STMT.set(null);
        DELETE_STMT.set(null);
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
      PreparedStatement stmt = DELETE_STMT.get();

      // Prepare statement on demand
      if (stmt == null) {
        stmt = session.prepare(QueryBuilder.delete().from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())));
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        final PreparedStatement prevStmt = DELETE_STMT.getAndSet(stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      LOG.debug(stmt.getQueryString());
      LOG.debug("key = {}", key);

      session.execute(stmt.bind(key));

      return Status.OK;
    } catch (final Exception e) {
      LOG.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
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

        readConsistencyLevel = DefaultConsistencyLevel.valueOf(this.getProperties().getProperty(
            READ_CONSISTENCY_LEVEL_PROPERTY,
            READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        writeConsistencyLevel = DefaultConsistencyLevel.valueOf(this.getProperties().getProperty(
            WRITE_CONSISTENCY_LEVEL_PROPERTY,
            WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

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
   * @param values A HashMap of field/value pairs to insert in the record
   *
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {

    try {

      final Set<String> fields = values.keySet();
      PreparedStatement stmt = INSERT_STMTS.get(fields);

      // Prepare statement on demand

      if (stmt == null) {

        RegularInsert insertInto = QueryBuilder.insertInto(table).value(YCSB_KEY, QueryBuilder.bindMarker());

        for (final String field : fields) {
          insertInto = insertInto.value(field, QueryBuilder.bindMarker());
        }

        stmt = session.prepare(insertInto);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        final PreparedStatement prevStmt = INSERT_STMTS.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(stmt.getQueryString());
        LOG.debug("key = {}", key);
        for (final Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          LOG.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add key
      final BoundStatement boundStmt = stmt.bind().setString(0, key);

      // Add fields
      final ColumnDefinitions vars = stmt.getVariables();
      for (int i = 1; i < vars.size(); i++) {
        boundStmt.setString(i, values.get(vars.getName(i)).toString());
      }

      session.execute(boundStmt);

      return Status.OK;
    } catch (final Exception e) {
      LOG.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
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
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = (fields == null) ? READ_ALL_STMT.get() : READ_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        final Select.Builder selectBuilder;

        if (fields == null) {
          selectBuilder = QueryBuilder.select().all();
        } else {
          selectBuilder = QueryBuilder.select();
          for (final String col : fields) {
            ((Select.Selection) selectBuilder).column(col);
          }
        }

        stmt = session.prepare(selectBuilder.from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()))
            .limit(1));
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        final PreparedStatement prevStmt = (fields == null) ?
            READ_ALL_STMT.getAndSet(stmt) :
            READ_STMTS.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      LOG.debug(stmt.getQueryString());
      LOG.debug("key = {}", key);

      final ResultSet rs = session.execute(stmt.bind(key));

      if (rs.isExhausted()) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      final Row row = rs.one();
      final ColumnDefinitions cd = row.getColumnDefinitions();

      for (final ColumnDefinitions.Definition def : cd) {
        final ByteBuffer val = row.getBytesUnsafe(def.getName());
        if (val != null) {
          result.put(def.getName(), new ByteArrayByteIterator(val.array()));
        } else {
          result.put(def.getName(), null);
        }
      }

      return Status.OK;

    } catch (final Exception e) {
      LOG.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be
   * stored in
   * a HashMap. Cassandra CQL uses "token" method for range scan which doesn't always yield intuitive results.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   *
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(final String table, final String startkey, final int recordcount,
                     final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {

    try {
      PreparedStatement stmt = (fields == null) ? SCAN_ALL_STMT.get() : SCAN_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        final Select.Builder selectBuilder;

        if (fields == null) {
          selectBuilder = QueryBuilder.select().all();
        } else {
          selectBuilder = QueryBuilder.select();
          for (final String col : fields) {
            ((Select.Selection) selectBuilder).column(col);
          }
        }

        final Select selectStmt = selectBuilder.from(table);

        // The statement builder is not setup right for tokens.
        // So, we need to build it manually.
        final String initialStmt = selectStmt.toString();
        final StringBuilder scanStmt = new StringBuilder();
        scanStmt.append(initialStmt, 0, initialStmt.length() - 1);
        scanStmt.append(" WHERE ");
        scanStmt.append(QueryBuilder.token(YCSB_KEY));
        scanStmt.append(" >= ");
        scanStmt.append("token(");
        scanStmt.append(QueryBuilder.bindMarker());
        scanStmt.append(")");
        scanStmt.append(" LIMIT ");
        scanStmt.append(QueryBuilder.bindMarker());

        stmt = session.prepare(scanStmt.toString());
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        final PreparedStatement prevStmt = (fields == null) ?
            SCAN_ALL_STMT.getAndSet(stmt) :
            SCAN_STMTS.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      LOG.debug(stmt.getQueryString());
      LOG.debug("startKey = {}, recordcount = {}", startkey, recordcount);

      final ResultSet rs = session.execute(stmt.bind(startkey, Integer.valueOf(recordcount)));

      HashMap<String, ByteIterator> tuple;
      while (!rs.isExhausted()) {
        final Row row = rs.one();
        tuple = new HashMap<String, ByteIterator>();

        final ColumnDefinitions cd = row.getColumnDefinitions();

        for (final ColumnDefinitions.Definition def : cd) {
          final ByteBuffer val = row.getBytesUnsafe(def.getName());
          if (val != null) {
            tuple.put(def.getName(), new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(def.getName(), null);
          }
        }

        result.add(tuple);
      }

      return Status.OK;

    } catch (final Exception e) {
      LOG.error(
          MessageFormatter.format("Error scanning with startkey: {}", startkey).getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   *
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {

    try {
      final Set<String> fields = values.keySet();
      PreparedStatement stmt = UPDATE_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        final Update updateStmt = QueryBuilder.update(table);

        // Add fields
        for (final String field : fields) {
          updateStmt.with(QueryBuilder.set(field, QueryBuilder.bindMarker()));
        }

        // Add key
        updateStmt.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

        stmt = session.prepare(updateStmt);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        final PreparedStatement prevStmt = UPDATE_STMTS.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(stmt.getQueryString());
        LOG.debug("key = {}", key);
        for (final Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          LOG.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add fields
      final ColumnDefinitions vars = stmt.getVariables();
      final BoundStatement boundStmt = stmt.bind();
      for (int i = 0; i < vars.size() - 1; i++) {
        boundStmt.setString(i, values.get(vars.getName(i)).toString());
      }

      // Add key
      boundStmt.setString(vars.size() - 1, key);

      session.execute(boundStmt);

      return Status.OK;
    } catch (final Exception e) {
      LOG.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }
}
