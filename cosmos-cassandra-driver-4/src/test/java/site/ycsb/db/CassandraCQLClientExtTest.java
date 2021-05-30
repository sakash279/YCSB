/**
 * Copyright (c) 2015 YCSB contributors All rights reserved. Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License. See accompanying LICENSE file.
 */

package site.ycsb.db;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static site.ycsb.db.CassandraCQLClientExt.CONFIG_FILE_PROPERTY;
import static site.ycsb.db.CassandraCQLClientExt.REQUEST_TRACING_PROPERTY;
import static site.ycsb.db.CassandraCQLClientExt.YCSB_KEY;

/**
 * Integration tests for the Cosmos Cassandra API DataStax Java Driver 4 client.
 */
public class CassandraCQLClientExtTest {

  // region Fields

  @ClassRule
  public static final CassandraUnit cassandraUnit;
  private static final String TABLE_NAME;

  static {

    final UUID id = UUID.randomUUID();

    TABLE_NAME = "test_" + Long.toUnsignedString(
        id.getLeastSignificantBits() ^ id.getMostSignificantBits(),
        Character.MAX_RADIX);

    cassandraUnit = new CassandraUnit(
        getProperty("azure.cosmos.cassandra.config-file"),
        TABLE_NAME,
        new String[] { "field0", "field1", "field2", "field3", "field4", "field5" },
        Boolean.parseBoolean(getProperty("azure.cosmos.cassandra.request-tracing", "false")));

    // HOCON does not support expansion of arrays from environment variables or system properties. You must add array
    // elements one-by-one using the syntax illustrated here:
    //
    //     preferred-regions += ${?AZURE_COSMOS_CASSANDRA_PREFERRED_REGION_1}
    //     preferred-regions += ${?AZURE_COSMOS_CASSANDRA_PREFERRED_REGION_2}
    //     preferred-regions += ${?AZURE_COSMOS_CASSANDRA_PREFERRED_REGION_3}
    //     ...
    //
    // We make that easier by parsing and adding the relevant values as system properties in the code that follows.

    final String preferredRegions = System.getenv("AZURE_COSMOS_CASSANDRA_PREFERRED_REGIONS");

    if (preferredRegions != null) {

      final Map<String, String> environment = System.getenv();
      int i = 0;

      for (final String preferredRegion : preferredRegions.split("\\s*,\\s*")) {
        System.setProperty("AZURE_COSMOS_PREFERRED_REGION_" + (++i), preferredRegion);
      }
    }
  }

  private CassandraCQLClientExt client;

  // endregion

  // region Methods

  /**
   * Truncates the integration test {@link #TABLE_NAME} so that each test starts fresh.
   */
  @After
  public void clearTable() throws InterruptedException {

    final SimpleStatement truncate = QueryBuilder.truncate(TABLE_NAME).build();

    try {

      final ResultSet resultSet = cassandraUnit.getSession().execute(truncate);
      assertThat(resultSet.getExecutionInfo().getErrors().size(), is(0));

    } catch (final ServerError error) {

      if (error.getMessage().contains("not supported")) {
        cassandraUnit.recreateTable();
      } else {
        fail(error.toString());
      }

    } catch (final Throwable error) {
      fail(error.toString());
    }
  }

  @Before
  public void setUp() throws Exception {

    final Properties properties = new Properties();

    properties.setProperty(CONFIG_FILE_PROPERTY, cassandraUnit.getConfigurationFileName());
    properties.setProperty(REQUEST_TRACING_PROPERTY, Boolean.toString(cassandraUnit.getRequestTracing()));

    Measurements.setProperties(properties);

    final CoreWorkload workload = new CoreWorkload();
    workload.init(properties);

    this.client = new CassandraCQLClientExt();
    this.client.setProperties(properties);
    this.client.init();
  }

  @After
  public void tearDownClient() throws Exception {
    if (this.client != null) {
      this.client.cleanup();
    }
    this.client = null;
  }

  @Test
  public void testDelete() {

    this.insertRow();

    final Status status = this.client.delete(TABLE_NAME, YCSB_KEY);
    assertThat(status, is(Status.OK));

    final Select select = QueryBuilder.selectFrom(TABLE_NAME)
        .columns("field0", "field1")
        .where(Relation.column(YCSB_KEY).isEqualTo(literal(YCSB_KEY)))
        .limit(1);

    final ResultSet resultSet = cassandraUnit.getSession().execute(select.build());
    final Row row = resultSet.one();

    assertThat(row, nullValue());
  }

  @Test
  public void testInsert() {
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2");

    final Status status = this.client.insert(TABLE_NAME, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    final Select select =
        QueryBuilder.selectFrom(TABLE_NAME)
            .columns("field0", "field1")
            .where(Relation.column(YCSB_KEY).isEqualTo(literal(key)))
            .limit(1);

    final ResultSet resultSet = cassandraUnit.getSession().execute(select.build());
    final Row row = resultSet.one();

    assertThat(row, notNullValue());
    assertThat(row.getString("field0"), is("value1"));
    assertThat(row.getString("field1"), is("value2"));
    assertThat(resultSet.isFullyFetched() && resultSet.getAvailableWithoutFetching() == 0, is(true));
  }

  @Test
  public void testPreparedStatements() {
    final int LOOP_COUNT = 3;
    for (int i = 0; i < LOOP_COUNT; i++) {
      this.testInsert();
      this.testUpdate();
      this.testRead();
      this.testReadSingleColumn();
      this.testReadMissingRow();
      this.testDelete();
    }
  }

  @Test
  public void testRead() {

    this.insertRow();

    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status status = this.client.read(TABLE_NAME, YCSB_KEY, null, result);

    assertThat(status, is(Status.OK));

    assertThat(result.entrySet(), hasSize(cassandraUnit.getColumnCount()));

    assertThat(result.get(YCSB_KEY), notNullValue());
    assertThat(result.get("field0"), notNullValue());
    assertThat(result.get("field1"), notNullValue());

    assertThat(result, hasEntry("field2", null));
    assertThat(result, hasEntry("field3", null));
    assertThat(result, hasEntry("field4", null));
    assertThat(result, hasEntry("field5", null));

    final HashMap<String, String> namedValues = new HashMap<>();

    for (final Map.Entry<String, ByteIterator> entry : result.entrySet()) {
      if (entry.getValue() != null) {
        namedValues.put(entry.getKey(), entry.getValue().toString());
      }
    }

    assertThat(namedValues, hasEntry(YCSB_KEY, YCSB_KEY));
    assertThat(namedValues, hasEntry("field0", "value1"));
    assertThat(namedValues, hasEntry("field1", "value2"));
  }

  @Test
  public void testReadMissingRow() {
    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status status = this.client.read(TABLE_NAME, "Missing row", null, result);
    assertThat(result.size(), is(0));
    assertThat(status, is(Status.NOT_FOUND));
  }

  @Test
  public void testReadSingleColumn() {

    this.insertRow();

    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Set<String> fields = new HashSet<>(Collections.singleton("field1"));
    final Status status = this.client.read(TABLE_NAME, YCSB_KEY, fields, result);

    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(1));

    final Map<String, String> strResult = StringByteIterator.getStringMap(result);
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testUpdate() {

    this.insertRow();

    final Map<String, String> input = new HashMap<>();
    input.put("field0", "new-value1");
    input.put("field1", "new-value2");

    final Status status = this.client.update(TABLE_NAME, YCSB_KEY, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    final Select select =
        QueryBuilder.selectFrom(TABLE_NAME)
            .columns("field0", "field1")
            .where(Relation.column(YCSB_KEY).isEqualTo(literal(YCSB_KEY)))
            .limit(1);

    final ResultSet resultSet = cassandraUnit.getSession().execute(select.build());
    final Row row = resultSet.one();

    assertThat(row, notNullValue());
    assertThat(row.getString("field0"), is("new-value1"));
    assertThat(row.getString("field1"), is("new-value2"));
    assertThat(resultSet.isFullyFetched() && resultSet.getAvailableWithoutFetching() == 0, is(true));
  }

  // endregion

  // region Privates

  private void insertRow() {
    final Insert insert = insertInto(TABLE_NAME)
        .value(YCSB_KEY, literal(YCSB_KEY))
        .value("field0", literal("value1"))
        .value("field1", literal("value2"));
    cassandraUnit.getSession().execute(insert.build());
  }

  // endregion

  private static class CassandraUnit implements TestRule {

    final String[] columnNames;
    final File configurationFile;
    final boolean requestTracing;
    final String tableName;
    CqlSession session;

    CassandraUnit(
        final String configurationFileName,
        final String tableName,
        final String[] columnNames,
        final boolean requestTracing) {

      requireNonNull(configurationFileName, "expected non-null configurationFileName");
      requireNonNull(configurationFileName, "expected non-null tableName");

      this.configurationFile = new File(configurationFileName);
      this.session = null;
      this.tableName = tableName;
      this.columnNames = columnNames;
      this.requestTracing = requestTracing;

      assertThat(this.configurationFile.exists(), is(true));
    }

    public int getColumnCount() {
      return this.columnNames.length + 1;
    }

    public String getConfigurationFileName() {
      return this.configurationFile.getPath();
    }

    public boolean getRequestTracing() {
      return this.requestTracing;
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          try {
            CassandraUnit.this.before();
            base.evaluate();
          } finally {
            CassandraUnit.this.after();
          }
        }
      };
    }

    public void recreateTable() throws InterruptedException {

      final Drop drop = SchemaBuilder.dropTable(this.tableName).ifExists();
      this.session.execute(drop.build());

      CreateTable createTable = SchemaBuilder.createTable(this.tableName).withPartitionKey(YCSB_KEY, DataTypes.TEXT);

      for (final String name : this.columnNames) {
        createTable = createTable.withColumn(name, DataTypes.TEXT);
      }

      this.session.execute(createTable.build().setTimeout(Duration.ofSeconds(10)));
      Thread.sleep(2_000L);  // allowing time for the table creation to sync across regions
    }

    CqlSession getSession() {
      return this.session;
    }

    String getTableName() {
      return this.tableName;
    }

    private void after() {
      if (this.session != null) {
        final Drop drop = SchemaBuilder.dropTable(this.tableName).ifExists();
        this.session.execute(drop.build());
        this.session.close();
        this.session = null;
      }
    }

    private void before() throws InterruptedException {

      this.after();

      final DriverConfigLoader configLoader = DriverConfigLoader.fromFile(this.configurationFile);
      final String applicationName = "ycsb.cosmos-cassandra-driver-4-binding.integration-test";
      final DriverOption sessionKeyspaceOption = DefaultDriverOption.SESSION_KEYSPACE;

      final String keyspaceName = configLoader.getInitialConfig()
          .getDefaultProfile()
          .getString(DefaultDriverOption.SESSION_KEYSPACE);

      assertThat(keyspaceName, notNullValue());
      assertThat(keyspaceName, not(""));

      this.session = null;

      for (int i = 0; i <= 1; i++) {

        try {

          this.session = CqlSession.builder().withApplicationName(applicationName)
              .withConfigLoader(configLoader)
              .build();

          // TODO (DANOBLE) eliminate the need for this workaround
          //  It happens that after dropping a keyspace, the Cosmos Cassandra API allows us to set the session keyspace
          //  even though the keyspace no longer exists. More than that this statement will do nothing because--from a
          //  Cosmos Cassandra API perspective, the keyspace still exists.
          //  Evidence:
          //  - The session can be configured with the non-existent keyspace.
          //        datastax-java-driver.basic.session-keyspace = <non-existent-keyspace-name>
          //    Expected: Setting session-keyspace to a non-existent keyspace name causes CqlSessionBuilder::build to
          //    throw an InvalidKeyspaceException.
          //  - This command does not create a keyspace:
          //        CREATE KEYSPACE IF NOT EXISTS <non-existent-keyspace-name>
          //    Expected: A keyspace with the non-existent keyspace name is created.
          //  Workaround:
          //  - Try to create the keyspace:
          //        CREATE KEYSPACE <non-existent-keyspace-name>
          //  - When the command fails catch and ignore the AlreadyExistsException thrown by CqlSession::execute.

          final SimpleStatement createKeyspace = SchemaBuilder.createKeyspace(keyspaceName)
              .withSimpleStrategy(4)
              .build();

          this.session.execute(createKeyspace);
          break;

        } catch (final InvalidKeyspaceException error) {

          final DriverConfigLoader programmaticConfigLoader = DriverConfigLoader.programmaticBuilder()
              .withString(sessionKeyspaceOption, "system")
              .build();

          try (CqlSession session = CqlSession.builder().withApplicationName(applicationName)
              .withConfigLoader(DriverConfigLoader.compose(programmaticConfigLoader, configLoader))
              .build()) {

            final SimpleStatement createKeyspace = SchemaBuilder.createKeyspace(keyspaceName)
                .withSimpleStrategy(4)
                .build();

            session.execute(createKeyspace);
          }
        } catch (final AlreadyExistsException error) {
          break;
        } catch (final Throwable error) {
          fail(error.toString());
        }
      }

      assertThat(this.session, notNullValue());
      this.recreateTable();
    }
  }
}
