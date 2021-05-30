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
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
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
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
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
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static site.ycsb.db.CassandraCQLClientExt.YCSB_KEY;

/**
 * Integration tests for the Cassandra client
 */
public class CassandraCQLClientExtTest {

  // region Fields

  @ClassRule
  public static final CassandraUnit cassandraUnit;

  private static final String[] COLUMN_NAMES = { "field0", "field1" };
  private static final String TABLE_NAME;

  private CassandraCQLClientExt client;
  private CqlSession session;

  static {
    final UUID id = UUID.randomUUID();
    TABLE_NAME = "integration_test_" + (id.getLeastSignificantBits() ^ id.getMostSignificantBits());
    cassandraUnit = new CassandraUnit(getProperty("azure.cosmos.cassandra.config-file"), TABLE_NAME, COLUMN_NAMES);
  }
  
  // endregion

  // region Methods

  /**
   * Truncates the integration test {@link #TABLE_NAME} so that each test starts fresh.
   */
  @After
  public void clearTable() {
    final Truncate truncate = QueryBuilder.truncate(TABLE_NAME);
    cassandraUnit.getSession().execute(truncate.build());
  }

  @Before
  public void setUp() throws Exception {

    this.session = cassandraUnit.getSession();

    final Properties properties = new Properties();

    properties.setProperty("hosts", cassandraUnit.getHosts().get(0));
    properties.setProperty("port", Integer.toString(cassandraUnit.getPort()));
    properties.setProperty("table", cassandraUnit.getTableName());

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

    final ResultSet resultSet = this.session.execute(select.build());
    final Row row = resultSet.one();

    assertThat(row, nullValue());
  }

  @Test
  public void testInsert() {
    final String key = "key";
    final Map<String, String> input = new HashMap<String, String>();
    input.put("field0", "value1");
    input.put("field1", "value2");

    final Status status = this.client.insert(TABLE_NAME, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    final Select select =
        QueryBuilder.selectFrom(TABLE_NAME)
            .columns("field0", "field1")
            .where(Relation.column(YCSB_KEY).isEqualTo(literal(key)))
            .limit(1);

    final ResultSet resultSet = this.session.execute(select.build());
    final Row row = resultSet.one();

    assertThat(row, notNullValue());
    assertThat(row.getString("field0"), is("value1"));
    assertThat(row.getString("field1"), is("value2"));
    assertThat(resultSet.isFullyFetched() && resultSet.getAvailableWithoutFetching() == 0, is(true));
  }

  @Test
  public void testPreparedStatements() throws Exception {
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

    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = this.client.read(TABLE_NAME, YCSB_KEY, null, result);

    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(11));
    assertThat(result, hasEntry("field2", null));

    final HashMap<String, String> strResult = new HashMap<String, String>();

    for (final Map.Entry<String, ByteIterator> e : result.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }

    assertThat(strResult, hasEntry(YCSB_KEY, YCSB_KEY));
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testReadMissingRow() {
    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
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

    final ResultSet resultSet = this.session.execute(select.build());
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
    this.session.execute(insert.build());
  }

  // endregion

  private static class CassandraUnit implements TestRule {

    final String[] columnNames;
    final File configurationFile;
    final String tableName;
    CqlSession session;

    CassandraUnit(final String configurationFileName, final String tableName, final String[] columnNames) {

      requireNonNull(configurationFileName, "expected non-null configurationFileName");
      requireNonNull(configurationFileName, "expected non-null tableName");

      this.session = null;
      this.tableName = tableName;
      this.columnNames = columnNames;
      this.configurationFile = new File(configurationFileName);

      assertThat(this.configurationFile.exists(), is(true));
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          CassandraUnit.this.before();
          try {
            CassandraUnit.this.before();
          } finally {
            CassandraUnit.this.after();
          }
        }
      };
    }

    List<String> getHosts() {
      final Collection<Node> nodes = this.session.getMetadata().getNodes().values();
      return nodes.stream()
          .map(node -> getInetSocketAddress(node).getHostName())
          .collect(Collectors.toList());
    }

    int getPort() {
      final InternalDriverContext driverContext = (InternalDriverContext) this.session.getContext();
      final Node node = driverContext.getMetadataManager().getContactPoints().iterator().next();
      final InetSocketAddress address = getInetSocketAddress(node);
      return address.getPort();
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

      this.session = CqlSession.builder()
          .withConfigLoader(DriverConfigLoader.fromFile(this.configurationFile))
          .withApplicationName("ycsb.cassandra.driver-4.integration-test")
          .withKeyspace("ycsb")
          .build();

      final CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace("ycsb").ifNotExists().withSimpleStrategy(4);
      this.session.execute(createKeyspace.build());

      final Drop drop = SchemaBuilder.dropTable(this.tableName).ifExists();
      this.session.execute(drop.build());

      CreateTable createTable = SchemaBuilder.createTable(this.tableName).withPartitionKey(YCSB_KEY, DataTypes.TEXT);

      for (final String name : this.columnNames) {
        createTable = createTable.withColumn(name, DataTypes.TEXT);
      }

      this.session.execute(createTable.build());
      Thread.sleep(5_000L);  // allowing time for the table creation to sync across regions
    }

    private static InetSocketAddress getInetSocketAddress(final Node node) {
      final InetSocketAddress address = (InetSocketAddress) node.getEndPoint().resolve();
      return address.isUnresolved() ? address : new InetSocketAddress(address.getHostName(), address.getPort());
    }
  }
}
