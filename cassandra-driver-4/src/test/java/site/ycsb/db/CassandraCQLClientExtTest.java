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
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import com.google.common.collect.Sets;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for the Cassandra client
 */
public class CassandraCQLClientExtTest {

  // region Fields

  @ClassRule
  public static CassandraUnit cassandraUnit = new CassandraUnit(System.getProperty("cassandra.driver-4.config-file"));

  private static final String DEFAULT_ROW_KEY = "integration_test";
  private static final String TABLE = "users";

  private CassandraCQLClientExt client;
  private CqlSession session;

  // endregion

  // region Methods

  @After
  public void clearTable() {
    // Clear the table so that each test starts fresh.
    final Truncate truncate = QueryBuilder.truncate(TABLE);
    if (cassandraUnit != null) {
      cassandraUnit.getSession().execute(truncate.build());
    }
  }

  @Before
  public void setUp() throws Exception {

    this.session = cassandraUnit.getSession();

    final Properties properties = new Properties();

    properties.setProperty("config-file", cassandraUnit.configurationFile.toString());
    properties.setProperty("table", TABLE);

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

    final Status status = this.client.delete(TABLE, DEFAULT_ROW_KEY);
    assertThat(status, is(Status.OK));

    final Select select = QueryBuilder.selectFrom(TABLE)
        .columns("field0", "field1")
        .where(Relation.column(CassandraCQLClientExt.YCSB_KEY).isEqualTo(literal(DEFAULT_ROW_KEY)))
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

    final Status status = this.client.insert(TABLE, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    final Select select =
        QueryBuilder.selectFrom(TABLE)
            .columns("field0", "field1")
            .where(Relation.column(CassandraCQLClientExt.YCSB_KEY).isEqualTo(literal(key)))
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
    final Status status = this.client.read(TABLE, DEFAULT_ROW_KEY, null, result);

    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(11));
    assertThat(result, hasEntry("field2", null));

    final HashMap<String, String> strResult = new HashMap<String, String>();

    for (final Map.Entry<String, ByteIterator> e : result.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }

    assertThat(strResult, hasEntry(CassandraCQLClientExt.YCSB_KEY, DEFAULT_ROW_KEY));
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testReadMissingRow() {
    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = this.client.read(TABLE, "Missing row", null, result);
    assertThat(result.size(), is(0));
    assertThat(status, is(Status.NOT_FOUND));
  }

  @Test
  public void testReadSingleColumn() {

    this.insertRow();

    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Set<String> fields = Sets.newHashSet("field1");
    final Status status = this.client.read(TABLE, DEFAULT_ROW_KEY, fields, result);

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

    final Status status = this.client.update(TABLE, DEFAULT_ROW_KEY, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    final Select select =
        QueryBuilder.selectFrom(TABLE)
            .columns("field0", "field1")
            .where(Relation.column(CassandraCQLClientExt.YCSB_KEY).isEqualTo(literal(DEFAULT_ROW_KEY)))
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
    final Insert insert = insertInto(TABLE)
        .value(CassandraCQLClientExt.YCSB_KEY, literal(DEFAULT_ROW_KEY))
        .value("field0", literal("value1"))
        .value("field1", literal("value2"));
    this.session.execute(insert.build());
  }

  // endregion

  private static class CassandraUnit implements TestRule {

    final File configurationFile;
    CqlSession session;

    CassandraUnit(final String configurationFileName) {
      requireNonNull(configurationFileName, "expected non-null configurationFileName");
      this.session = null;
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

    CqlSession getSession() {
      throw new UnsupportedOperationException();
    }

    private void after() {
      if (this.session != null) {
        this.session.close();
        this.session = null;
      }
    }

    private void before() {
      this.after();
      this.session = CqlSession.builder()
          .withConfigLoader(DriverConfigLoader.fromFile(this.configurationFile))
          .withApplicationName("ycsb.cassandra.driver-4.integration-test")
          .build();
    }
  }
}
