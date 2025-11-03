/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.datastream.values.DatastreamRow;
import com.google.cloud.teleport.v2.datastream.values.DmlInfo;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of Database Migration utilities to convert JSON data to DML. */
public abstract class DatastreamToDML
    extends DoFn<FailsafeElement<String, String>, KV<String, DmlInfo>> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToDML.class);

  public static class ColumnInfo {
    public final String typeName;
    public final String isNullable; // "YES", "NO", "" (empty string if unknown)

    public ColumnInfo(String typeName, String isNullable) {
      this.typeName = typeName;
      this.isNullable = isNullable;
    }

    public String getTypeName() {
      return typeName;
    }

    public String getIsNullable() {
      return isNullable;
    }
  }

  private static String rowIdColumnName = "rowid";
  private static List<String> defaultPrimaryKeys;
  private static MappedObjectCache<List<String>, Map<String, DatastreamToDML.ColumnInfo>> tableCache;
  private static MappedObjectCache<List<String>, List<String>> primaryKeyCache;
  private CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration;
  private DataSource dataSource;
  public String quoteCharacter;
  protected Map<String, String> schemaMap = new HashMap<String, String>();

  public abstract String getDefaultQuoteCharacter();

  public abstract String getDeleteDmlStatement();

  public abstract String getUpsertDmlStatement();

  public abstract String getInsertDmlStatement();

  public abstract String getTargetCatalogName(DatastreamRow row);

  public abstract String getTargetSchemaName(DatastreamRow row);

  public abstract String getTargetTableName(DatastreamRow row);

  /* An exception for delete DML without a primary key */
  private class DeletedWithoutPrimaryKey extends RuntimeException {
    public DeletedWithoutPrimaryKey(String errorMessage) {
      super(errorMessage);
    }
  }

  public DatastreamToDML(CdcJdbcIO.DataSourceConfiguration config) {
    this.dataSourceConfiguration = config;
    this.quoteCharacter = getDefaultQuoteCharacter();
  }

  public DatastreamToDML withQuoteCharacter(String quoteChar) {
    this.quoteCharacter = quoteChar;
    return this;
  }

  public DatastreamToDML withSchemaMap(Map<String, String> schemaMap) {
    this.schemaMap = schemaMap;
    return this;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    FailsafeElement<String, String> element = context.element();
    String jsonString = element.getPayload();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rowObj;

    try {
      rowObj = mapper.readTree(jsonString);
      DmlInfo dmlInfo = convertJsonToDmlInfo(rowObj, element.getOriginalPayload());

      // Null rows suggest no DML is required.
      if (dmlInfo != null) {
        LOG.debug("Output Data: {}", jsonString);
        context.output(KV.of(dmlInfo.getStateWindowKey(), dmlInfo));
      } else {
        LOG.debug("Skipping Null DmlInfo: {}", jsonString);
      }
    } catch (IOException e) {
      // TODO: Push failure to DLQ collection
      LOG.error("IOException: {} :: {}", jsonString, e.toString());
    }
  }

  protected String cleanTableName(String tableName) {
    return applySchemaMap(tableName);
  }

  protected String cleanSchemaName(String schemaName) {
    schemaName = applySchemaMap(schemaName);

    return schemaName;
  }

  protected String applySchemaMap(String sourceSchema) {
    return schemaMap.getOrDefault(sourceSchema, sourceSchema);
  }

  protected String applyLowercase(String name) {
    return name.toLowerCase();
  }

  // TODO(dhercher): Only if source is oracle, pull from DatastreamRow
  public List<String> getDefaultPrimaryKeys() {
    if (this.defaultPrimaryKeys == null) {
      this.defaultPrimaryKeys = Arrays.asList(this.rowIdColumnName);
    }
    return this.defaultPrimaryKeys;
  }

  public DataSource getDataSource() {
    if (this.dataSource == null) {
      this.dataSource = this.dataSourceConfiguration.buildDatasource();
    }
    return this.dataSource;
  }

  private synchronized void setUpTableCache() {
    if (tableCache == null) {
      tableCache = new JdbcTableCache(this.getDataSource()).withCacheResetTimeUnitValue(1440);
    }
  }

  private synchronized void setUpPrimaryKeyCache() {
    if (primaryKeyCache == null) {
      primaryKeyCache =
          new JdbcPrimaryKeyCache(this.getDataSource()).withCacheResetTimeUnitValue(1440);
    }
  }

  public Map<String, DatastreamToDML.ColumnInfo> getTableSchema(
      String catalogName, String schemaName, String tableName) {
    List<String> searchKey = ImmutableList.of(catalogName, schemaName, tableName);

    if (this.tableCache == null) {
      setUpTableCache();
    }

    return this.tableCache.get(searchKey);
  }

  public List<String> getPrimaryKeys(
      String catalogName, String schemaName, String tableName, JsonNode rowObj) {
    List<String> searchKey = ImmutableList.of(catalogName, schemaName, tableName);

    if (this.primaryKeyCache == null) {
      setUpPrimaryKeyCache();
    }

    List<String> primaryKeys = this.primaryKeyCache.get(searchKey);
    // If primary keys exist, but are not in the data
    // than we assume this is a rolled back delete.
    for (String primaryKey : primaryKeys) {
      if (!rowObj.has(primaryKey)) {
        return this.getDefaultPrimaryKeys();
      }
    }

    return primaryKeys;
  }

  public String quote(String name) {
    return quoteCharacter + name + quoteCharacter;
  }

  public DmlInfo convertJsonToDmlInfo(JsonNode rowObj, String failsafeValue) {
    DatastreamRow row = DatastreamRow.of(rowObj);
    try {
      // Oracle uses upper case while Postgres uses all lowercase.
      // We lowercase the values of these metadata fields to align with
      // our schema conversion rules.
      String catalogName = this.getTargetCatalogName(row);
      String schemaName = this.getTargetSchemaName(row);
      String tableName = this.getTargetTableName(row);

      Map<String, DatastreamToDML.ColumnInfo> tableSchema = this.getTableSchema(catalogName, schemaName, tableName);
      if (tableSchema.isEmpty()) {
        // If the table DNE we return null (NOOP).
        return null;
      }

      List<String> primaryKeys = this.getPrimaryKeys(catalogName, schemaName, tableName, rowObj);
      List<String> orderByFields = row.getSortFields();
      List<String> primaryKeyValues = getFieldValues(rowObj, primaryKeys, tableSchema);
      List<String> orderByValues = getFieldValues(rowObj, orderByFields, tableSchema);

      String dmlSqlTemplate = getDmlTemplate(rowObj, primaryKeys);
      Map<String, String> sqlTemplateValues =
          getSqlTemplateValues(
              rowObj, catalogName, schemaName, tableName, primaryKeys, tableSchema);

      String dmlSql = StringSubstitutor.replace(dmlSqlTemplate, sqlTemplateValues, "{", "}");
      return DmlInfo.of(
          failsafeValue,
          dmlSql,
          schemaName,
          tableName,
          primaryKeys,
          orderByFields,
          primaryKeyValues,
          orderByValues);
    } catch (DeletedWithoutPrimaryKey e) {
      LOG.error("CDC Error: {} :: {}", rowObj.toString(), e.toString());
      return null;
    } catch (Exception e) {
      // TODO(dhercher): Consider raising an error and pushing to DLQ
      LOG.error("Value Error: {} :: {}", rowObj.toString(), e.toString());
      return null;
    }
  }

  public String getDmlTemplate(JsonNode rowObj, List<String> primaryKeys) {
    Boolean isDelete = rowObj.get("_metadata_deleted").asBoolean();
    Boolean hasPrimaryKeys = primaryKeys.size() != 0;
    if (isDelete && !hasPrimaryKeys) {
      throw new DeletedWithoutPrimaryKey("Delete DML without primary keys cannot be applied");
    } else if (isDelete) {
      return getDeleteDmlStatement();
    } else if (hasPrimaryKeys) {
      return getUpsertDmlStatement();
    } else {
      return getInsertDmlStatement();
    }
  }

  public Map<String, String> getSqlTemplateValues(
      JsonNode rowObj,
      String catalogName,
      String schemaName,
      String tableName,
      List<String> primaryKeys,
      Map<String, DatastreamToDML.ColumnInfo> tableSchema) {
    Map<String, String> sqlTemplateValues = new HashMap<>();

    sqlTemplateValues.put("quoted_catalog_name", quote(catalogName));
    sqlTemplateValues.put("quoted_schema_name", quote(schemaName));
    sqlTemplateValues.put("quoted_table_name", quote(tableName));
    sqlTemplateValues.put(
        "primary_key_kv_sql", getPrimaryKeyToValueFilterSql(rowObj, primaryKeys, tableSchema));
    sqlTemplateValues.put("quoted_column_names", getColumnsListSql(rowObj, tableSchema));
    sqlTemplateValues.put("column_value_sql", getColumnsValuesSql(rowObj, tableSchema));
    sqlTemplateValues.put("primary_key_names_sql", String.join(",", primaryKeys)); // TODO: quoted?
    sqlTemplateValues.put("column_kv_sql", getColumnsUpdateSql(rowObj, tableSchema));

    return sqlTemplateValues;
  }

  public String getValueSql(JsonNode rowObj, String columnName, Map<String, DatastreamToDML.ColumnInfo> tableSchema) {
    JsonNode columnObj = rowObj.get(columnName);

    String rawValue;
    boolean isText;

    // Handle JSON null immediately for all objects.
    if (columnObj == null || columnObj.isNull()) {
      // The type-specific cleaner will decide if a non-nullable column needs a default.
      rawValue = null;
      isText = false; // Treat as non-text to avoid quoting 'null' later.
    } else {
      isText = columnObj.isTextual();
      if (isText) {
        rawValue = columnObj.textValue();
      } else {
        rawValue = columnObj.toString(); // For numbers, booleans etc.
      }
    }
    
    // Pass the raw, unquoted value to the type-specific cleaner
    String cleanedValue = cleanDataTypeValueSql(rawValue, columnName, tableSchema);

    // If the cleaner returns null, it means we should use the SQL NULL value.
    if (cleanedValue == null) {
      return getNullValueSql();
    }

    // Check if cleanedValue is a special SQL form/function that shouldn't be quoted
    if (cleanedValue.equalsIgnoreCase("NULL") // cleaner might return "NULL" for empty strings etc.
        || cleanedValue.startsWith("UNHEX(")
        || cleanedValue.startsWith("X'")
        /* add other function calls or special values here if needed */) {
      return cleanedValue;
    }

    // If it was originally a text node and not a special SQL form, then quote it.
    // Non-textual nodes (numbers, booleans) are already valid SQL literals via rawValue/cleanedValue.
    if (isText) {
      return "'" + cleanSql(cleanedValue) + "'";
    } else {
        // For numbers, booleans, if cleanDataTypeValueSql didn't change them,
        // they are already in a form that doesn't need quoting.
        // If it did change them (e.g. number to string), it should be a special form
        // or handled by cleanDataTypeValueSql to return a string that might need quoting (covered by isText).
        return cleanedValue;
    }
  }

  public String cleanDataTypeValueSql(
      String columnValue, String columnName, Map<String, DatastreamToDML.ColumnInfo> tableSchema) {
    // This method is intended to be overridden by database-specific DML generators.
    // The base implementation returns the value as-is.
    return columnValue;
  }

  public String getNullValueSql() {
    return "NULL";
  }

  public static String cleanSql(String str) {
    if (str == null) {
      return null;
    }
    String cleanedNullBytes = StringUtils.replace(str, "\u0000", "");

    return escapeSql(cleanedNullBytes);
  }

  public static String escapeSql(String str) {
    return StringUtils.replace(str, "'", "''");
  }

  public List<String> getFieldValues(
      JsonNode rowObj, List<String> fieldNames, Map<String, DatastreamToDML.ColumnInfo> tableSchema) {
    List<String> fieldValues = new ArrayList<String>();

    for (String fieldName : fieldNames) {
      fieldValues.add(getValueSql(rowObj, fieldName, tableSchema));
    }

    return fieldValues;
  }

  public String getColumnsListSql(JsonNode rowObj, Map<String, DatastreamToDML.ColumnInfo> tableSchema) {
    String columnsListSql = "";

    for (Iterator<String> fieldNames = rowObj.fieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      if (!tableSchema.containsKey(columnName)) {
        continue;
      }

      // Add column name
      String quotedColumnName = quote(columnName);
      if (Objects.equals(columnsListSql, "")) {
        columnsListSql = quotedColumnName;
      } else {
        columnsListSql = columnsListSql + "," + quotedColumnName;
      }
    }

    return columnsListSql;
  }

  public String getColumnsValuesSql(JsonNode rowObj, Map<String, DatastreamToDML.ColumnInfo> tableSchema) {
    String valuesInsertSql = "";

    for (Iterator<String> fieldNames = rowObj.fieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      if (!tableSchema.containsKey(columnName)) {
        continue;
      }

      String columnValue = getValueSql(rowObj, columnName, tableSchema);
      if (Objects.equals(valuesInsertSql, "")) {
        valuesInsertSql = columnValue;
      } else {
        valuesInsertSql = valuesInsertSql + "," + columnValue;
      }
    }

    return valuesInsertSql;
  }

  public String getColumnsUpdateSql(JsonNode rowObj, Map<String, DatastreamToDML.ColumnInfo> tableSchema) {
    String onUpdateSql = "";
    for (Iterator<String> fieldNames = rowObj.fieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      if (!tableSchema.containsKey(columnName)) {
        continue;
      }

      String quotedColumnName = quote(columnName);
      String columnValue = getValueSql(rowObj, columnName, tableSchema);

      if (onUpdateSql.equals("")) {
        onUpdateSql = quotedColumnName + "=" + columnValue;
      } else {
        onUpdateSql = onUpdateSql + "," + quotedColumnName + "=" + columnValue;
      }
    }

    return onUpdateSql;
  }

  public String getPrimaryKeyToValueFilterSql(
      JsonNode rowObj, List<String> primaryKeys, Map<String, DatastreamToDML.ColumnInfo> tableSchema) {
    String pkToValueSql = "";

    for (String columnName : primaryKeys) {
      if (!tableSchema.containsKey(columnName)) {
        continue;
      }

      String columnValue = getValueSql(rowObj, columnName, tableSchema);
      if (pkToValueSql.equals("")) {
        pkToValueSql = columnName + "=" + columnValue;
      } else {
        pkToValueSql = pkToValueSql + " AND " + columnName + "=" + columnValue;
      }
    }

    return pkToValueSql;
  }

  private static Connection getConnection(
      DataSource dataSource, int retriesRemaining, int maxRetries) {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      if (retriesRemaining > 0) {
        int sleepSecs = (maxRetries - retriesRemaining + 1) * 10;
        LOG.info(
            "SQLException: Will retry after {} seconds: Connection Error: {}",
            sleepSecs,
            e.toString());
        try {
          Thread.sleep(sleepSecs * 1000);
          return getConnection(dataSource, retriesRemaining - 1, maxRetries);
        } catch (InterruptedException i) {
          LOG.info("InterruptedException retrieving connection");
        }
      }
      LOG.error("SQLException: Connection Error: {}", e.toString());
    }

    return connection;
  }

  /**
   * The {@link JdbcTableCache} manages safely getting and setting JDBC Table objects from a local
   * cache for each worker thread.
   *
   * <p>The key factors addressed are ensuring expiration of cached tables, consistent update
   * behavior to ensure reliability, and easy cache reloads. Open Question: Does the class require
   * thread-safe behaviors? Currently, it does not since there is no iteration and get/set are not
   * continuous.
   */
  public static class JdbcTableCache
      extends MappedObjectCache<List<String>, Map<String, DatastreamToDML.ColumnInfo>> {

    private DataSource dataSource;
    private static final int MAX_RETRIES = 5;

    /**
     * Create an instance of a {@link JdbcTableCache} to track table schemas.
     *
     * @param dataSource A DataSource instance used to extract Table objects.
     */
    public JdbcTableCache(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    private Map<String, DatastreamToDML.ColumnInfo> getTableSchema(
        String catalogName, String schemaName, String tableName, int retriesRemaining) {
      Map<String, DatastreamToDML.ColumnInfo> tableSchema = new HashMap<>(); // Changed type here

      try (Connection connection = getConnection(this.dataSource, MAX_RETRIES, MAX_RETRIES)) {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(catalogName, schemaName, tableName, null)) {

          while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String typeName = columns.getString("TYPE_NAME");
            String isNullable = columns.getString("IS_NULLABLE"); // "YES", "NO", or ""
            tableSchema.put(columnName, new DatastreamToDML.ColumnInfo(typeName, isNullable)); // Store ColumnInfo
          }
        }
      } catch (SQLException e) {
        if (retriesRemaining > 0) {
          int sleepSecs = (MAX_RETRIES - retriesRemaining + 1) * 10;
          LOG.info(
              "SQLException, will retry after {} seconds: Failed to Retrieve Schema: {}.{} : {}",
              sleepSecs,
              schemaName,
              tableName,
              e.toString());
          try {
            Thread.sleep(sleepSecs * 1000);
            return getTableSchema(catalogName, schemaName, tableName, retriesRemaining - 1);
          } catch (InterruptedException i) {
            LOG.info("InterruptedException retrieving schema: {}.{}", schemaName, tableName);
          }
        }
        LOG.error(
            "SQLException: Failed to Retrieve Schema: {}.{} : {}",
            schemaName,
            tableName,
            e.toString());
      }

      if (tableSchema.isEmpty()) {
        LOG.info(
            "Table Not Found: Catalog: {}, Schema: {}, Table: {}",
            catalogName,
            schemaName,
            tableName);
      }
      return tableSchema;
    }

    @Override
    public Map<String, DatastreamToDML.ColumnInfo> getObjectValue(List<String> key) { // Changed return type
      String catalogName = key.get(0);
      String schemaName = key.get(1);
      String tableName = key.get(2);

      // Call the modified getTableSchema
      Map<String, DatastreamToDML.ColumnInfo> tableSchema =
          getTableSchema(catalogName, schemaName, tableName, MAX_RETRIES);

      return tableSchema; // Return Map<String, ColumnInfo>
    }
  }

  /**
   * The {@link JdbcPrimaryKeyCache} manages safely getting and setting JDBC Table PKs from a local
   * cache for each worker thread.
   *
   * <p>The key factors addressed are ensuring expiration of cached tables, consistent update
   * behavior to ensure reliability, and easy cache reloads. Open Question: Does the class require
   * thread-safe behaviors? Currently, it does not since there is no iteration and get/set are not
   * continuous.
   */
  public static class JdbcPrimaryKeyCache extends MappedObjectCache<List<String>, List<String>> {

    private DataSource dataSource;
    private static final int MAX_RETRIES = 5;

    /**
     * Create an instance of a {@link JdbcPrimaryKeyCache} to track table primary keys.
     *
     * @param dataSource A DataSource instance used to extract Table objects.
     */
    public JdbcPrimaryKeyCache(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    private List<String> getTablePrimaryKeys(
        String catalogName, String schemaName, String tableName, int retriesRemaining) {
      List<String> primaryKeys = new ArrayList<String>();
      try (Connection connection = getConnection(this.dataSource, MAX_RETRIES, MAX_RETRIES)) {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet jdbcPrimaryKeys =
            metaData.getPrimaryKeys(catalogName, schemaName, tableName)) {
          while (jdbcPrimaryKeys.next()) {
            primaryKeys.add(jdbcPrimaryKeys.getString("COLUMN_NAME"));
          }
        }
      } catch (SQLException e) {
        if (retriesRemaining > 0) {
          int sleepSecs = (MAX_RETRIES - retriesRemaining + 1) * 10;
          LOG.info(
              "SQLException, will retry after {} seconds: Failed to Retrieve Primary Key: {}.{} :"
                  + " {}",
              sleepSecs,
              schemaName,
              tableName,
              e.toString());
          try {
            Thread.sleep(sleepSecs * 1000);
            return getTablePrimaryKeys(catalogName, schemaName, tableName, retriesRemaining - 1);
          } catch (InterruptedException i) {
            LOG.info("InterruptedException retrieving pk: {}.{}", schemaName, tableName);
          }
        }
        LOG.error(
            "SQLException: Failed to Retrieve Primary Key: {}.{} : {}",
            schemaName,
            tableName,
            e.toString());
      }

      return primaryKeys;
    }

    @Override
    public List<String> getObjectValue(List<String> key) {
      String catalogName = key.get(0);
      String schemaName = key.get(1);
      String tableName = key.get(2);

      List<String> primaryKeys =
          getTablePrimaryKeys(catalogName, schemaName, tableName, MAX_RETRIES);

      return primaryKeys;
    }
  }
}
