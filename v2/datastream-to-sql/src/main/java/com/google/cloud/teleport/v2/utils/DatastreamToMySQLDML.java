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


import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO.DataSourceConfiguration;
import com.google.cloud.teleport.v2.datastream.values.DatastreamRow;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of Database Migration utilities to convert JSON data to DML. */
public class DatastreamToMySQLDML extends DatastreamToDML {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToMySQLDML.class);

  private DatastreamToMySQLDML(DataSourceConfiguration config) {
    super(config);
  }

  public static DatastreamToMySQLDML of(DataSourceConfiguration config) {
    return new DatastreamToMySQLDML(config);
  }

  @Override
  public String getDefaultQuoteCharacter() {
    return "`";
  }

  @Override
  public String getDeleteDmlStatement() {
    return "DELETE FROM {quoted_catalog_name}.{quoted_table_name} WHERE {primary_key_kv_sql};";
  }

  @Override
  public String getUpsertDmlStatement() {
    return "INSERT INTO {quoted_catalog_name}.{quoted_table_name} "
        + "({quoted_column_names}) VALUES ({column_value_sql}) "
        + "ON DUPLICATE KEY UPDATE {column_kv_sql};";
  }

  @Override
  public String getInsertDmlStatement() {
    return "INSERT INTO {quoted_catalog_name}.{quoted_table_name} "
        + "({quoted_column_names}) VALUES ({column_value_sql});";
  }

  @Override
  public String getTargetCatalogName(DatastreamRow row) {
    String schemaName = row.getSchemaName();
    return cleanSchemaName(schemaName);
  }

  @Override
  public String getTargetSchemaName(DatastreamRow row) {
    return "";
  }

  @Override
  public String getTargetTableName(DatastreamRow row) {
    String tableName = row.getTableName();
    return cleanTableName(tableName);
  }
  
  @Override
  public String cleanDataTypeValueSql(
      String columnValue, String columnName, Map<String, DatastreamToDML.ColumnInfo> tableSchema) {
    DatastreamToDML.ColumnInfo columnInfo = tableSchema.get(columnName);

    if (columnInfo == null) {
      if (!columnName.startsWith("_metadata_", 0)) {
        LOG.warn(
            "ColumnInfo not found in schema for column: {}. Returning original value: {}",
            columnName,
            columnValue);
      }
      return columnValue;
    }

    String dataType = columnInfo.getTypeName();
    String isNullable = columnInfo.getIsNullable();

    // Handle mandatory datetime types that are null or empty string
    if (dataType != null &&
        (dataType.toUpperCase().equals("DATETIME") || dataType.toUpperCase().equals("TIMESTAMP")) &&
        "NO".equalsIgnoreCase(isNullable)
        && (columnValue == null || columnValue.isEmpty() || "''".equals(columnValue) || "null".equalsIgnoreCase(columnValue))) {
      return "'0000-00-00 00:00:00'"; // Default value for mandatory null datetimes
    }

    // For any nullable column, if the value is null or an empty string, it should be treated as SQL NULL.
    // This check is placed after the mandatory datetime handling.
    if (!"NO".equalsIgnoreCase(isNullable)
        && (columnValue == null || columnValue.isEmpty() || "''".equals(columnValue) || "null".equalsIgnoreCase(columnValue))) {
      return getNullValueSql();
    }


    // If dataType is null at this point (e.g. columnInfo.getTypeName() was null after all),
    // returning columnValue is a safe default.
    if (dataType == null) {
        if (!columnName.startsWith("_metadata_", 0)) {
           LOG.warn(
               "Data type is null in ColumnInfo for column: {}. Returning original value: {}",
               columnName,
               columnValue);
        }
      return columnValue;
    }
    
    String upperDataType = dataType.toUpperCase();

    switch (upperDataType) {
      case "DATE":
      case "DATETIME":
      case "TIMESTAMP": // Timestamp specific handling from original code
        // Apply handling for DATETIME/DATE datatype as these require transformation
        // before inserting to MySQL.
        // The mandatory null check is done above. If it passes, it means the value is not null,
        // or it's nullable, or not a datetime type.
        return convertJsonToMysqlDatetime(columnValue, columnName, upperDataType);      
      case "TIME":
        // The generic null check at the top handles null values.
        return columnValue;
      case "BINARY":
        // Enhanced handling for BINARY types (from original code)
        String innerValue = columnValue;
        boolean isQuoted = columnValue.startsWith("'") && columnValue.endsWith("'") && columnValue.length() > 1;
        if (isQuoted) {
          innerValue = columnValue.substring(1, columnValue.length() - 1);
        }

        int n = -1;
        String upperDataTypeTrimmed = upperDataType.trim();
        if (upperDataTypeTrimmed.startsWith("BINARY(") && upperDataTypeTrimmed.endsWith(")")) {
          try {
            String nString = upperDataTypeTrimmed.substring("BINARY(".length(), upperDataTypeTrimmed.length() - 1);
            n = Integer.parseInt(nString);
          } catch (NumberFormatException e) {
            LOG.warn(
                "Error parsing length N from dataType '{}' for column: {}. Value: '{}'. Error: {}. Returning original value.",
                dataType, columnName, columnValue, e.getMessage());
            return columnValue;
          }
        }

        if (n == -1 && innerValue != null && !innerValue.isEmpty()) { // if N not specified in type, infer from value length if possible
            n = innerValue.length() / 2;
        }

        if (n == 16 && innerValue != null && innerValue.matches("^[0-9a-fA-F]{32}$")) {
          return "UNHEX('" + innerValue + "')";
        }

        if (innerValue != null && innerValue.toUpperCase().startsWith("X'") && innerValue.endsWith("'")) {
          String hexContent = innerValue.substring(2, innerValue.length() - 1);
          if (hexContent.matches("^[0-9a-fA-F]*$") && !hexContent.toUpperCase().startsWith("X'")) {
            return columnValue;
          }
        }

        if (n > 0 && innerValue != null && innerValue.matches("^[0-9a-fA-F]{" + (2 * n) + "}$")) {
          return "X'" + innerValue + "'";
        }

        LOG.warn(
            "Unexpected format for BINARY type column: {}, value: '{}', dataType: {}. Returning original value.",
            columnName, columnValue, dataType);
        return columnValue;

      case "TINYINT":
      case "SMALLINT":
      case "MEDIUMINT":
      case "INT":
      case "INTEGER":
      case "BIGINT":
      case "FLOAT":
      case "DOUBLE":
      case "DECIMAL":
      case "NUMERIC":
      case "BIT": // BIT type was missing, often treated as numeric/binary string
        // The generic null check at the top handles null values for these types.
        // For BIT, if it's in the form b'0101', it should be passed as is.
        // If it's a plain number for other numerics, pass as is.
        if (upperDataType.equals("BIT") && columnValue.toLowerCase().startsWith("b'")) {
            return columnValue;
        }
        // Otherwise, for numerics (including BIT values that are just numbers), return columnValue
        return columnValue;
      default:
        // For other types (like VARCHAR, TEXT, etc.), perform a null check.
        if (columnValue == null || "null".equalsIgnoreCase(columnValue)) {
          return getNullValueSql();
        }
        // Otherwise, return the value as is. Quoting will be handled by the caller.
        return columnValue;
    }
  }

  public String convertJsonToMysqlDatetime(String columnValue, String columnName, String columnType) {
    // MySQL DATETIME format 'YYYY-MM-DD HH:MM:SS[.fraction]'
    // Handle empty string literal or unquoted empty string from source
    if (columnValue.equals("''") || columnValue.equals("")) {
      return getNullValueSql();
    }
    // If it's already "NULL" (e.g. from a JSON null value), pass it through
    if (columnValue.equalsIgnoreCase("NULL")) {
      return columnValue;
    }

    // LOG.info("Datatype: {}",columnType);
    
    String innerValue = columnValue;
    boolean isQuoted = columnValue.startsWith("'") && columnValue.endsWith("'") && columnValue.length() > 1;
    if (isQuoted) {
        innerValue = columnValue.substring(1, columnValue.length() - 1);
    }
    
    if (innerValue.length() > 1) {

      // Check if it's an ISO 8601 format that needs conversion for DATETIME
      // Example: 2025-05-11T22:30:10.000000Z or with offset +01:00
      if (innerValue.contains("T")
          && (innerValue.endsWith("Z")
              || innerValue.matches(".*[+-]\\d{2}(:?\\d{2})?$"))) {
        try {
          switch (columnType) {
            case "DATETIME":
              // OffsetDateTime can parse various ISO 8601 formats, including those with 'Z'
              // or offsets
              OffsetDateTime odt = OffsetDateTime.parse(innerValue);
              // Convert to LocalDateTime at UTC, as DATETIME is naive but we want to preserve
              // the instant
              LocalDateTime localDateTimeInUTC = odt.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();

              DateTimeFormatter mysqlDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
              String formattedValue = localDateTimeInUTC.format(mysqlDateTimeFormatter);

              // Clean up trailing zeros from microseconds for aesthetics and to avoid
              // potential issues.
              if (formattedValue.endsWith(".000000")) {
                formattedValue = formattedValue.substring(0, formattedValue.length() - 7);
              } else { // remove other trailing zeros from microseconds part
                formattedValue = formattedValue.replaceAll("0*$", "");
                if (formattedValue.endsWith(".")) { // if it became "YYYY-MM-DD HH:MM:SS."
                  formattedValue = formattedValue.substring(0, formattedValue.length() - 1);
                }
              }
              if (isQuoted) {
                return "'" + cleanSql(formattedValue) + "'"; // Re-quote and ensure SQL safety
              } else { 
                return cleanSql(formattedValue); 
              }

            case "DATE":
              isQuoted = columnValue.startsWith("'") && columnValue.endsWith("'") && columnValue.length() > 1;
              OffsetDateTime odtdt = OffsetDateTime.parse(innerValue);
              // Convert to LocalDate at UTC to get the correct date part
              LocalDate localDateInUTC = odtdt.atZoneSameInstant(ZoneOffset.UTC).toLocalDate();
              String formattedDateValue = localDateInUTC.format(DateTimeFormatter.ISO_LOCAL_DATE); // YYYY-MM-DD
              if(isQuoted) {
                return "'" + cleanSql(formattedDateValue) + "'";
              } else {
                return cleanSql(formattedDateValue);
              }

          }
          
        } catch (DateTimeParseException e) {
          LOG.warn(
              "DateTimeParseException for {} column: {}. Value: '{}'. Error: {}. Returning original value.",
              columnType,
              columnName,
              innerValue,
              e.getMessage());
          // Fallback to returning the original columnValue if parsing fails
        }
      }
    }
    // If not empty, not "NULL", not a string literal, or not the problematic ISO
    // format, return as is.
    return columnValue;
  }

}