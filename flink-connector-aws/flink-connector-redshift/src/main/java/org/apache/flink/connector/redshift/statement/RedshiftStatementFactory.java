/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.redshift.statement;

import org.apache.flink.annotation.Internal;

import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Statement Factory. */
@Internal
public class RedshiftStatementFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    public static String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(RedshiftStatementFactory::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO "
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ") VALUES ("
                + placeholders
                + ")";
    }

    public static String getUpdateStatement(
            String tableName, String[] fieldNames, String[] keyFields) {
        String setClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !ArrayUtils.contains(keyFields, f))
                        .map(f -> quoteIdentifier(f) + "= ?")
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(keyFields)
                        .map(f -> quoteIdentifier(f) + "= ?")
                        .collect(Collectors.joining(" AND "));

        return "UPDATE "
                + quoteIdentifier(tableName)
                + " set "
                + setClause
                + " WHERE "
                + conditionClause;
    }

    public static String getDeleteStatement(String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.<String>stream(conditionFields)
                        .map(f -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
    }

    public static String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> quoteIdentifier(f) + "=?")
                        .collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
    }

    public static String getUpsertStatement(
            String tableName,
            String stageTableName,
            String[] fieldNames,
            String[] conditionFields) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(RedshiftStatementFactory::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String matchCondition =
                Arrays.stream(conditionFields)
                        .map(
                                f ->
                                        quoteIdentifier(tableName)
                                                + "."
                                                + quoteIdentifier(f)
                                                + "="
                                                + quoteIdentifier("stage")
                                                + "."
                                                + quoteIdentifier(f))
                        .collect(Collectors.joining(" AND "));
        String setClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !ArrayUtils.contains(conditionFields, f))
                        .map(
                                f ->
                                        quoteIdentifier(f)
                                                + "="
                                                + quoteIdentifier("stage")
                                                + "."
                                                + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        String insertValue =
                Arrays.stream(fieldNames)
                        .map(f -> quoteIdentifier("stage") + "." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        return "MERGE INTO "
                + quoteIdentifier(tableName)
                + " USING "
                + quoteIdentifier(stageTableName)
                + " stage on "
                + matchCondition
                + " WHEN MATCHED THEN UPDATE SET "
                + setClause
                + " WHEN NOT MATCHED THEN INSERT ("
                + columns
                + ") VALUES ("
                + insertValue
                + ")";
    }

    public static String getDropTableStatement(String tableName) {
        return "DROP TABLE IF EXISTS " + quoteIdentifier(tableName);
    }

    public static String getCreateTempTableAsStatement(String tableName, String tempTableName) {
        return "CREATE TEMP TABLE IF NOT EXISTS "
                + quoteIdentifier(tempTableName)
                + "(LIKE "
                + quoteIdentifier(tableName)
                + ")";
    }

    public static String getTableCopyStatement(
            String tableName, String s3Uri, String[] fieldNames, String iamRoleArn) {
        final String columns = Arrays.<String>stream(fieldNames).collect(Collectors.joining(", "));
        return "COPY "
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " FROM "
                + "'"
                + s3Uri
                + "'"
                + " iam_role "
                + "'"
                + iamRoleArn
                + "' "
                + "FORMAT AS CSV";
    }

    public static String getInsertFromStageTable(
            String tableName, String tempTableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(RedshiftStatementFactory::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        return "INSERT INTO "
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ") SELECT "
                + columns
                + " FROM "
                + quoteIdentifier(tempTableName);
    }

    public static String getDeleteFromStageTable(
            String tableName, String tempTableName, String[] conditionFields) {
        String matchCondition =
                Arrays.stream(conditionFields)
                        .map(
                                f ->
                                        quoteIdentifier(tableName)
                                                + "."
                                                + quoteIdentifier(f)
                                                + "="
                                                + quoteIdentifier(tempTableName)
                                                + "."
                                                + quoteIdentifier(f))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM "
                + quoteIdentifier(tableName)
                + " USING "
                + quoteIdentifier(tempTableName)
                + " WHERE "
                + matchCondition;
    }

    public static String getTruncateTable(String tableName) {
        return "TRUNCATE " + quoteIdentifier(tableName);
    }

    public static String quoteIdentifier(String identifier) {
        return identifier;
    }
}
