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

package org.apache.flink.connector.redshift.executor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.redshift.connection.RedshiftConnectionProvider;
import org.apache.flink.connector.redshift.converter.RedshiftRowConverter;
import org.apache.flink.connector.redshift.options.RedshiftOptions;
import org.apache.flink.connector.redshift.statement.RedshiftStatementFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.jdbc.RedshiftPreparedStatement;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Redshift Executor. */
@Internal
public interface RedshiftExecutor extends Serializable {
    Logger LOG = LoggerFactory.getLogger(RedshiftUpsertExecutor.class);

    void prepareStatement(RedshiftConnectionImpl connection) throws SQLException;

    void prepareStatement(RedshiftConnectionProvider connectionProvider) throws SQLException;

    void setRuntimeContext(RuntimeContext context);

    void addToBatch(RowData rowData) throws SQLException;

    void executeBatch() throws SQLException;

    void closeStatement();

    default void attemptExecuteBatch(RedshiftPreparedStatement stmt, int maxRetries)
            throws SQLException {
        attemptExecuteBatch(stmt, maxRetries, true);
    }

    default void attemptExecuteBatch(
            RedshiftPreparedStatement stmt, int maxRetries, Boolean batchMode) throws SQLException {
        for (int i = 0; i <= maxRetries; i++) {
            try {
                if (batchMode) {
                    stmt.executeBatch();
                } else {
                    stmt.execute();
                }

                return;
            } catch (Exception exception) {
                LOG.error("Redshift executeBatch error, retry times = {}", i, exception);
                if (i >= maxRetries) {
                    throw new SQLException(
                            String.format(
                                    "Attempt to execute batch failed, exhausted retry times = %d",
                                    maxRetries),
                            exception);
                }
                try {
                    Thread.sleep(1000L * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new SQLException(
                            "Unable to flush; interrupted while doing another attempt", ex);
                }
            }
        }
    }

    static RedshiftExecutor createRedshiftExecutor(
            String[] fieldNames,
            String[] keyFields,
            LogicalType[] fieldTypes,
            RedshiftOptions options) {
        if (keyFields.length > 0) {
            switch (options.getSinkMode()) {
                case COPY:
                    LOG.info("Creating COPY Mode UPSERT Executor.");
                    return createUploadUpsertExecutor(fieldNames, keyFields, fieldTypes, options);
                case JDBC:
                    LOG.info("Creating JDBC Mode UPSERT Executor.");
                    return createUpsertExecutor(fieldNames, keyFields, fieldTypes, options);
                default:
                    throw new IllegalArgumentException(
                            "Sink Mode "
                                    + options.getSinkMode()
                                    + " not recognised. "
                                    + "Flink Connector Redshift Supports only JDBC / COPY mode.");
            }

        } else {
            switch (options.getSinkMode()) {
                case COPY:
                    LOG.info("Creating COPY Mode batch Executor.");
                    return createUploadBatchExecutor(fieldNames, fieldTypes, options);
                case JDBC:
                    LOG.info("Creating JDBC Mode batch Executor.");
                    return createBatchExecutor(fieldNames, fieldTypes, options);
                default:
                    throw new IllegalArgumentException(
                            "Sink Mode "
                                    + options.getSinkMode()
                                    + " not recognised. "
                                    + "Flink Connector Redshift Supports only JDBC / COPY mode.");
            }
        }
    }

    /**
     *
     * @param fieldNames field names.
     * @param fieldTypes dataTypes for the field.
     * @param options Redshift options.
     * @return { @RedshiftUploadBatchExecutor } executor.
     */
    static RedshiftUploadBatchExecutor createUploadBatchExecutor(
            final String[] fieldNames,
            final LogicalType[] fieldTypes,
            final RedshiftOptions options) {
        return new RedshiftUploadBatchExecutor(fieldNames, fieldTypes, options);
    }

    static RedshiftUploadUpsertExecutor createUploadUpsertExecutor(
            String[] fieldNames,
            String[] keyFields,
            LogicalType[] fieldTypes,
            RedshiftOptions options) {
        int[] delFields =
                Arrays.stream(keyFields)
                        .mapToInt(pk -> ArrayUtils.indexOf(fieldNames, pk))
                        .toArray();
        LogicalType[] delTypes =
                Arrays.stream(delFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);

        return new RedshiftUploadUpsertExecutor(
                fieldNames,
                keyFields,
                fieldTypes,
                new RedshiftRowConverter(RowType.of(delTypes)),
                createExtractor(fieldTypes, delFields),
                options);
    }

    static RedshiftBatchExecutor createBatchExecutor(
            String[] fieldNames, LogicalType[] fieldTypes, RedshiftOptions options) {
        String insertSql =
                RedshiftStatementFactory.getInsertIntoStatement(options.getTableName(), fieldNames);
        RedshiftRowConverter converter = new RedshiftRowConverter(RowType.of(fieldTypes));
        return new RedshiftBatchExecutor(insertSql, converter, options);
    }

    static RedshiftUpsertExecutor createUpsertExecutor(
            String[] fieldNames,
            String[] keyFields,
            LogicalType[] fieldTypes,
            RedshiftOptions options) {
        String tableName = options.getTableName();
        String insertSql = RedshiftStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        String updateSql =
                RedshiftStatementFactory.getUpdateStatement(tableName, fieldNames, keyFields);
        String deleteSql = RedshiftStatementFactory.getDeleteStatement(tableName, keyFields);

        // Re-sort the order of fields to fit the sql statement.
        int[] delFields =
                Arrays.stream(keyFields)
                        .mapToInt(pk -> ArrayUtils.indexOf(fieldNames, pk))
                        .toArray();
        int[] updatableFields =
                IntStream.range(0, fieldNames.length)
                        .filter(idx -> !ArrayUtils.contains(keyFields, fieldNames[idx]))
                        .toArray();
        int[] updFields = ArrayUtils.addAll(updatableFields, delFields);

        LogicalType[] delTypes =
                Arrays.stream(delFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);
        LogicalType[] updTypes =
                Arrays.stream(updFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);

        return new RedshiftUpsertExecutor(
                insertSql,
                updateSql,
                deleteSql,
                new RedshiftRowConverter(RowType.of(fieldTypes)),
                new RedshiftRowConverter(RowType.of(updTypes)),
                new RedshiftRowConverter(RowType.of(delTypes)),
                createExtractor(fieldTypes, updFields),
                createExtractor(fieldTypes, delFields),
                options);
    }

    static Function<RowData, RowData> createExtractor(LogicalType[] logicalTypes, int[] fields) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[fields[i]], fields[i]);
        }

        return row -> {
            GenericRowData rowData = new GenericRowData(row.getRowKind(), fieldGetters.length);
            for (int i = 0; i < fieldGetters.length; i++) {
                rowData.setField(i, fieldGetters[i].getFieldOrNull(row));
            }
            return rowData;
        };
    }
}
