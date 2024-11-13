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
import org.apache.flink.connector.redshift.converter.RedshiftCopyRowConverter;
import org.apache.flink.connector.redshift.converter.RedshiftRowConverter;
import org.apache.flink.connector.redshift.options.RedshiftOptions;
import org.apache.flink.connector.redshift.statement.RedshiftStatementFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.jdbc.RedshiftPreparedStatement;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/** Redshift Upload Upsert Executor. */
@Internal
public class RedshiftUploadUpsertExecutor implements RedshiftExecutor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedshiftUploadUpsertExecutor.class);

    private final int maxRetries;

    private final String tableName;

    private final String stageTableName;

    private final String[] fieldNames;

    private final String[] keyFields;

    private final String tempS3Uri;

    private final String iamRoleArn;

    private String copyInsertSql;

    private String updateTrxSql;

    private String deleteSql;

    private final RedshiftRowConverter deleteConverter;

    private final Function<RowData, RowData> deleteExtractor;

    private transient RedshiftPreparedStatement insertStatement;

    private transient RedshiftPreparedStatement deleteStatement;

    private transient RedshiftPreparedStatement updateTrxStatement;

    private transient List<String[]> csvInsertData;

    private transient List<String[]> csvUpdateData;

    private final RedshiftCopyRowConverter copyRowConverter;

    private transient AmazonS3 s3Client;

    private RedshiftConnectionProvider connectionProvider;

    public RedshiftUploadUpsertExecutor(
            String[] fieldNames,
            String[] keyFields,
            LogicalType[] fieldTypes,
            RedshiftRowConverter deleteConverter,
            Function<RowData, RowData> deleteExtractor,
            RedshiftOptions options) {

        this.maxRetries = options.getMaxRetries();
        this.fieldNames = fieldNames;
        this.keyFields = keyFields;
        this.deleteConverter = deleteConverter;
        this.deleteExtractor = deleteExtractor;
        this.csvInsertData = new ArrayList<>();
        this.csvUpdateData = new ArrayList<>();

        this.tableName = options.getTableName();
        this.iamRoleArn = options.getIamRoleArn();
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
        this.copyRowConverter = new RedshiftCopyRowConverter(fieldTypes);
        this.stageTableName = options.getDatabaseName() + "/" + tableName + "_stage";
        this.tempS3Uri = RedshiftS3Util.getS3UriWithFileName(options.getTempS3Uri());
    }

    @Override
    public void prepareStatement(RedshiftConnectionImpl connection) throws SQLException {
        final String createTableSql =
                RedshiftStatementFactory.getCreateTempTableAsStatement(tableName, stageTableName);
        final String insertSql =
                RedshiftStatementFactory.getInsertFromStageTable(
                        tableName, stageTableName, fieldNames);
        deleteSql = RedshiftStatementFactory.getDeleteStatement(tableName, keyFields);
        final String deleteFromStageSql =
                RedshiftStatementFactory.getDeleteFromStageTable(
                        tableName, stageTableName, keyFields);
        final String truncateSql = RedshiftStatementFactory.getTruncateTable(stageTableName);
        copyInsertSql =
                RedshiftStatementFactory.getTableCopyStatement(
                        tableName, tempS3Uri, fieldNames, iamRoleArn);
        final String copyUpdateSql =
                RedshiftStatementFactory.getTableCopyStatement(
                        stageTableName, tempS3Uri, fieldNames, iamRoleArn);

        updateTrxSql =
                "BEGIN;"
                        + createTableSql
                        + "; "
                        + truncateSql
                        + ";"
                        + copyUpdateSql
                        + "; "
                        + deleteFromStageSql
                        + "; "
                        + insertSql
                        + "; "
                        + "END;";

        insertStatement = (RedshiftPreparedStatement) connection.prepareStatement(copyInsertSql);
        updateTrxStatement = (RedshiftPreparedStatement) connection.prepareStatement(updateTrxSql);
        deleteStatement = (RedshiftPreparedStatement) connection.prepareStatement(deleteSql);
    }

    @Override
    public void prepareStatement(RedshiftConnectionProvider connectionProvider)
            throws SQLException {
        this.connectionProvider = connectionProvider;
        prepareStatement(connectionProvider.getOrCreateConnection());
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {}

    @Override
    public void addToBatch(RowData record) throws SQLException {
        switch (record.getRowKind()) {
            case INSERT:
                csvInsertData.add(copyRowConverter.toExternal(record));
                break;
            case UPDATE_AFTER:
                csvUpdateData.add(copyRowConverter.toExternal(record));
                break;
            case DELETE:
                deleteConverter.toExternal(deleteExtractor.apply(record), deleteStatement);
                deleteStatement.addBatch();
                break;
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                                record.getRowKind()));
        }
    }

    @Override
    public void executeBatch() throws SQLException {

        LOG.info("Begin to COPY command.");
        try {
            if (!csvInsertData.isEmpty()) {
                RedshiftS3Util.s3OutputCsv(s3Client, tempS3Uri, csvInsertData);
                attemptExecuteBatch(insertStatement, maxRetries, false);
            }
            if (!csvUpdateData.isEmpty()) {
                RedshiftS3Util.s3OutputCsv(s3Client, tempS3Uri, csvUpdateData);
                attemptExecuteBatch(updateTrxStatement, maxRetries, false);
            }
            if (deleteStatement != null) {
                attemptExecuteBatch(deleteStatement, maxRetries);
            }

            RedshiftS3Util.s3DeleteObj(s3Client, tempS3Uri);
            csvInsertData = new ArrayList<>();
            csvUpdateData = new ArrayList<>();
        } catch (Exception e) {
            throw new SQLException("Redshift COPY mode execute error!", e);
        }

        LOG.info("End to COPY command.");
    }

    @Override
    public void closeStatement() {
        for (RedshiftPreparedStatement redshiftStatement :
                Arrays.asList(insertStatement, updateTrxStatement, deleteStatement)) {
            if (redshiftStatement != null) {
                try {
                    redshiftStatement.close();
                } catch (SQLException exception) {
                    LOG.warn("Redshift statement could not be closed.", exception);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "RedshiftUploadUpsertExecutor{"
                + "copyInsertSql='"
                + copyInsertSql
                + '\''
                + "updateTrxSql='"
                + updateTrxSql
                + '\''
                + ", deleteSql='"
                + deleteSql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
