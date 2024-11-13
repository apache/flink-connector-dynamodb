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
import java.util.List;

/** Upload Batch Executor. */
@Internal
public class RedshiftUploadBatchExecutor implements RedshiftExecutor {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedshiftUploadBatchExecutor.class);

    private final int maxRetries;

    private final String tableName;

    private final String[] fieldNames;

    private String copySql;

    private final RedshiftCopyRowConverter copyRowConverter;

    private final String tempS3Uri;

    private final String iamRoleArn;

    private transient List<String[]> csvData;

    private transient AmazonS3 s3Client;

    private transient RedshiftPreparedStatement statement;

    private transient RedshiftConnectionProvider connectionProvider;

    public RedshiftUploadBatchExecutor(
            String[] fieldNames, LogicalType[] fieldTypes, RedshiftOptions options) {
        this.tableName = options.getTableName();
        this.fieldNames = fieldNames;
        this.maxRetries = options.getMaxRetries();
        this.csvData = new ArrayList<>();
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
        this.copyRowConverter = new RedshiftCopyRowConverter(fieldTypes);

        this.tempS3Uri = RedshiftS3Util.getS3UriWithFileName(options.getTempS3Uri());
        this.iamRoleArn = options.getIamRoleArn();
    }

    @Override
    public void prepareStatement(RedshiftConnectionImpl connection) throws SQLException {
        copySql =
                RedshiftStatementFactory.getTableCopyStatement(
                        tableName, tempS3Uri, fieldNames, iamRoleArn);
        statement = (RedshiftPreparedStatement) connection.prepareStatement(copySql);
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
        csvData.add(copyRowConverter.toExternal(record));
    }

    @Override
    public void executeBatch() throws SQLException {
        LOG.info("Begin to COPY command.");
        try {
            RedshiftS3Util.s3OutputCsv(s3Client, tempS3Uri, csvData);
            attemptExecuteBatch(statement, maxRetries, false);
            RedshiftS3Util.s3DeleteObj(s3Client, tempS3Uri);
        } catch (Exception e) {
            throw new SQLException("Batch Copy failed!", e);
        }

        LOG.info("End to COPY command.");
    }

    @Override
    public void closeStatement() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException exception) {
                LOG.warn("Redshift batch statement could not be closed.", exception);
            } finally {
                statement = null;
            }
        }
    }

    @Override
    public String toString() {
        return "RedshiftUploadBatchExecutor{"
                + "copySql='"
                + copySql
                + '\''
                + ", maxRetries="
                + maxRetries
                + ", connectionProvider="
                + connectionProvider
                + '}';
    }
}
