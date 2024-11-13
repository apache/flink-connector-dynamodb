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
import org.apache.flink.table.data.RowData;

import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.jdbc.RedshiftPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

/** Upsert Executor. */
@Internal
public class RedshiftUpsertExecutor implements RedshiftExecutor {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedshiftUpsertExecutor.class);

    private final String insertSql;

    private final String updateSql;

    private final String deleteSql;

    private final RedshiftRowConverter insertConverter;

    private final RedshiftRowConverter updateConverter;

    private final RedshiftRowConverter deleteConverter;

    private final Function<RowData, RowData> updateExtractor;

    private final Function<RowData, RowData> deleteExtractor;

    private final int maxRetries;

    private transient RedshiftPreparedStatement insertStatement;

    private transient RedshiftPreparedStatement updateStatement;

    private transient RedshiftPreparedStatement deleteStatement;

    private transient RedshiftConnectionProvider connectionProvider;

    public RedshiftUpsertExecutor(
            String insertSql,
            String updateSql,
            String deleteSql,
            RedshiftRowConverter insertConverter,
            RedshiftRowConverter updateConverter,
            RedshiftRowConverter deleteConverter,
            Function<RowData, RowData> updateExtractor,
            Function<RowData, RowData> deleteExtractor,
            RedshiftOptions options) {
        this.insertSql = insertSql;
        this.updateSql = updateSql;
        this.deleteSql = deleteSql;
        this.insertConverter = insertConverter;
        this.updateConverter = updateConverter;
        this.deleteConverter = deleteConverter;
        this.updateExtractor = updateExtractor;
        this.deleteExtractor = deleteExtractor;
        this.maxRetries = options.getMaxRetries();
    }

    @Override
    public void prepareStatement(RedshiftConnectionImpl connection) throws SQLException {
        this.insertStatement =
                (RedshiftPreparedStatement) connection.prepareStatement(this.insertSql);
        this.updateStatement =
                (RedshiftPreparedStatement) connection.prepareStatement(this.updateSql);
        this.deleteStatement =
                (RedshiftPreparedStatement) connection.prepareStatement(this.deleteSql);
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
        // TODO: how to handle the ROW sequece?
        switch (record.getRowKind()) {
            case INSERT:
                insertConverter.toExternal(record, insertStatement);
                insertStatement.addBatch();
                break;
            case UPDATE_AFTER:
                updateConverter.toExternal(updateExtractor.apply(record), updateStatement);
                updateStatement.addBatch();
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
        for (RedshiftPreparedStatement redshiftStatement :
                Arrays.asList(insertStatement, updateStatement, deleteStatement)) {
            if (redshiftStatement != null) {
                attemptExecuteBatch(redshiftStatement, maxRetries);
            }
        }
    }

    @Override
    public void closeStatement() {
        for (RedshiftPreparedStatement redshiftStatement :
                Arrays.asList(insertStatement, updateStatement, deleteStatement)) {
            if (redshiftStatement != null) {
                try {
                    redshiftStatement.close();
                } catch (SQLException exception) {
                    LOG.warn("Redshift upsert statement could not be closed.", exception);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "RedshiftUpsertExecutor{"
                + "insertSql='"
                + insertSql
                + '\''
                + ", updateSql='"
                + updateSql
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
