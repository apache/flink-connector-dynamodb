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

package org.apache.flink.connector.redshift.format;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.redshift.connection.RedshiftConnectionProvider;
import org.apache.flink.connector.redshift.executor.RedshiftExecutor;
import org.apache.flink.connector.redshift.options.RedshiftOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

/** Redshift Output Format. */
@Internal
public class RedshiftOutputFormat extends AbstractRedshiftOutputFormat {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedshiftOutputFormat.class);

    private final RedshiftConnectionProvider connectionProvider;

    private final String[] fieldNames;

    private final String[] keyFields;

    private final LogicalType[] fieldTypes;

    private final RedshiftOptions options;

    private transient RedshiftExecutor executor;

    private transient int batchCount = 0;

    protected RedshiftOutputFormat(
            RedshiftConnectionProvider connectionProvider,
            String[] fieldNames,
            String[] keyFields,
            LogicalType[] fieldTypes,
            RedshiftOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.keyFields = Preconditions.checkNotNull(keyFields);
        this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            executor =
                    RedshiftExecutor.createRedshiftExecutor(
                            fieldNames, keyFields, fieldTypes, options);

            executor.prepareStatement(connectionProvider);
            executor.setRuntimeContext(getRuntimeContext());

            LOG.info("Executor: " + executor);

            long flushIntervalMillis = options.getFlushInterval().toMillis();
            scheduledFlush(flushIntervalMillis, "redshift-batch-output-format");
        } catch (Exception exception) {
            throw new IOException("Unable to establish connection with Redshift.", exception);
        }
    }

    @Override
    public synchronized void writeRecord(RowData record) throws IOException {
        checkFlushException();

        try {
            executor.addToBatch(record);
            batchCount++;
            if (batchCount >= options.getBatchSize()) {
                flush();
            }
        } catch (SQLException exception) {
            throw new IOException("Writing record to Redshift statement failed.", exception);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (batchCount > 0) {
            checkBeforeFlush(executor);
            batchCount = 0;
        }
    }

    @Override
    public synchronized void closeOutputFormat() {
        try {
            executor.closeStatement();
            connectionProvider.closeConnection();
        } catch (SQLException exception) {
            LOG.error("Close Redshift statement failed.", exception);
        }
    }
}
