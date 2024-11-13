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
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redshift.connection.RedshiftConnectionProvider;
import org.apache.flink.connector.redshift.executor.RedshiftExecutor;
import org.apache.flink.connector.redshift.options.RedshiftOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Abstract Redshift Output format. */
@Internal
public abstract class AbstractRedshiftOutputFormat extends RichOutputFormat<RowData>
        implements Flushable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRedshiftOutputFormat.class);

    protected transient volatile boolean closed = false;

    protected transient ScheduledExecutorService scheduler;

    protected transient ScheduledFuture<?> scheduledFuture;

    protected transient volatile Exception flushException;

    public AbstractRedshiftOutputFormat() {}

    @Override
    public void configure(Configuration parameters) {}

    public void scheduledFlush(long intervalMillis, String executorName) {
        Preconditions.checkArgument(intervalMillis > 0, "flush interval must be greater than 0");
        scheduler = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory(executorName));
        scheduledFuture =
                scheduler.scheduleWithFixedDelay(
                        () -> {
                            synchronized (this) {
                                if (!closed) {
                                    try {
                                        flush();
                                    } catch (Exception e) {
                                        flushException = e;
                                    }
                                }
                            }
                        },
                        intervalMillis,
                        intervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    public void checkBeforeFlush(final RedshiftExecutor executor) throws IOException {
        checkFlushException();
        try {
            executor.executeBatch();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;

            try {
                flush();
            } catch (Exception exception) {
                LOG.warn("Flushing records to Redshift failed.", exception);
            }

            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            closeOutputFormat();
            checkFlushException();
        }
    }

    protected abstract void closeOutputFormat();

    protected void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Flush exception found.", flushException);
        }
    }

    /** Builder class. */
    public static class Builder {

        private static final Logger LOG =
                LoggerFactory.getLogger(AbstractRedshiftOutputFormat.Builder.class);

        private DataType[] fieldTypes;

        private LogicalType[] logicalTypes;

        private RedshiftOptions connectionProperties;

        private String[] fieldNames;

        private String[] primaryKeys;

        public Builder() {}

        public Builder withConnectionProperties(RedshiftOptions connectionProperties) {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public Builder withFieldTypes(DataType[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            this.logicalTypes =
                    Arrays.stream(fieldTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            return this;
        }

        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withPrimaryKey(String[] primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public AbstractRedshiftOutputFormat build() {
            Preconditions.checkNotNull(fieldNames);
            Preconditions.checkNotNull(fieldTypes);
            Preconditions.checkNotNull(primaryKeys);
            if (primaryKeys.length > 0) {
                LOG.warn("If primary key is specified, connector will be in UPSERT mode.");
                LOG.warn(
                        "The data will be updated / deleted by the primary key, you will have significant performance loss.");
            } else {
                LOG.warn("No primary key is specified, connector will be INSERT only mode.");
            }

            RedshiftConnectionProvider connectionProvider =
                    new RedshiftConnectionProvider(connectionProperties);

            return new RedshiftOutputFormat(
                    connectionProvider,
                    fieldNames,
                    primaryKeys,
                    logicalTypes,
                    connectionProperties);
        }
    }
}
