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

package org.apache.flink.connector.redshift.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.redshift.mode.SinkMode;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

/** Options. */
@Internal
public class RedshiftOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String hostname;

    private final int port;

    private final String username;

    private final String password;

    private final String databaseName;

    private final String tableName;

    private final int batchSize;

    private final Duration flushInterval;

    private final int maxRetries;

    private final SinkMode sinkMode;

    private final String tempFileS3Uri;

    private final String iamRoleArn;

    private RedshiftOptions(
            String hostname,
            int port,
            String username,
            String password,
            String databaseName,
            String tableName,
            int batchSize,
            Duration flushInterval,
            int maxRetires,
            SinkMode sinkMode,
            String tempFileS3Uri,
            String iamRoleArn) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetires;
        this.sinkMode = sinkMode;
        this.tempFileS3Uri = tempFileS3Uri;
        this.iamRoleArn = iamRoleArn;
    }

    public String getHostname() {
        return this.hostname;
    }

    public int getPort() {
        return this.port;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(this.username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(this.password);
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public Duration getFlushInterval() {
        return this.flushInterval;
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }

    public SinkMode getSinkMode() {
        return this.sinkMode;
    }

    public String getTempS3Uri() {
        return this.tempFileS3Uri;
    }

    public String getIamRoleArn() {
        return this.iamRoleArn;
    }

    /** Builder Class. */
    public static class Builder {
        private String hostname;

        private int port;

        private String username;

        private String password;

        private String databaseName;

        private String tableName;

        private int batchSize;

        private Duration flushInterval;

        private int maxRetries;

        private SinkMode sinkMode;

        private String tempS3Uri;

        private String iamRoleArn;

        public Builder withHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder withSinkMode(SinkMode sinkMode) {
            this.sinkMode = sinkMode;
            return this;
        }

        public Builder withTempS3Uri(String tempS3Uri) {
            this.tempS3Uri = tempS3Uri;
            return this;
        }

        public Builder withIamRoleArn(String iamRoleArn) {
            this.iamRoleArn = iamRoleArn;
            return this;
        }

        public RedshiftOptions build() {
            Preconditions.checkNotNull(this.hostname, "No hostname supplied.");
            Preconditions.checkNotNull(this.port, "No port supplied.");
            Preconditions.checkNotNull(this.databaseName, "No databaseName supplied.");
            Preconditions.checkNotNull(this.tableName, "No tableName supplied.");
            Preconditions.checkNotNull(this.sinkMode, "No copyMode supplied.");
            return new RedshiftOptions(
                    this.hostname,
                    this.port,
                    this.username,
                    this.password,
                    this.databaseName,
                    this.tableName,
                    this.batchSize,
                    this.flushInterval,
                    this.maxRetries,
                    this.sinkMode,
                    this.tempS3Uri,
                    this.iamRoleArn);
        }
    }
}
