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

package org.apache.flink.connector.redshift.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.redshift.format.AbstractRedshiftOutputFormat;
import org.apache.flink.connector.redshift.options.RedshiftOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

/** AWS Redshift Dynamic Table Sink . */
@PublicEvolving
public class RedshiftDynamicTableSink implements DynamicTableSink {
    private final String[] primaryKeys;

    private final String[] fieldNames;

    private final DataType[] fieldDataTypes;

    private final RedshiftOptions options;

    public RedshiftDynamicTableSink(
            RedshiftOptions options,
            String[] primaryKeys,
            String[] fieldNames,
            DataType[] fieldDataTypes) {

        this.primaryKeys = primaryKeys;
        this.fieldNames = fieldNames;
        this.fieldDataTypes = fieldDataTypes;
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState(
                (ChangelogMode.insertOnly().equals(requestedMode) || primaryKeys.length > 0),
                "Declare primary key for UPSERT operation.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        AbstractRedshiftOutputFormat outputFormat =
                new AbstractRedshiftOutputFormat.Builder()
                        .withConnectionProperties(options)
                        .withFieldNames(fieldNames)
                        .withFieldTypes(fieldDataTypes)
                        .withPrimaryKey(primaryKeys)
                        .build();
        return OutputFormatProvider.of(outputFormat);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedshiftDynamicTableSink(
                this.options, this.primaryKeys, this.fieldNames, this.fieldDataTypes);
    }

    @Override
    public String asSummaryString() {
        return "Amazon Redshift Sink";
    }
}
