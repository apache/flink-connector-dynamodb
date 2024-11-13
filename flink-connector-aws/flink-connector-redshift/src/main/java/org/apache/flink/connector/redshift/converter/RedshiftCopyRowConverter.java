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

package org.apache.flink.connector.redshift.converter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

/** Copy Converter Utils. */
public class RedshiftCopyRowConverter implements Serializable {

    private final LogicalType[] fieldTypes;

    public RedshiftCopyRowConverter(LogicalType[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public String[] toExternal(RowData rowData) {
        ArrayList<String> csvLine = new ArrayList<>();
        for (int index = 0; index < rowData.getArity(); index++) {
            LogicalType type = fieldTypes[index];
            String val = "";
            switch (type.getTypeRoot()) {
                case BOOLEAN:
                    val = Boolean.toString(rowData.getBoolean(index));
                    break;
                case FLOAT:
                    val = String.valueOf(rowData.getFloat(index));
                    break;
                case DOUBLE:
                    val = String.valueOf(rowData.getDouble(index));
                    break;
                case INTERVAL_YEAR_MONTH:
                case INTEGER:
                    val = String.valueOf(rowData.getInt(index));
                    break;
                case INTERVAL_DAY_TIME:
                case BIGINT:
                    val = String.valueOf(rowData.getLong(index));
                    break;
                case TINYINT:
                case SMALLINT:
                case CHAR:
                case VARCHAR:
                    val = rowData.getString(index).toString();
                    break;
                case BINARY:
                case VARBINARY:
                case DATE:
                    val =
                            LocalDate.ofEpochDay(rowData.getInt(index))
                                    .format(DateTimeFormatter.ISO_DATE);
                    break;
                case TIME_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    final int timestampPrecision = ((TimestampType) type).getPrecision();
                    final DateTimeFormatter timeFormaterr =
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                    val =
                            rowData.getTimestamp(index, timestampPrecision)
                                    .toLocalDateTime()
                                    .format(timeFormaterr);
                    break;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:

                case DECIMAL:
                    final int decimalPrecision = ((DecimalType) type).getPrecision();
                    final int decimalScale = ((DecimalType) type).getScale();
                    val = String.valueOf(rowData.getDecimal(index, decimalPrecision, decimalScale));
                    break;
                case ARRAY:

                case MAP:
                case MULTISET:
                case ROW:
                case RAW:
                default:
                    throw new UnsupportedOperationException("Unsupported type:" + type);
            }
            csvLine.add(val);
        }
        return csvLine.toArray(new String[0]);
    }
}
