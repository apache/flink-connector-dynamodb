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

package org.apache.flink.connector.redshift.connection;

import org.apache.flink.connector.redshift.options.RedshiftOptions;

import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Redshift Connection Provider. */
public class RedshiftConnectionProvider implements Serializable {
    private static final long serialVersionUID = 1L;

    static final Logger LOG = LoggerFactory.getLogger(RedshiftConnectionProvider.class);

    private static final String REDSHIFT_DRIVER_NAME = "com.amazon.redshift.Driver";

    private transient RedshiftConnectionImpl connection;

    private final RedshiftOptions options;

    public RedshiftConnectionProvider(RedshiftOptions options) {
        this.options = options;
    }

    public synchronized RedshiftConnectionImpl getConnection() throws SQLException {
        if (connection == null) {
            connection =
                    createConnection(
                            options.getHostname(), options.getPort(), options.getDatabaseName());
        }
        return connection;
    }

    private RedshiftConnectionImpl createConnection(String hostname, int port, String dbName)
            throws SQLException {
        // String url = parseUrl(urls);

        RedshiftConnectionImpl conn;
        String url = "jdbc:redshift://" + hostname + ":" + port + "/" + dbName;
        LOG.info("connection to {}", url);

        try {
            Class.forName(REDSHIFT_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }

        if (options.getUsername().isPresent()) {
            conn =
                    (RedshiftConnectionImpl)
                            DriverManager.getConnection(
                                    url,
                                    options.getUsername().orElse(null),
                                    options.getPassword().orElse(null));
        } else {
            conn = (RedshiftConnectionImpl) DriverManager.getConnection(url);
        }

        return conn;
    }

    public void closeConnection() throws SQLException {
        if (this.connection != null) {
            this.connection.close();
        }
    }

    public RedshiftConnectionImpl getOrCreateConnection() throws SQLException {
        if (connection == null) {
            connection =
                    createConnection(
                            options.getHostname(), options.getPort(), options.getDatabaseName());
        }
        return connection;
    }
}
