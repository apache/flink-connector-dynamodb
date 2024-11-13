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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** S3 Utils for Redshift for COPY Mode. */
@Internal
public class RedshiftS3Util implements Serializable {

    private static final long serialVersionUID = 1L;

    public static void s3OutputCsv(AmazonS3 s3Client, String s3Uri, List<String[]> csvData)
            throws IOException {
        try {
            String bucket = getS3Parts(s3Uri)[0];
            String key = getS3Parts(s3Uri)[1];

            S3OutputStream tempOut = new S3OutputStream(s3Client, bucket, key);
            OutputStreamWriter tempOutWriter = new OutputStreamWriter(tempOut);
            CSVPrinter csvPrinter = new CSVPrinter(tempOutWriter, CSVFormat.DEFAULT);
            csvPrinter.printRecords(csvData);
            tempOutWriter.close();
            tempOut.close();
            csvPrinter.close();
        } catch (Exception e) {
            throw new IOException(String.format("The S3 URI {} is not correct! ", s3Uri), e);
        }
    }

    public static void s3DeleteObj(AmazonS3 s3Client, String s3Uri) throws IOException {
        try {
            String bucket = getS3Parts(s3Uri)[0];
            String key = getS3Parts(s3Uri)[1];
            s3Client.deleteObject(bucket, key);
        } catch (Exception e) {
            throw new IOException(String.format("The S3 object {} delete error!", s3Uri), e);
        }
    }

    public static String[] getS3Parts(String s3Uri) throws IllegalArgumentException {
        AmazonS3URI amazonS3Uri = new AmazonS3URI(s3Uri);
        String key = amazonS3Uri.getKey();
        if ((key.charAt(key.length() - 1)) == '/') {
            key = removeLastCharOptional(key);
        }
        return new String[] {amazonS3Uri.getBucket(), key};
    }

    public static String removeLastCharOptional(String s) {
        return Optional.ofNullable(s)
                .filter(str -> str.length() != 0)
                .map(str -> str.substring(0, str.length() - 1))
                .orElse(s);
    }

    public static String getS3UriWithFileName(String s3Uri) {
        String tempFileName = UUID.randomUUID().toString();
        String tempS3Uri = s3Uri;
        if (tempS3Uri.charAt(tempS3Uri.length() - 1) == '/') {
            tempS3Uri = removeLastCharOptional(tempS3Uri);
        }
        return tempS3Uri + "/" + tempFileName;
    }
}
