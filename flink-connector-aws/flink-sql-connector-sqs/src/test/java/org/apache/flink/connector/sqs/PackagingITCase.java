package org.apache.flink.connector.sqs;

import org.apache.flink.packaging.PackagingTestUtils;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.test.resources.ResourceTestUtils;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;

/** Packaging test for the SQS SQL connector. */
public class PackagingITCase {

    @Test
    void testPackaging() throws Exception {
        final Path jar = ResourceTestUtils.getResource(".*/flink-sql-connector-sqs[^/]*\\.jar");

        PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                jar,
                Arrays.asList(
                        "org/apache/flink/",
                        "org/apache/commons/",
                        "META-INF/",
                        "mozilla/",
                        "mime.types",
                        "VersionInfo.java"));
        PackagingTestUtils.assertJarContainsServiceEntry(jar, Factory.class);
    }
}
