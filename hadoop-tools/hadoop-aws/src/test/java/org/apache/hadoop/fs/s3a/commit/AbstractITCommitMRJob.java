/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.commit;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.StorageStatisticsTracker;
import org.apache.hadoop.fs.s3a.commit.files.SuccessData;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.service.ServiceOperations;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.DIRECTORY_COMMITTER_FACTORY;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.SUCCESS_FILE_NAME;

/** Full integration test of an MR job. */
public abstract class AbstractITCommitMRJob extends AbstractS3ATestBase {

  private static MiniDFSTestCluster hdfs;
  private static MiniMRYarnCluster yarn = null;
  private static JobConf conf = null;
  private boolean uniqueFilenames = false;

  protected static FileSystem getDFS() {
    return hdfs.getClusterFS();
  }

  @BeforeClass
  public static void setupClusters() throws IOException {
    JobConf c = new JobConf();
    hdfs = new MiniDFSTestCluster();
    hdfs.init(c);
    hdfs.start();
    conf = c;
    yarn = new MiniMRYarnCluster(
        "TestStagingMRJobr", 2);
    yarn.init(c);
    yarn.start();
  }

  @AfterClass
  public static void teardownClusters() throws IOException {
    conf = null;
    ServiceOperations.stopQuietly(yarn);
    ServiceOperations.stopQuietly(hdfs);
    hdfs = null;
    yarn = null;
  }

  public static JobConf getConf() {
    return conf;
  }

  public static MiniDFSCluster getHdfs() {
    return hdfs.getCluster();
  }

  public static FileSystem getLocalFS() {
    return hdfs.getLocalFS();
  }

  /** Test Mapper. */
  public static class MapClass
      extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  protected abstract String getCommitterFactoryClassname();

  @Test
  public void testMRJob() throws Exception {
    FileSystem s3 = getFileSystem();
    // final dest is in S3A
    Path outputPath = path("testMRJob");
    StorageStatisticsTracker tracker = new StorageStatisticsTracker(s3);

    String commitUUID = UUID.randomUUID().toString();
    String suffix = uniqueFilenames ? ("-" + commitUUID) : "";
    int numFiles = 3;
    Set<String> expectedFiles = Sets.newHashSet();
    for (int i = 0; i < numFiles; i += 1) {
      File file = temp.newFile(String.valueOf(i) + ".text");
      try (FileOutputStream out = new FileOutputStream(file)) {
        out.write(("file " + i).getBytes(StandardCharsets.UTF_8));
      }
      String filename = "part-m-0000" + i +
          suffix;
      expectedFiles.add(new Path(
          outputPath, filename).toString());
    }

    Job mrJob = Job.getInstance(yarn.getConfig(), "test-committer-job");
    Configuration jobConf = mrJob.getConfiguration();
    jobConf.setBoolean(FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES, uniqueFilenames);

    jobConf.set(PathOutputCommitterFactory.OUTPUTCOMMITTER_FACTORY_CLASS,
        getCommitterFactoryClassname());

    mrJob.setOutputFormatClass(LoggingTextOutputFormat.class);
    FileOutputFormat.setOutputPath(mrJob, outputPath);

    File mockResultsFile = temp.newFile("committer.bin");
    mockResultsFile.delete();
    String committerPath = "file:" + mockResultsFile;
    jobConf.set("mock-results-file", committerPath);
    jobConf.set(CommitConstants.FS_S3A_COMMITTER_STAGING_UUID, commitUUID);

    mrJob.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(mrJob,
        new Path(temp.getRoot().toURI()));

    mrJob.setMapperClass(MapClass.class);
    mrJob.setNumReduceTasks(0);

    describe("Submitting Job");
    mrJob.submit();
    boolean succeeded = mrJob.waitForCompletion(true);
    assertTrue("MR job failed", succeeded);

    assertPathExists("Output directory", outputPath);
    Set<String> actualFiles = Sets.newHashSet();
    FileStatus[] results = s3.listStatus(outputPath, TEMP_FILE_FILTER);
    LOG.info("Found {} files", results.length);
    for (FileStatus result : results) {
      LOG.debug("result: {}", result);
      actualFiles.add(result.getPath().toString());
    }

    assertEquals("Should commit the correct file paths",
        expectedFiles, actualFiles);
    // now load in the success data marker: this guarantees that a s3guard
    // committer was used
    SuccessData.load(s3, new Path(outputPath, SUCCESS_FILE_NAME));
  }

}
