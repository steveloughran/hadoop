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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
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

import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/** Full integration test of an MR job. */
public abstract class AbstractITCommitMRJob extends AbstractS3ATestBase {

  private static final int TEST_FILE_COUNT = 8;
  private static MiniDFSTestCluster hdfs;
  private static MiniMRYarnCluster yarn = null;
  private static JobConf conf = null;
  private boolean uniqueFilenames = false;

  protected static FileSystem getDFS() {
    return hdfs.getClusterFS();
  }

  @BeforeClass
  public static void setupClusters() throws IOException {
    // the HDFS and YARN clusters share the same configuration, so
    // the HDFS cluster binding is implicitly propagated to YARN
    JobConf c = new JobConf();
    hdfs = new MiniDFSTestCluster();
    hdfs.init(c);
    hdfs.start();
    conf = c;
    yarn = new MiniMRYarnCluster("ITCommitMRJob", 2);
    yarn.init(c);
    yarn.start();
  }

  @SuppressWarnings("ThrowableNotThrown")
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
    protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      // force in Log4J logging
      org.apache.log4j.BasicConfigurator.configure();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  /**
   * Get the classname of the factory for this committer.
   * @return the classname to set in the job setup
   */
  protected abstract String committerFactoryClassname();

  /**
   * The name of the committer as returned by
   * {@link AbstractS3GuardCommitter#getName()}.
   */
  protected abstract String committerName();

  @Test
  public void testMRJob() throws Exception {
    S3AFileSystem fs = getFileSystem();
    // final dest is in S3A
    Path outputPath = path("testMRJob");
    StorageStatisticsTracker tracker = new StorageStatisticsTracker(fs);

    String commitUUID = UUID.randomUUID().toString();
    String suffix = uniqueFilenames ? ("-" + commitUUID) : "";
    int numFiles = TEST_FILE_COUNT;
    Set<String> expectedPaths = Sets.newHashSet();
    Set<String> expectedKeys = Sets.newHashSet();
    for (int i = 0; i < numFiles; i += 1) {
      File file = temp.newFile(String.valueOf(i) + ".text");
      try (FileOutputStream out = new FileOutputStream(file)) {
        out.write(("file " + i).getBytes(StandardCharsets.UTF_8));
      }
      String filename = "part-m-0000" + i +
          suffix;
      Path path = new Path(outputPath, filename);
      expectedPaths.add(path.toString());
      expectedKeys.add(fs.pathToKey(path));
    }

    Job mrJob = Job.getInstance(yarn.getConfig(), "test-committer-job");
    JobConf jobConf = (JobConf)mrJob.getConfiguration();
    jobConf.setBoolean(FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES,
        uniqueFilenames);

    jobConf.set(PathOutputCommitterFactory.OUTPUTCOMMITTER_FACTORY_CLASS,
        committerFactoryClassname());

    mrJob.setOutputFormatClass(LoggingTextOutputFormat.class);
    FileOutputFormat.setOutputPath(mrJob, outputPath);

    File mockResultsFile = temp.newFile("committer.bin");
    mockResultsFile.delete();
    String committerPath = "file:" + mockResultsFile;
    jobConf.set("mock-results-file", committerPath);
    jobConf.set(CommitConstants.FS_S3A_COMMITTER_STAGING_UUID, commitUUID);

    mrJob.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(mrJob, new Path(temp.getRoot().toURI()));

    mrJob.setMapperClass(MapClass.class);
    mrJob.setNumReduceTasks(0);

    // an attempt to set up log4j properly, which clearly doesn't work
    URL log4j = getClass().getClassLoader().getResource("log4j.properties");
    if (log4j != null && log4j.getProtocol().equals("file")) {
      Path log4jPath = new Path(log4j.toURI());
      LOG.debug("Using log4j path {}", log4jPath);
      mrJob.addFileToClassPath(log4jPath);
      String sysprops = String.format("-Xmx256m -Dlog4j.configuration=%s",
          log4j);
      jobConf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, sysprops);
      jobConf.set("yarn.app.mapreduce.am.command-opts", sysprops);
    }

    applyCustomConfigOptions(jobConf);
    // fail fast if anything goes wrong
    mrJob.setMaxMapAttempts(1);

    mrJob.submit();
    boolean succeeded = mrJob.waitForCompletion(true);
    assertTrue("MR job failed", succeeded);

    assertIsDirectory(outputPath);
    Set<String> actualFiles = Sets.newHashSet();
    FileStatus[] results = fs.listStatus(outputPath, TEMP_FILE_FILTER);
    assertTrue("No files in output directory", results.length != 0);
    LOG.info("Found {} files", results.length);
    for (FileStatus result : results) {
      LOG.debug("result: {}", result);
      actualFiles.add(result.getPath().toString());
    }

    assertEquals("Should commit the correct file paths",
        expectedPaths, actualFiles);
    // now load in the success data marker: this guarantees that a s3guard
    // committer was used
    SuccessData successData = SuccessData.load(fs,
        new Path(outputPath, SUCCESS_FILE_NAME));
    String commitDetails = successData.toString();
    LOG.info("Committer from " + committerFactoryClassname() + "\n{}",
        commitDetails);
    assertEquals("Wrong committer in " + commitDetails,
        committerName(), successData.getCommitter());
    List<String> successFiles = successData.getFilenames();
    assertTrue("No filenames in " + commitDetails,
        !successFiles.isEmpty());
    Set<String> summaryKeys = Sets.newHashSet();
    summaryKeys.addAll(successFiles);
    assertEquals("Summary keyset doesn't list the the expected paths "
            + commitDetails, expectedKeys, summaryKeys);
    assertPathDoesNotExist("temporary dir",
        new Path(outputPath, CommitConstants.PENDING_DIR_NAME));
    Map<String, Long> metrics = successData.getMetrics();
    List<String> list = new ArrayList<>(metrics.keySet());
    Collections.sort(list);
    StringBuilder sb = new StringBuilder(list.size() * 32);
    for (String k : list) {
      sb.append(k).append(" = ").append(metrics.get(k)).append("\n");
    }
    LOG.info("Committer statistics: \n{}", sb);
    customPostExecutionValidation(outputPath, successData);
  }

  /**
   * Override point to let implementations tune the MR Job conf.
   * @param c configuration
   */
  protected void applyCustomConfigOptions(Configuration c) {

  }

  /**
   * Override point for any committer specific validation operations;
   * called after the base assertions have all passed.
   * @param destPath destination of work
   * @param successData loaded success data
   * @throws Exception failure
   */
  protected void customPostExecutionValidation(Path destPath,
      SuccessData successData)
      throws Exception {

  }

}
