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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory.COMMITTER_FACTORY_CLASS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter.ManifestCommitterConfig.createCloseableTaskSubmitter;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_ID_SOURCE_MAPREDUCE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_COMMITTER_FACTORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_VALIDATE_OUTPUT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.createIOStatisticsStore;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterTestSupport.validateSuccessFile;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

/**
 * Tests which work with manifest committers.
 * This is a filesystem contract bound to the local filesystem;
 * subclasses may change the FS to test against other stores.
 */
public abstract class AbstractManifestCommitterTest
    extends AbstractFSContractTestBase {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractManifestCommitterTest.class);

  /**
   * Some Job and task IDs.
   */
  protected static final ManifestCommitterTestSupport.JobAndTaskIDsForTests
      TASK_IDS =
      new ManifestCommitterTestSupport.JobAndTaskIDsForTests(2, 2);

  public static final int JOB1 = 1;

  public static final int TASK0 = 0;

  public static final int TASK1 = 1;

  /**
   * Task attempt 0 index.
   */
  public static final int TA0 = 0;

  /**
   * Task attempt 1 index.
   */
  public static final int TA1 = 1;

  /**
   * Depth of dir tree to generate.
   */
  public static final int DEPTH = 3;

  /**
   * Width of dir tree at every level.
   */
  public static final int WIDTH = 2;

  /**
   * How many files to create in the leaf directories.
   */
  public static final int FILES_PER_DIRECTORY = 4;

  /**
   * Pool size.
   */
  public static final int POOL_SIZE = 32;

  /**
   * FileSystem statistics are collected across every test case.
   */
  protected static final IOStatisticsSnapshot FILESYSTEM_IOSTATS =
      snapshotIOStatistics();

  /**
   * Counter for creating files. Ensures that across all test suites,
   * duplicate filenames are never created. Helps assign blame.
   */
  private static final AtomicLong CREATE_FILE_COUNTER = new AtomicLong();

  /**
   * Submitter for tasks; may be null.
   */
  private CloseableTaskSubmitter submitter;

  /**
   * Stage statistics. Created in test setup, and in
   * teardown updates {@link #FILESYSTEM_IOSTATS}.
   */
  private IOStatisticsStore stageStatistics;

  /**
   * Prefer to use these to interact with the FS to
   * ensure more implicit coverage.
   */
  private StoreOperations storeOperations;

  /**
   * Progress counter used in all stage configs.
   */
  private final ProgressCounter progressCounter = new ProgressCounter();

  /**
   * Get the contract configuration.
   * @return the config used to create the FS.
   */
  protected Configuration getConfiguration() {
    return getContract().getConf();
  }

  /**
   * Store operations to interact with..
   * @return store operations.
   */
  protected StoreOperations getStoreOperations() {
    return storeOperations;
  }

  /**
   * Set store operations.
   * @param storeOperations new value
   */
  protected void setStoreOperations(final StoreOperations storeOperations) {
    this.storeOperations = storeOperations;
  }


  /**
   * Describe a test in the logs.
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        getMethodName(),
        String.format(text, args));
  }

  /**
   * Local FS unless overridden.
   * @param conf configuration
   * @return the FS contract.
   */
  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    return new LocalFSContract(conf);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    setStageStatistics(createIOStatisticsStore().build());
    setSubmitter(createCloseableTaskSubmitter(POOL_SIZE, TASK_IDS.getJobId()));
    storeOperations = new StoreOperationsThroughFileSystem(getFileSystem());
  }


  @Override
  public void teardown() throws Exception {
    Thread.currentThread().setName("teardown");
    storeOperations = null;

    super.teardown();
    IOUtils.closeStream(getSubmitter());
    FILESYSTEM_IOSTATS.aggregate(retrieveIOStatistics(getFileSystem()));
    FILESYSTEM_IOSTATS.aggregate(getStageStatistics());
  }

  /**
   * Get the task submitter.
   * @return submitter or null
   */
  protected CloseableTaskSubmitter getSubmitter() {
    return submitter;
  }

  /**
   * Set the task submitter.
   * @param submitter new value.
   */
  protected void setSubmitter(CloseableTaskSubmitter submitter) {
    this.submitter = submitter;
  }

  /**
   * @return IOStatistics for stage.
   */
  protected IOStatisticsStore getStageStatistics() {
    return stageStatistics;
  }

  /**
   * Set the statistics.
   * @param stageStatistics statistics.
   */
  protected void setStageStatistics(IOStatisticsStore stageStatistics) {
    this.stageStatistics = stageStatistics;
  }


  protected ProgressCounter getProgressCounter() {
    return progressCounter;
  }

  /**
   * Dump the filesystem statistics after the class.
   */
  @AfterClass
  public static void dumpFileSystemIOStatistics() {
    LOG.info("Aggregate FileSystem Statistics {}",
        ioStatisticsToPrettyString(FILESYSTEM_IOSTATS));
  }

  /**
   * Create a directory tree through an executor.
   * dirs created = width^depth;
   * file count = width^depth * files
   * @param base base dir
   * @param prefix prefix for filenames.
   * @param submit submitter.
   * @param depth depth of dirs
   * @param width width of dirs
   * @param files files to add in each base dir.
   * @return the list of paths
   * @throws IOException failure.
   */
  public List<Path> createFiles(Path base,
      String prefix,
      ExecutorService submit,
      int depth,
      int width,
      int files) throws IOException {

    try (DurationInfo ignored = new DurationInfo(LOG, true,
        "Creating Files %s (%d, %d, %d) under %s",
        prefix, depth, width, files, base)) {

      assertPathExists("Task attempt dir", base);

      // create the files in the thread pool.
      List<Future<Path>> futures = createFiles(
          new ArrayList<>(),
          base, prefix, submit, depth, width, files);
      List<Path> result = new ArrayList<>();

      // now wait for the creations to finish.
      for (Future<Path> f : futures) {
        result.add(awaitFuture(f));
      }
      return result;
    }
  }

  /**
   * Create the files; done in a treewalk and building up
   * a list of futures to wait for. The list is
   * build up incrementally rather than through some merging of
   * lists created down the tree.
   * @param prefix prefix for filenames.
   * @param futures list of futures to build up.
   * @param base base dir
   * @param submit submitter.
   * @param depth depth of dirs
   * @param width width of dirs
   * @param files files to add in each base dir.
   * @return the list of futures
   */
  private List<Future<Path>> createFiles(
      List<Future<Path>> futures,
      Path base,
      String prefix,
      ExecutorService submit,
      int depth,
      int width,
      int files) {

    if (depth > 0) {
      // still creating directories
      for (int i = 0; i < width; i++) {
        Path child = new Path(base,
            String.format("child-%02d-%02d", depth, i));
        createFiles(futures, child, prefix, submit, depth - 1, width, files);
      }
    } else {
      // time to create files
      for (int i = 0; i < files; i++) {
        Path file = new Path(base,
            String.format("%s-%04d", prefix,
                CREATE_FILE_COUNTER.incrementAndGet()));
        Future<Path> f = submit.submit(() -> {
          touch(getFileSystem(), file);
          return file;
        });
        futures.add(f);
      }
    }
    return futures;

  }

  /**
   * Convert the manifest list to a map by task attempt ID.
   * @param list manifests
   * @return a map, indexed by task attempt ID.
   */
  protected Map<String, TaskManifest> toMap(List<TaskManifest> list) {
    return list.stream()
        .collect(Collectors.toMap(TaskManifest::getTaskAttemptID, x -> x));
  }

  /**
   * Verify the manifest files match the list of paths.
   * @param manifest manifest to audit
   * @param files list of files.
   */
  protected void verifyManifestFilesMatch(final TaskManifest manifest,
      final List<Path> files) {
    // get the list of source paths
    Set<Path> filesToRename = manifest.getFilesToCommit()
        .stream()
        .map(FileOrDirEntry::getSourcePath)
        .collect(Collectors.toSet());
    // which must match that of all the files created
    Assertions.assertThat(filesToRename)
        .containsExactlyInAnyOrderElementsOf(files);
  }

  /**
   * Verify that a task manifest has a given attempt ID.
   * @param manifest manifest, may be null.
   * @param attemptId expected attempt ID
   * @return the manifest, guaranteed to be non-null and of task attempt.
   */
  protected TaskManifest verifyManifestTaskAttemptID(
      final TaskManifest manifest,
      final String attemptId) {
    Assertions.assertThat(manifest)
        .describedAs("Manifest of task %s", attemptId)
        .isNotNull();
    Assertions.assertThat(manifest.getTaskAttemptID())
        .describedAs("Task Attempt ID of manifest %s", manifest)
        .isEqualTo(attemptId);
    return manifest;
  }

  /**
   * Assert that a path must exist; return the path.
   * @param message text for error message.
   * @param path path to validate.
   * @return the path
   * @throws IOException IO Failure
   */
  Path pathMustExist(final String message,
      final Path path) throws IOException {
    assertPathExists(message, path);
    return path;
  }

  /**
   * Assert that a path must exist; return the path.
   * It must also equal the expected value.
   * @param message text for error message.
   * @param expected expected path.
   * @param path path to validate.
   * @return the path
   * @throws IOException IO Failure
   */
  Path verifyPath(final String message,
      final Path expected,
      final Path path) throws IOException {
    Assertions.assertThat(path)
        .describedAs(message)
        .isEqualTo(expected);
    return pathMustExist(message, path);
  }

  /**
   * Verify that the specified dir has the {@code _SUCCESS} marker
   * and that it can be loaded.
   * The contents will be logged and returned.
   * @param dir directory to scan
   * @param jobId job ID, only verified if non-empty
   * @return the loaded success data
   * @throws IOException IO Failure
   */
  protected ManifestSuccessData verifySuccessMarker(Path dir, String jobId)
      throws IOException {
    return validateSuccessFile(dir, getFileSystem(), "query", 0, jobId);
  }

  /**
   * Read a UTF-8 file.
   * @param path path to read
   * @return string value
   * @throws IOException IO failure
   */
  protected String readFile(Path path) throws IOException {
    return ContractTestUtils.readUTF8(getFileSystem(), path, -1);
  }

  /**
   * Modify a (job) config to switch to the manifest committer;
   * output validation is also enabled.
   * @param conf config to patch.
   */
  protected void enableManifestCommitter(final Configuration conf) {
    conf.set(COMMITTER_FACTORY_CLASS, MANIFEST_COMMITTER_FACTORY);
    // always create a job marker
    conf.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
    // and validate the output, for extra rigorousness
    conf.setBoolean(OPT_VALIDATE_OUTPUT, true);
  }

  /**
   * Create the stage config for a job but don't finalize it.
   * Uses {@link #TASK_IDS} for job/task ID.
   * @param jobAttemptNumber job attempt number
   * @param outputPath path where the final output goes
   * @return the config
   */
  protected StageConfig createStageConfigForJob(
      final int jobAttemptNumber,
      final Path outputPath) {
    return createStageConfig(jobAttemptNumber, -1, 0, outputPath);
  }

  /**
   * Create the stage config for job or task but don't finalize it.
   * Uses {@link #TASK_IDS} for job/task ID.
   * @param jobAttemptNumber job attempt number
   * @param taskIndex task attempt index; -1 for job attempt only.
   * @param taskAttemptNumber task attempt number
   * @param outputPath path where the final output goes
   * @return the config
   */
  protected StageConfig createStageConfig(
      final int jobAttemptNumber,
      final int taskIndex,
      final int taskAttemptNumber,
      final Path outputPath) {
    ManifestCommitterSupport.AttemptDirectories attemptDirs =
        new ManifestCommitterSupport.AttemptDirectories(outputPath,
            TASK_IDS.getJobId(), jobAttemptNumber);
    StageConfig config = new StageConfig();
    config
        .withOperations(getStoreOperations())
        .withJobId(TASK_IDS.getJobId())
        .withJobIdSource(JOB_ID_SOURCE_MAPREDUCE)
        .withJobAttemptNumber(jobAttemptNumber)
        .withJobDirectories(attemptDirs)
        .withIOProcessors(getSubmitter())
        .withIOStatistics(getStageStatistics())
        .withProgressable(getProgressCounter());

    // if there's a task attempt ID set, set up its details
    if (taskIndex >= 0) {
      String taskAttempt = TASK_IDS.getTaskAttempt(taskIndex, taskAttemptNumber);
      config
          .withTaskAttemptId(taskAttempt)
          .withTaskId(TASK_IDS.getTaskIdType(taskIndex).toString())
          .withTaskAttemptDir(
              attemptDirs.getTaskAttemptPath(taskAttempt));
    }
    return config;
  }

  /**
   * Counter.
   */
  protected static final class ProgressCounter implements Progressable {

    private final AtomicLong counter = new AtomicLong();

    /**
     * Increment the counter.
     */
    @Override
    public void progress() {
      counter.incrementAndGet();
    }

    /**
     * Get the counter value.
     * @return the current value.
     */
    public long value() {
      return counter.get();
    }

    /**
     * Reset the counter.
     */
    public void reset() {
      counter.set(0);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "ProgressCounter{");
      sb.append("counter=").append(counter.get());
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Get the progress counter of a stage.
   * @param stageConfig stage
   * @return its progress counter.
   */
  ProgressCounter progressOf(StageConfig stageConfig) {
    return (ProgressCounter) stageConfig.getProgressable();
  }
}
