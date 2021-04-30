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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.createTaskManifest;

/**
 * Test loading manifests from a store.
 * By not creating files we can simulate a large job just by
 * creating the manifests.
 * The SaveTaskManifestStage stage is used for the save operation;
 * this does a save + rename.
 * For better test performance against a remote store, a thread
 * pool is used to save the manifests in parallel.
 *
 * The various private field are set up in
 * {@link #testSaveThenLoadManyManifests()};
 * they are also accessed in {@link #buildOneManifest(Integer)},
 * which is why fields are used.
 * when synchronized access is needed; synchronize on (this) rather
 * than individual fields.
 */
public class TestLoadManifestsStage extends AbstractManifestCommitterTest {

  private String[] dirNames;

  private final int taskAttemptCount = numberOfTaskAttempts();

  /**
   * How many task attempts to make.
   * @return a number > 0.
   */
  protected int numberOfTaskAttempts() {
    return 200;
  }

  private final int filesPerTaskAttempt = 10;

  private List<String> taskAttemptIds = new ArrayList<>(taskAttemptCount);
  ;

  private List<String> taskIds = new ArrayList<>(taskAttemptCount);

  private StageConfig jobStageConfig;

  private Path destDir;

  private final AtomicLong totalSize = new AtomicLong();

  @Override
  public void setup() throws Exception {
    super.setup();
    Assertions.assertThat(taskAttemptCount)
        .describedAs("Task attempt count")
        .isGreaterThan(0);
    dirNames = new String[filesPerTaskAttempt];
    for (int i = 0; i < filesPerTaskAttempt; i++) {
      dirNames[i] = String.format("dir-%03d", i);
    }
  }

  /**
   * build a large number of manifests, but without the real files
   * and directories.
   * Save the manifests under the job attempt dir, then load
   * them via the {@link LoadManifestsStage}.
   * The directory preparation process is then executed after this.
   * Because we know each task attempt creates the same number of directories,
   * they will all be merged and so only a limited number of output dirs created.
   *
   */
  @Test
  public void testSaveThenLoadManyManifests() throws Throwable {

    describe("Creating many manifests with fake file/dir entries,"
        + " load them and prepare the output dirs.");

    LOG.info("Number of task attempts: {}, files per task attempt {}",
        taskAttemptCount, filesPerTaskAttempt);

    // destination directory.
    destDir = methodPath();

    jobStageConfig = createStageConfigForJob(JOB1, destDir);

    // set up the job.
    new SetupJobStage(jobStageConfig).apply(false);

    LOG.info("Creating manifest files for {}", taskAttemptCount);

    try (DurationInfo di = new DurationInfo(LOG, true, "create manifests")) {

      // build a list of the task IDs.
      // it's really hard to create a list of Integers; the java8
      // IntStream etc doesn't quite fit as they do their best
      // keep things unboxed, trying to map(Integer::valueOf) doesn't help.
      List<Integer> taskIdList = new ArrayList<>(taskAttemptCount);
      for (int t = 0; t < taskAttemptCount; t++) {
        taskIdList.add(t);
      }

      // then submit their creation/save to the pool.
      TaskPool.foreach(taskIdList)
          .executeWith(getSubmitter())
          .stopOnFailure()
          .run(this::buildOneManifest);
    }

    LOG.info("Loading in the manifests");

    // Load in the manifests
    LoadManifestsStage stage = new LoadManifestsStage(
        jobStageConfig);

    Pair<LoadManifestsStage.SummaryInfo, List<TaskManifest>> result
        = stage.apply(true);
    LoadManifestsStage.SummaryInfo summary = result.getLeft();
    List<TaskManifest> loadedManifests = result.getRight();

    Assertions.assertThat(summary.getManifestCount())
        .describedAs("Manifest count of  %s", summary)
        .isEqualTo(taskAttemptCount);
    Assertions.assertThat(summary.getFileCount())
        .describedAs("File count of  %s", summary)
        .isEqualTo(taskAttemptCount * filesPerTaskAttempt);
    Assertions.assertThat(summary.getTotalFileSize())
        .describedAs("File Size of  %s", summary)
        .isEqualTo(totalSize.get());

    // now that manifest list.
    List<String> manifestTaskIds = loadedManifests.stream()
        .map(TaskManifest::getTaskID)
        .collect(Collectors.toList());
    Assertions.assertThat(taskIds)
        .describedAs("Task IDs of all tasks")
        .containsExactlyInAnyOrderElementsOf(manifestTaskIds);

    // now let's see about aggregating a large set of directories
    List<Path> createdDirectories = new PrepareDirectoriesStage(jobStageConfig)
        .apply(loadedManifests);

    // but after the merge process, only one per generated file output dir exists
    Assertions.assertThat(createdDirectories)
        .describedAs("Directories created")
        .hasSize(filesPerTaskAttempt);

    // and skipping the rename stage (which is going to fail),
    // go straight to cleanup
    new CleanupJobStage(jobStageConfig).apply(
        new CleanupJobStage.Options(true, true, false, false));
  }

  /**
   * Build one manifest.
   * @param task task index
   * @throws IOException failure
   */
  private void buildOneManifest(final Integer task) throws IOException {
    String tid = String.format("task_%03d", task);
    String taskAttemptId = String.format("%s_%02d",
        tid, task ^ 0x03);
    synchronized (this) {
      taskIds.add(tid);
      taskAttemptIds.add(taskAttemptId);
    }
    // for each task, a job config is created then patched with the task info
    Path jobAttemptTaskSubDir = jobStageConfig.getJobAttemptTaskSubDir();
    StageConfig taskStageConfig = createStageConfigForJob(JOB1, destDir)
        .withTaskId(tid)
        .withTaskAttemptId(taskAttemptId)
        .withTaskAttemptDir(new Path(jobAttemptTaskSubDir, taskAttemptId));

    LOG.info("Generating manifest for {}", taskAttemptId);

    // task setup: create dest dir.
    // this isn't actually needed, but it helps generate a realistic
    // workload for the parallelized job cleanup.
    new SetupTaskStage(taskStageConfig).apply("task " + taskAttemptId);

    final TaskManifest manifest = createTaskManifest(taskStageConfig);

    Path taDir = taskStageConfig.getTaskAttemptDir();
    long size = task * 1000_0000;

    // for each task, 10 dirs, one file per dir.
    for (int i = 0; i < filesPerTaskAttempt; i++) {
      Path in = new Path(taDir, "dir" + i);
      Path out = new Path(destDir, "dir" + i);
      manifest.addDirectory(new FileOrDirEntry(in, out, 0));
      String name = taskStageConfig.getTaskAttemptId() + ".csv";
      Path src = new Path(in, name);
      Path dest = new Path(out, name);
      long fileSize = size + i * 1000;
      manifest.addFileToCommit(new FileOrDirEntry(src, dest,
          fileSize));
      totalSize.addAndGet(fileSize);
    }

    // save the manifest for this stage.
    new SaveTaskManifestStage(taskStageConfig).apply(manifest);
  }

}
