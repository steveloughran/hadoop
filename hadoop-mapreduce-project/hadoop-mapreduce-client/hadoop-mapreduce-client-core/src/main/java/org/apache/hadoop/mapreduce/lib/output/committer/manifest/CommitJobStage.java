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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_JOB_COMMITTED_BYTES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_JOB_COMMITTED_FILES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;

/**
 * Commit the Job.
 * Arguments (save manifest, validate output)
 * Inputs: saveMarker: boolean, validateOutput: boolean
 * Outputs: SuccessData
 */
public class CommitJobStage extends
    AbstractJobCommitStage<
        Pair<Boolean, Boolean>,
        ManifestSuccessData> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CommitJobStage.class);

  public CommitJobStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_COMMIT, true);
  }

  @Override
  protected ManifestSuccessData executeStage(
      final Pair<Boolean, Boolean> arguments) throws IOException {

    boolean saveMarker = arguments.getLeft();
    boolean validate = arguments.getRight();

    // load the manifests
    Pair<LoadManifestsStage.SummaryInfo, List<TaskManifest>> pair
        = new LoadManifestsStage(getStageConfig()).apply(true);
    List<TaskManifest> manifests = pair.getRight();
    LoadManifestsStage.SummaryInfo summary = pair.getLeft();

    LOG.debug("Job Summary {}", summary);
    LOG.info("Committing job with file count: {}; total size {} bytes",
        summary.getFileCount(),
        byteCountToDisplaySize(summary.getTotalFileSize()));

    // add in the manifest statistics to our local IOStatistics for
    // reporting.
    IOStatisticsStore iostats = getIOStatistics();
    iostats.aggregate(summary.getIOStatistics());

    // update the counter of bytes committed and files.
    iostats.incrementCounter(
        OP_JOB_COMMITTED_FILES,
        summary.getFileCount());
    iostats.incrementCounter(
        OP_JOB_COMMITTED_BYTES,
        summary.getTotalFileSize());

    // prepare destination directories.
    List<Path> dirs = new PrepareDirectoriesStage(getStageConfig())
        .apply(manifests);

    // commit all the tasks.
    // The success data includes a snapshot of the IO Statistics
    // and hence all aggregate stats from the tasks.
    ManifestSuccessData jobSuccessData;
    jobSuccessData = new RenameFilesStage(getStageConfig()).apply(manifests);
    LOG.debug("_SUCCESS file summary {}", jobSuccessData.toJson());

    // save the _SUCCESS if the option is enabled.
    if (saveMarker) {
      Path succesPath = new SaveSuccessFileStage(getStageConfig())
          .apply(jobSuccessData);
      LOG.debug("Saving _SUCCESS file to {}", succesPath);
    }

    if (validate) {
      LOG.info("Validating output.");
      new ValidateRenamedFilesStage(getStageConfig())
          .apply(manifests);
    }

    return jobSuccessData;
  }

}
