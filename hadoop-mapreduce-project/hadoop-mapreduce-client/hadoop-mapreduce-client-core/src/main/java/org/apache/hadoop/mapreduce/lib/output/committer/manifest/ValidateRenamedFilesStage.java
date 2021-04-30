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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_VALIDATE_OUTPUT;
import static org.apache.hadoop.thirdparty.com.google.common.collect.Iterables.concat;

/**
 * This stage validates all files by scanning the manifests
 * and verifying every file in every manifest is of the given size.
 * Returns a list of all files committed.
 *
 * Its cost is one getFileStatus() call (parallelized) per file.
 */
public class ValidateRenamedFilesStage extends
    AbstractJobCommitStage<List<TaskManifest>, List<FileOrDirEntry>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      ValidateRenamedFilesStage.class);

  /**
   * Set this to halt all workers.
   */
  private final AtomicBoolean halt = new AtomicBoolean();

  /**
   * List of all files committed.
   */
  private List<FileOrDirEntry> filesCommitted = new ArrayList<>();

  public ValidateRenamedFilesStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_VALIDATE_OUTPUT, true);
  }

  @Override
  protected List<FileOrDirEntry> executeStage(
      final List<TaskManifest> taskManifests)
      throws IOException {

    // set the list of files to be as big as the number of tasks.
    filesCommitted = new ArrayList<>(taskManifests.size());

    // validate all the files.

    final Iterable<FileOrDirEntry> filesToCommit = concat(taskManifests.stream()
        .map(TaskManifest::getFilesToCommit)
        .collect(Collectors.toList()));

    TaskPool.foreach(filesToCommit)
        .executeWith(getIOProcessors())
        .stopOnFailure()
        .run(this::validateOneFile);

    return filesCommitted;
  }

  private void validateOneFile(FileOrDirEntry entry) throws IOException {
    if (halt.get()) {
      // told to stop
      return;
    }
    // report progress back
    progress();
    // look validate the file.
    // raising an FNFE if the file isn't there.
    FileStatus st = getFileStatus(entry.getDestPath());

    // it must be a file
    if (!st.isFile()) {
      throw new PathIOException(st.getPath().toString(),
          "Expected a file, found " + st);
    }
    // of the expected length
    if (st.getLen() != entry.getSize()) {
      throw new PathIOException(st.getPath().toString(),
          String.format("Expected a file of length %s"
                  + " but found a file of length %s",
              entry.getSize(),
              st.getLen()));
    }
    synchronized (this) {
      filesCommitted.add(entry);
    }
  }

}
