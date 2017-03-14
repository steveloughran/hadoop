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

package org.apache.hadoop.fs.s3a.commit.staging;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.s3a.commit.DurationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class PartitionedStagingCommitter extends StagingS3GuardCommitter {
  protected static final String TABLE_ROOT = "table_root";

  private static final Logger LOG = LoggerFactory.getLogger(
      PartitionedStagingCommitter.class);

  public PartitionedStagingCommitter(Path outputPath, JobContext context)
      throws IOException {
    super(outputPath, context);
  }

  public PartitionedStagingCommitter(Path outputPath,
                                      TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "PartitionedStagingCommitter{");
    sb.append(super.toString());
    sb.append('}');
    return sb.toString();
  }

  @Override
  protected List<FileStatus> getTaskOutput(TaskAttemptContext context)
      throws IOException {
    PathFilter filter = Paths.HiddenPathFilter.get();

    // get files on the local FS in the attempt path
    Path attemptPath = getTaskAttemptPath(context);
    FileSystem attemptFS = getTaskAttemptFilesystem(context);
    RemoteIterator<LocatedFileStatus> iter = attemptFS
        .listFiles(attemptPath, true /* recursive */ );

    List<FileStatus> stats = Lists.newArrayList();
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      if (filter.accept(stat.getPath())) {
        stats.add(stat);
      }
    }

    return stats;
  }

  @Override
  protected int commitTaskInternal(TaskAttemptContext context,
      List<FileStatus> taskOutput) throws IOException {
    Path attemptPath = getTaskAttemptPath(context);
    Set<String> partitions = getPartitions(attemptPath, taskOutput);

    // enforce conflict resolution, but only if the mode is FAIL. for APPEND,
    // it doesn't matter that the partitions are already there, and for REPLACE,
    // deletion should be done during task commit.
    if (getConflictResolutionMode(context) == ConflictResolution.FAIL) {
      FileSystem s3 = getDestFS();
      for (String partition : partitions) {
        // getFinalPath adds the UUID to the file name. this needs the parent.
        Path partitionPath = getFinalPath(partition + "/file",
            context).getParent();
        if (s3.exists(partitionPath)) {
          throw new PathExistsException(partitionPath.toString());
        }
      }
    }
    return super.commitTaskInternal(context, taskOutput);
  }

  /**
   * The partition path conflict resolution assumes that:
   * <o>
   *   <li>FAIL checking has taken place earlier.</li>
   *   <li>APPEND is allowed</li>
   *   <li>REPLACE deletes all existing partitions</li>
   * </o>
   * @param context job context
   * @throws IOException failure in an operation
   */
  @Override
  public void commitJob(JobContext context) throws IOException {

    boolean threw = false;
    List<S3Util.PendingUpload> pending = null;
    try(DurationInfo d = new DurationInfo("Commit Job %s",
        context.getJobID())) {
      pending = getPendingUploads(context);

      FileSystem s3 = getDestFS();
      Set<Path> partitions = Sets.newLinkedHashSet();
      for (S3Util.PendingUpload commit : pending) {
        Path filePath = new Path(
            "s3a://" + commit.getBucketName() + "/" + commit.getKey());
        partitions.add(filePath.getParent());
      }

    // enforce conflict resolution
    threw = true;
      switch (getConflictResolutionMode(context)) {
        case FAIL:
          // FAIL checking is done on the task side, so this does nothing
          break;
        case APPEND:
          // no check is needed because the output may exist for appending
          break;
        case REPLACE:
          for (Path partitionPath : partitions) {
            LOG.info("Removing partition path to be replaced: " +
                partitionPath);
            s3.delete(partitionPath, true /* recursive */);
          }
          break;
        default:
          throw new IOException("Unknown conflict resolution mode: "
                  + getConflictResolutionMode(context));
      }

      threw = false;
    } finally {
      if (threw /* && pending != null */) {
        abortJobInternal(context, pending, threw);
      }
    }

    commitJobInternal(context, pending);
  }

  protected Set<String> getPartitions(Path attemptPath,
      List<FileStatus> taskOutput)
      throws IOException {
    // get a list of partition directories
    Set<String> partitions = Sets.newLinkedHashSet();
    for (FileStatus fileStatus : taskOutput) {
      // sanity check the output paths
      Path outputFile = fileStatus.getPath();
      if (!fileStatus.isFile()) {
        throw new PathIsDirectoryException(outputFile.toString());
      }
      String partition = getPartition(
          Paths.getRelativePath(attemptPath, outputFile));
      if (partition != null) {
        partitions.add(partition);
      } else {
        partitions.add(TABLE_ROOT);
      }
    }

    return partitions;
  }
}
