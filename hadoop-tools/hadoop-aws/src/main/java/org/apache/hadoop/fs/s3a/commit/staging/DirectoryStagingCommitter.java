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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.s3a.commit.DurationInfo;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DirectoryStagingCommitter extends StagingS3GuardCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(
      DirectoryStagingCommitter.class);

  public DirectoryStagingCommitter(Path outputPath, JobContext context) throws IOException {
    super(outputPath, context);
  }

  public DirectoryStagingCommitter(Path outputPath, TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    super.setupJob(context);
    Path outputPath = getOutputPath(context);
    FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
    if (fs.exists(outputPath)) {
      switch (getMode(context)) {
        case FAIL:
          throw new PathExistsException(outputPath.toString());
        case APPEND:
        case REPLACE:
          // do nothing.
          // removing the directory, if overwriting is done in commitJob, in
          // case there is a failure before commit.
      }
    }
  }

  // TODO: handle aborting commits if delete or exists throws an exception
  @Override
  public void commitJob(JobContext context) throws IOException {
    try (DurationInfo d = new DurationInfo("Commit Job %s",
        context.getJobID())) {
      Path outputPath = getOutputPath(context);
      // use the FS implementation because it will check for _$folder$
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      if (fs.exists(outputPath)) {
        switch (getMode(context)) {
          case FAIL:
            // this was checked in setupJob, but this avoids some cases where
            // output was created while the job was processing
            throw new PathExistsException(outputPath.toString());
          case APPEND:
            // do nothing
            break;
          case REPLACE:
            LOG.info("Removing output path to be replaced: {}", outputPath);
            fs.delete(outputPath, true /* recursive */);
            break;
          default:
            throw new IOException(
                "Unknown conflict resolution mode: "
                    + getConfictModeOption(context));
        }
      }
    } catch (IllegalArgumentException e) {
      // raised when unable to map from confict mode to an enum value.
      throw new IOException(
          "Unknown conflict resolution mode: "
              + getConfictModeOption(context), e);
    }

      super.commitJob(context);
  }
}
