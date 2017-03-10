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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;

import java.io.IOException;

/**
 * Dynamically create the output committer based on the filesystem type.
 * For S3A output, uses the {@link MagicS3GuardCommitter}; for other filesystems
 * use the classic committer.
 */
public abstract class Abstract3GuardCommitterFactory
    extends PathOutputCommitterFactory {
  public static final Logger LOG = LoggerFactory.getLogger(
      Abstract3GuardCommitterFactory.class);

  /**
   * Name of this class: {@value}.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory";

  @Override
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    FileSystem fs = getDestFS(outputPath, context);
    PathOutputCommitter outputCommitter;
    if (fs instanceof S3AFileSystem) {
      outputCommitter = createTaskCommitter(outputPath, context);
    } else {
      outputCommitter = super.createOutputCommitter(outputPath, context);
    }
    LOG.info("Using Commmitter {} for {}",
        outputCommitter,
        outputPath);
    return outputCommitter;
  }

  protected abstract AbstractS3GuardCommitter createTaskCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException;

  public FileSystem getDestFS(Path outputPath, JobContext context)
      throws IOException {
    return outputPath != null ?
          FileSystem.get(outputPath.toUri(), context.getConfiguration())
          : null;
  }

  @Override
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      JobContext context) throws IOException {
    FileSystem fs = getDestFS(outputPath, context);
    PathOutputCommitter outputCommitter;
    if (fs instanceof S3AFileSystem) {
      outputCommitter = createJobCommitter(outputPath, context);
    } else {
      outputCommitter = super.createOutputCommitter(outputPath, context);
    }
    LOG.info("Using Commmitter {} for {}",
        outputCommitter,
        outputPath);
    return outputCommitter;
  }

  protected abstract AbstractS3GuardCommitter createJobCommitter(Path outputPath,
      JobContext context) throws IOException;
}
