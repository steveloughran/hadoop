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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractS3GuardCommitterFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;

/**
 * Factory for the Directory committer.
 */
public class DirectoryStagingCommitterFactory
    extends AbstractS3GuardCommitterFactory {

  /**
   * Name of this class: {@value}.
   */
  public static final String CLASSNAME
      = "org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory";

  public PathOutputCommitter createTaskCommitter(S3AFileSystem fileSystem,
      Path outputPath,
      TaskAttemptContext context) throws IOException {
    return new DirectoryStagingCommitter(outputPath, context);
  }

  public PathOutputCommitter createJobCommitter(S3AFileSystem fileSystem,
      Path outputPath,
      JobContext context) throws IOException {
    return new DirectoryStagingCommitter(outputPath, context);
  }
}
