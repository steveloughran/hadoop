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

package org.apache.hadoop.mapreduce.lib.output;

import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * A factory for committers implementing the {@link PathOutputCommitter}
 * methods, and so can be used from {@link FileOutputFormat}.
 * The base implementation returns {@link FileOutputFormat} instances.
 */
public class PathOutputCommitterFactory extends Configured {
  private static final Logger LOG =
      LoggerFactory.getLogger(PathOutputCommitterFactory.class);

  /**
   * Name of the configuration option used to configure the output committer.
   */
  public static final String OUTPUTCOMMITTER_FACTORY_CLASS =
      "mapreduce.pathoutputcommitter.factory.class";

  /**
   * Default committer factory name: {@value}.
   */

  public static final String OUTPUTCOMMITTER_FACTORY_DEFAULT =
      "org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory";

  /**
   * Create an output committer for a task attempt.
   * @param outputPath output path. This may be null.
   * @param context context
   * @return a new committer
   * @throws IOException problems instantiating the committer
   */
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    return new FileOutputCommitter(outputPath, context);
  }

  /**
   * Create a path output committer for a job
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException problems instantiating the committer
   */
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      JobContext context) throws IOException {
    return new FileOutputCommitter(outputPath, context);
  }

  /**
   * Get the committer factory for a configuration.
   * @param conf configuration
   * @return an instantiated committer factory
   */
  public static PathOutputCommitterFactory getOutputCommitterFactory(
      Configuration conf) {

    Class<? extends PathOutputCommitterFactory> factory =
        conf.getClass(OUTPUTCOMMITTER_FACTORY_CLASS,
            PathOutputCommitterFactory.class,
            PathOutputCommitterFactory.class);
    LOG.debug("Using FileOutputCommitter factory class {}", factory);
    return ReflectionUtils.newInstance(factory, conf);
  }
}
