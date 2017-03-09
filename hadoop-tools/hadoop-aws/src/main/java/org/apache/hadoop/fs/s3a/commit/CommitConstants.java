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

import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public interface CommitConstants {

  /**
   * Flag to indicate whether the S3 committer is enabled, and
   * so {@code create()} calls under the path {@link #MAGIC_DIR_NAME} will
   * be converted to pending commit operations.
   * Value: {@value}.
   */
  String COMMITTER_ENABLED
      = "fs.s3a.committer.enabled";

  /**
   * Is the committer enabled by default? No.
   */
  boolean DEFAULT_COMMITTER_ENABLED = false;

  /**
   * Path for "magic" pending writes: path and {@link #PENDING_SUFFIX} files:
   * {@value}.
   */
  String MAGIC_DIR_NAME = "__magic";

  /**
   * This is the "Pending" directory of the FileOutputCommitter;
   * data written here is, in that algorithm, renamed into place.
   * Value: {@value}. Why is the name unchanged? For consistency
   * of code review.
   */
  String PENDING_DIR_NAME = "_temporary";

  /**
   * Marker of the start of a directory tree for calculating
   * the final path names: {@value}.
   */
  String BASE_PATH = "__base";

  /**
   * Temp data which is not auto-committed: {@value}.
   * Uses a different name from normal just to make clear it is different.
   */
  String TEMP_DATA_PATH = "__temp-data";


  /**
   * Suffix applied to pending commit data: {@value}.
   */
  String PENDING_SUFFIX = ".pending";

  /**
   * Flag to trigger creation of a marker file on job completion.
   */
  String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER
      = FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;

  /**
   * Marker file to create on success.
   */
  String SUCCESS_FILE_NAME = "_SUCCESS";

}
