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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_RENAME;

/**
 * Statistic names for committers.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ManifestCommitterStatisticNames {

  /** "Count of successful tasks:: {@value}. */
  public static final String COMMITTER_TASKS_COMPLETED =
      "committer_tasks_completed";

  /** Amount of data committed: {@value}. */
  public static final String COMMITTER_BYTES_COMMITTED_COUNT =
      "committer_bytes_committed";

  /** Count of failed tasks: {@value}. */
  public static final String COMMITTER_TASKS_FAILED_COUNT =
      "committer_tasks_failed";

  /** Count of commits aborted: {@value}. */
  public static final String COMMITTER_COMMITS_ABORTED_COUNT =
      "committer_commits_aborted";

  /** Count of commits reverted: {@value}. */
  public static final String COMMITTER_COMMITS_REVERTED_COUNT =
      "committer_commits_reverted";

  /** Count of commits failed: {@value}. */
  public static final String COMMITTER_COMMITS_FAILED =
      "committer_commits" + StoreStatisticNames.SUFFIX_FAILURES;

  /** Duration Tracking of time to commit an entire job: {@value}. */
  public static final String COMMITTER_COMMIT_JOB =
      "committer_commit_job";



  public static final String OP_CREATE_DIRECTORIES = "op_create_directories";

  public static final String OP_CREATE_ONE_DIRECTORY =
      "op_create_one_directory";

  /** {@value}. */
  public static final String OP_DIRECTORY_SCAN = "op_directory_scan";

  public static final String OP_STAGE_JOB_COMMIT = "op_job_commit";

  /**
   * Counter of bytes committed in job.
   */
  public static final String OP_JOB_COMMITTED_BYTES = "op_job_committed_bytes";

  /**
   * Counter of files committed in job.
   */
  public static final String OP_JOB_COMMITTED_FILES = "op_job_committed_files";

  /** {@value}. */
  public static final String OP_LOAD_ALL_MANIFESTS = "op_load_all_manifests";

  /**
   * Load a task manifest.
   */
  public static final String OP_LOAD_MANIFEST = "op_load_manifest";

  /** {@value}. */
  public static final String OP_RENAME_FILE = OP_RENAME;

  /**
   * Save a task manifest.
   */
  public static final String OP_SAVE_TASK_MANIFEST =
      "op_save_task_manifest";

  /**
   * Task abort.
   */
  public static final String OP_STAGE_TASK_ABORT_TASK = "op_task_stage_abort_task";

  /**
   * Job cleanup.
   */
  public static final String OP_STAGE_JOB_CLEANUP = "op_job_stage_cleanup";

  /**
   * Rename files stage duration.
   */
  public static final String OP_STAGE_JOB_RENAME_FILES =
      "op_job_stage_rename_files";

  /**
   * Prepare Directories Stage.
   */
  public static final String OP_STAGE_JOB_CREATE_TARGET_DIRS =
      "op_job_stage_create_target_dirs";

  public static final String OP_STAGE_JOB_LOAD_MANIFESTS =
      "op_job_stage_load_manifests";

  public static final String OP_STAGE_JOB_SETUP = "op_job_stage_setup";

  public static final String OP_STAGE_JOB_SAVE_SUCCESS =
      "op_job_stage_save_success_marker";

  public static final String OP_STAGE_TASK_SAVE_MANIFEST =
      "op_task_stage_save_manifest";

  public static final String OP_STAGE_TASK_SETUP = "op_task_stage_setup";

  public static final String OP_STAGE_JOB_VALIDATE_OUTPUT =
      "op_job_stage_optional_validate_output";

  public static final String OP_STAGE_TASK_COMMIT = "op_stage_task_commit";

  /** {@value}. */
  public static final String OP_STAGE_TASK_SCAN_DIRECTORY = "op_stage_task_scan_directory";
}
