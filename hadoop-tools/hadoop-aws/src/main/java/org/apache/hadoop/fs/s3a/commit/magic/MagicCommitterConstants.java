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

package org.apache.hadoop.fs.s3a.commit.magic;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants related to the Magic committer
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class MagicCommitterConstants {

  /**
   * Path for "magic" pending writes: path and {@link #PENDING_SUFFIX} files:
   * {@value}.
   */
  public static final String MAGIC_DIR_NAME = "__magic";

  /**
   * Marker of the start of a directory tree for calculating
   * the final path names: {@value}.
   */
  public static final String BASE_PATH = "__base";

  /**
   * Suffix applied to pending commit data: {@value}.
   */
  public static final String PENDING_SUFFIX = ".pending";

  /**
   * Suffix applied to multiple pending commits data: {@value}.
   */
  public static final String PENDING_MANY_SUFFIX = ".pending-many";

  private MagicCommitterConstants() {
  }
}
