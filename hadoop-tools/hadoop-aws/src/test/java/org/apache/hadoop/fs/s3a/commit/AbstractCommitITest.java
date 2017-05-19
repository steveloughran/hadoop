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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_COMMITTER_ENABLED;

/**
 * Test case for committer operations; sets up the config for delayed commit
 * and the S3 committer.
 */
public abstract class AbstractCommitITest extends AbstractS3ATestBase {

  /**
   * Creates a configuration for commit operations: commit is enabled in the FS
   * and output is multipart to on-heap arrays.
   * @return a configuration to use when creating an FS.
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
//    conf.setBoolean("fs.s3a.impl.disable.cache", true);
    conf.setBoolean(MAGIC_COMMITTER_ENABLED, true);
    conf.setLong(MIN_MULTIPART_THRESHOLD, MULTIPART_MIN_SIZE);
    conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    conf.setBoolean(FAST_UPLOAD, true);
    conf.set(FAST_UPLOAD_BUFFER, FAST_UPLOAD_BUFFER_ARRAY);
    return conf;
  }
}
