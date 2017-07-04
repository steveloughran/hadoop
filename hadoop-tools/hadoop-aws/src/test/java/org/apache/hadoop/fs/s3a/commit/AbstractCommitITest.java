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

import java.io.IOException;
import java.io.InterruptedIOException;

import com.amazonaws.services.s3.AmazonS3;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.InconsistentAmazonS3Client;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.enableInconsistentS3Client;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_COMMITTER_ENABLED;

/**
 * Test case for committer operations; sets up the config for delayed commit
 * and the S3 committer.
 *
 * By default, these tests enable the inconsistent committer, with
 * a delay of {@link #CONSISTENCY_DELAY}
 */
public abstract class AbstractCommitITest extends AbstractS3ATestBase {


  protected static final int CONSISTENCY_DELAY = 500;
  protected static final int CONSISTENCY_PROBE_INTERVAL = 500;
  protected static final int CONSISTENCY_WAIT = CONSISTENCY_DELAY * 2;
  protected InconsistentAmazonS3Client inconsistentClient;

  /**
   * Should the inconsistent S3A client be used?
   * Default value: true.
   * @return true for inconsistent listing
   */
  public boolean useInconsistentClient() {
    return true;
  }

  /**
   * switch to an inconsistent path if in inconsistent mode.
   * {@inheritDoc}
   */
  @Override
  protected Path path(String filepath) throws IOException {
    return useInconsistentClient() ?
           super.path(InconsistentAmazonS3Client.DEFAULT_DELAY_KEY_SUBSTRING
               + "/" + filepath)
           : super.path(filepath);
  }

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
    if (useInconsistentClient()) {
      enableInconsistentS3Client(conf, CONSISTENCY_DELAY);
    }
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    if (useInconsistentClient()) {
      AmazonS3 client = getFileSystem()
          .getAmazonS3ClientForTesting("fault injection");
      Assert.assertTrue(
          "AWS client is not inconsistent, even though the test requirees it "
          + client,
          client instanceof InconsistentAmazonS3Client);
      inconsistentClient = (InconsistentAmazonS3Client) client;
    }
  }

  @Override
  public void teardown() throws Exception {
    waitForConsistency();
    // make sure there are no failures any more
    resetFailures();
    super.teardown();
  }

  /**
   * Wait a multiple of the inconsistency delay for things to stabilize;
   * no-op if the consistent client is used.
   * @throws InterruptedIOException if the sleep is interrupted
   */
  protected void waitForConsistency() throws InterruptedIOException {
    if (useInconsistentClient() && inconsistentClient != null) {
      try {
        Thread.sleep(2* inconsistentClient.getDelayKeyMsec());
      } catch (InterruptedException e) {
        throw (InterruptedIOException)
            (new InterruptedIOException("while waiting for consistency: " + e)
                .initCause(e));
      }
    }
  }

  /**
   * Set the throttling factor on requests.
   * @param p probability of a throttling occurring: 0-1.0
   */
  protected void setThrottling(float p) {
    inconsistentClient.setThrottleProbability(p);
  }

  /**
   * Set the throttling factor on requests and number of calls to throttle.
   * @param p probability of a throttling occurring: 0-1.0
   * @param limit limit to number of calls which fail
   */
  protected void setThrottling(float p, int limit) {
    inconsistentClient.setThrottleProbability(p);
    setFailureLimit(limit);
  }

  /**
   * Turn off throttling.
   */
  protected void resetFailures() {
    if (inconsistentClient != null) {
      setThrottling(0, 0);
    }
  }

  /**
   * Set failure limit.
   * @param limit limit to number of calls which fail
   */
  private void setFailureLimit(int limit) {
    inconsistentClient.setFailureLimit(limit);
  }
}
