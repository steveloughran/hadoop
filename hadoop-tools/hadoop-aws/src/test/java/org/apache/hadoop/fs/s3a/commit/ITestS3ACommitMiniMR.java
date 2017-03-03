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

import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;

/**
 * Test miniMR operations.
 * The miniMR cluster is spun up the first test setup; it is torn down
 * after the entire class. That is: the cluster is shared by all
 * test cases.
 */
public class ITestS3ACommitMiniMR extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ACommitMiniMR.class);

  private static MiniMRYarnCluster mrCluster;

  @AfterClass
  public static void stopMiniMRCluster() {
    ServiceOperations.stopQuietly(mrCluster);
    mrCluster = null;
  }

  @Override
  public void teardown() throws Exception {
    ServiceOperations.stop(mrCluster);
    super.teardown();
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    if (mrCluster == null) {

      MiniMRYarnCluster cluster =
          new MiniMRYarnCluster("ITestS3AMiniMROutput");
      cluster.init(getConfiguration());
      cluster.start();
      LOG.info("Started cluster {}", cluster);
      mrCluster = cluster;
    }
  }

  @Test
  public void testMRClusterLive() throws Throwable {
    assertTrue("Wrong state " + mrCluster,
        mrCluster.isInState(Service.STATE.STARTED));
  }

}
