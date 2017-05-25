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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.STAGING_COMMITTER_FACTORY;

public class TestCommitterBinding extends Assert {

  @Test
  public void testCommitterFactory() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(PathOutputCommitterFactory.OUTPUTCOMMITTER_FACTORY_CLASS,
        STAGING_COMMITTER_FACTORY);
    PathOutputCommitterFactory factory
        = PathOutputCommitterFactory.getOutputCommitterFactory(conf);
    assertTrue("Wrong committer factory: " + factory,
        factory instanceof StagingCommitterFactory);
  }

  private final Throwable verifyCauseClass(Throwable ex,
      Class<? extends Throwable> clazz) throws Throwable {
    Throwable cause = ex.getCause();
    if (cause == null) {
      throw ex;
    }
    if (!cause.getClass().equals(clazz)) {
      throw cause;
    }
    return cause;
  }

  @Test
  public void testBadCommitterFactory() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(PathOutputCommitterFactory.OUTPUTCOMMITTER_FACTORY_CLASS,
        "Not a factory");
    RuntimeException ex = LambdaTestUtils.intercept(
        RuntimeException.class,
        () -> PathOutputCommitterFactory.getOutputCommitterFactory(conf));
    verifyCauseClass(
        verifyCauseClass(ex, RuntimeException.class),
        ClassNotFoundException.class);
  }
}
