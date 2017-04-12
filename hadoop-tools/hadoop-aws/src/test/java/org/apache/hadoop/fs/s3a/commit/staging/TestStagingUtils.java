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

/**
 * Test the various operations offered in Staging classes such as
 * {@link StagingS3Util} and {@link Paths}.
 */
public class TestStagingUtils extends Assert {

  public static final String UUID = "0ab34";

  @Test
  public void testPathAddUUID() throws Throwable {
    assertAdded("/example/part-0000",
        "/example/part-0000-0ab34");
    assertAdded("/example/part-0001.gz.csv",
        "/example/part-0001-0ab34.gz.csv");
    assertAdded("/example/part-0002-0ab34.gz.csv",
        "/example/part-0002-0ab34.gz.csv");
  }

  private void assertAdded(String pathStr, String expected) {
    assertEquals("adding UUID to "  + pathStr,
        expected, Paths.addUUID(pathStr, UUID));
  }
}
