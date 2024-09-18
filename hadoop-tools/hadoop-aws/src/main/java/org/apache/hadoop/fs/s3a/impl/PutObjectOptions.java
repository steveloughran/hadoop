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

package org.apache.hadoop.fs.s3a.impl;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Extensible structure for options when putting/writing objects.
 */
public final class PutObjectOptions {

  /**
   * Storage class, if not null.
   */
  private final String storageClass;

  /**
   * Headers; may be null.
   */
  private final Map<String, String> headers;

  /**
   * Constructor.
   * @param storageClass Storage class, if not null.
   * @param headers Headers; may be null.
   */
  public PutObjectOptions(
      @Nullable final String storageClass,
      @Nullable final Map<String, String> headers) {
    this.storageClass = storageClass;
    this.headers = headers;
  }

  /**
   * Headers for the put/post request.
   * @return headers or null.
   */
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public String toString() {
    return "PutObjectOptions{" +
        ", storageClass='" + storageClass + '\'' +
        '}';
  }

  private static final PutObjectOptions KEEP_DIRS = new PutObjectOptions(
      null, null);
  private static final PutObjectOptions DELETE_DIRS = new PutObjectOptions(
      null, null);

  /**
   * Get the options to keep directories.
   * @return an instance which keeps dirs
   */
  public static PutObjectOptions keepingDirs() {
    return KEEP_DIRS;
  }

  /**
   * Get the options to delete directory markers.
   * @return an instance which deletes dirs
   */
  public static PutObjectOptions deletingDirs() {
    return DELETE_DIRS;
  }

}