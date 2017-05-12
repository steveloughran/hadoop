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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;

import com.amazonaws.AmazonClientException;

/**
 * Class to provide lambda expression access to AWS operations, including
 * any retry policy.
 */
class AwsCall {

  /**
   * Execute an operation.
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param operation operation to execute
   * @param <T> type of return value
   * @return the result of the operation
   * @throws IOException any IOE raised, or translated exception
   */
  <T> T execute(String action, String path, Operation<T> operation)
      throws IOException {
    try {
      return operation.execute();
    } catch (AmazonClientException e) {
      throw S3AUtils.translateException(action, path, e);
    }
  }

  /**
   * Execute an operation with no result
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param operation operation to execute
   * @throws IOException any IOE raised, or translated exception
   */
  void execute(String action, String path, VoidOperation operation)
      throws IOException {
    execute(action, path,
        () -> {
          operation.execute();
          return null;
        });
  }

  /**
   * Arbitrary operation throwing an IOException
   * @param <T> return type
   */
  interface Operation<T> {
    T execute() throws IOException;
  }

  /**
   * Void operation which may raise an IOException
   */
  interface VoidOperation {
    void execute() throws IOException;
  }

}
