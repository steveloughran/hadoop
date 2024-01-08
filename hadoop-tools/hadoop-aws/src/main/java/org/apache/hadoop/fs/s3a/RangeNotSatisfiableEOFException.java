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

import java.io.EOFException;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Status code 416, range not satisfiable.
 * Subclass of {@link EOFException} so that any code which expects that to
 * be the outcome of a 416 failure will continue to work.
 */
public class RangeNotSatisfiableEOFException extends EOFException {
  public RangeNotSatisfiableEOFException(
      String operation,
      AwsServiceException cause) {
    super(operation);
    initCause(cause);
  }
}
