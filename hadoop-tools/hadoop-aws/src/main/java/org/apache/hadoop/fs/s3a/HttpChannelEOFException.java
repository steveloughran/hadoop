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

/**
 * Http channel exception; subclass of EOFException.
 * In particular:
 * - NoHttpResponseException
 * - OpenSSL errors
 * The http client library exceptions may be shaded/unshaded; this is the
 * exception used in retry policies.
 */
public class HttpChannelEOFException extends EOFException {

  public HttpChannelEOFException(final String path,
      final String error,
      final Throwable cause) {
    super(error);
    initCause(cause);
  }
}
