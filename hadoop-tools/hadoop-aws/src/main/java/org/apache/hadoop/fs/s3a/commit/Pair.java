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

/**
 * A typed tuple.
 * @param <L> left element type
 * @param <R> right element type
 */
public class Pair<L, R> {
  private final L first;
  private final R second;

  /**
   * Create an instance.
   * @param first first element
   * @param second second
   * @param <L> type of first element.
   * @param <R> type of second element
   * @return a pair instance
   */
  public static <L, R> Pair<L, R> of(L first, R second) {
    return new Pair<>(first, second);
  }

  public Pair(L first, R second) {
    this.first = first;
    this.second = second;
  }

  /**
   * Get the first element.
   * @return the first element
   */
  public L _1() {
    return first;
  }

  /**
   * Get the second element.
   * @return the second element.
   */
  public R _2() {
    return second;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("(");
    sb.append(first);
    sb.append(", ").append(second);
    sb.append(')');
    return sb.toString();
  }
}
