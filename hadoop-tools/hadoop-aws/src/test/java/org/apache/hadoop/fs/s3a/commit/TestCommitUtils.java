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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.test.LambdaTestUtils.*;
import static org.apache.hadoop.fs.s3a.commit.CommitUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Tests for {@link CommitUtils} methods.
 */
public class TestCommitUtils extends Assert {

  private static final List<String> PENDING_AT_ROOT = list(PENDING_PATH);
  private static final List<String> PENDING_AT_ROOT_WITH_CHILD =
      list(PENDING_PATH, "child");
  private static final List<String> PENDING_WITH_CHILD =
      list("parent", PENDING_PATH, "child");
  private static final List<String> PENDING_AT_WITHOUT_CHILD =
      list("parent", PENDING_PATH);

  private static final List<String> DEEP_PENDING =
      list("parent1", "parent2", PENDING_PATH, "child1", "child2");

  public static final String[] EMPTY = {};

  @Test
  public void testSplitPathEmpty() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> splitPathToElements(new Path("")));
  }

  @Test
  public void testSplitPathDoubleBackslash() throws Throwable {
    assertPathSplits("//", EMPTY);
  }

  @Test
  public void testSplitRootPath() throws Throwable {
    assertPathSplits("/", EMPTY);
  }

  @Test
  public void testSplitBasic() throws Throwable {
    assertPathSplits("/a/b/c",
        new String[]{"a", "b", "c"});
  }

  @Test
  public void testSplitTrailingSlash() throws Throwable {
    assertPathSplits("/a/b/c/",
        new String[]{"a", "b", "c"});
  }

  @Test
  public void testSplitShortPath() throws Throwable {
    assertPathSplits("/a",
        new String[]{"a"});
  }

  @Test
  public void testSplitShortPathTrailingSlash() throws Throwable {
    assertPathSplits("/a/",
        new String[]{"a"});
  }

  @Test
  public void testParentsPendingRoot() throws Throwable {
    assertParents(EMPTY, PENDING_AT_ROOT);
  }

  @Test
  public void testChildrenPendingRoot() throws Throwable {
    assertChildren(EMPTY, PENDING_AT_ROOT);
  }

  @Test
  public void testParentsPendingRootWithChild() throws Throwable {
    assertParents(EMPTY, PENDING_AT_ROOT_WITH_CHILD);
  }

  @Test
  public void testChildPendingRootWithChild() throws Throwable {
    assertChildren(a("child"), PENDING_AT_ROOT_WITH_CHILD);
  }

  @Test
  public void testChildrenPendingWithoutChild() throws Throwable {
    assertChildren(EMPTY, PENDING_AT_WITHOUT_CHILD);
  }

  @Test
  public void testChildPendingWithChild() throws Throwable {
    assertChildren(a("child"), PENDING_WITH_CHILD);
  }

  @Test
  public void testParentPendingWithChild() throws Throwable {
    assertParents(a("parent"), PENDING_WITH_CHILD);
  }

  @Test
  public void testParentDeepPending() throws Throwable {
    assertParents(a("parent1", "parent2"), DEEP_PENDING);
  }

  @Test
  public void testChildrenDeepPending() throws Throwable {
    assertChildren(a("child1", "child2"), DEEP_PENDING);
  }

  @Test
  public void testLastElementEmpty() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> lastElement(new ArrayList<>(0)));
  }

  @Test
  public void testLastElementSingle() throws Throwable {
    assertEquals("first", lastElement(l("first")));
  }

  @Test
  public void testLastElementDouble() throws Throwable {
    assertEquals("2", lastElement(l("first", "2")));
  }

  @Test
  public void testFinalDestinationNoPending() throws Throwable {
    assertEquals(l("first", "2"),
        finalDestination(l("first", "2")));
  }

  @Test
  public void testFinalDestinationPending1() throws Throwable {
    assertEquals(l("first", "2"),
        finalDestination(l("first", PENDING_PATH, "2")));
  }

  @Test
  public void testFinalDestinationPending2() throws Throwable {
    assertEquals(l("first", "3.txt"),
        finalDestination(l("first", PENDING_PATH, "2", "3.txt")));
  }

  @Test
  public void testFinalDestinationRootPending2() throws Throwable {
    assertEquals(l("3.txt"),
        finalDestination(l(PENDING_PATH, "2", "3.txt")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFinalDestinationPendingNoChild() throws Throwable {
    finalDestination(l(PENDING_PATH));
  }

  @Test
  public void testFinalDestinationBaseDirectChild() throws Throwable {
    finalDestination(l(PENDING_PATH, BASE_PATH, "3.txt"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFinalDestinationBaseNoChild() throws Throwable {
    assertEquals(l(),
        finalDestination(l(PENDING_PATH, BASE_PATH)));
  }

  @Test
  public void testFinalDestinationBaseSubdirsChild() throws Throwable {
    assertEquals(l("2", "3.txt"),
        finalDestination(l(PENDING_PATH, "4", BASE_PATH, "2", "3.txt")));
  }

  /**
   * If the base is above the pending dir, it's ignored.
   */
  @Test
  public void testFinalDestinationIgnoresBaseBeforePending() throws Throwable {
    assertEquals(l(BASE_PATH, "home", "3.txt"),
        finalDestination(l(BASE_PATH, "home", PENDING_PATH, "2", "3.txt")));
  }

  /** varargs to array. */
  private static String[] a(String... str) {
    return str;
  }

  /** list to array. */
  private static List<String> l(String... str) {
    return Arrays.asList(str);
  }

  /**
   * Varags to list.
   * @param args arguments
   * @return a list
   */
  private static List<String> list(String... args) {
    return Lists.newArrayList(args);
  }

  public void assertParents(String[] expected, List<String> elements) {
    assertListEquals(expected, pendingPathParents(elements));
  }

  public void assertChildren(String[] expected, List<String> elements) {
    assertListEquals(expected, pendingPathChildren(elements));
  }

  private void assertPathSplits(String pathString, String[] expected) {
    Path path = new Path(pathString);
    assertArrayEquals("From path " + path, expected,
        splitPathToElements(path).toArray());
  }

  private void assertListEquals(String[] expected, List<String> actual) {
    assertArrayEquals(expected, actual.toArray());
  }

}
