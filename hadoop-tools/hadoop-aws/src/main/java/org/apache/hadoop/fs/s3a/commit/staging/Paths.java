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

import java.io.IOException;
import java.net.URI;
import java.util.Random;

import com.google.common.base.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.s3a.commit.Pair;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Path operations for the staging committers.
 */
public final class Paths {

  private Paths() {
  }

  /**
   * Insert the UUID to a path if it is not there already.
   * If there is a trailing "." in the prefix after the last slash, the
   * UUID is inserted before it with a "-" prefix; otherwise appended.
   *
   * Examples:
   * <pre>
   *   /example/part-0000  ==> /example/part-0000-0ab34
   *   /example/part-0001.gz.csv  ==> /example/part-0001-0ab34.gz.csv
   *   /example/part-0002-0abc3.gz.csv  ==> /example/part-0002-0abc3.gz.csv
   * </pre>
   *
   * @param pathStr path as a string
   * @param uuid UUID to append
   * @return new path.
   */
  public static String addUUID(String pathStr, String uuid) {
    // In some cases, Spark will add the UUID to the filename itself.
    if (pathStr.contains(uuid)) {
      return pathStr;
    }

    int dot; // location of the first '.' in the file name
    int lastSlash = pathStr.lastIndexOf('/');
    if (lastSlash >= 0) {
      dot = pathStr.indexOf('.', lastSlash);
    } else {
      dot = pathStr.indexOf('.');
    }

    if (dot >= 0) {
      return pathStr.substring(0, dot) + "-" + uuid + pathStr.substring(dot);
    } else {
      return pathStr + "-" + uuid;
    }
  }

  public static Path getRoot(Path path) {
    Path current = path;
    while (!current.isRoot()) {
      current = current.getParent();
    }
    return current;
  }

  /**
   * Split a path into its parent path and filename.
   * @param pathStr path
   * @return a divided path string.
   */
  public static Pair<String, String> splitFilename(String pathStr) {
    int lastSlash = pathStr.lastIndexOf('/');
    return Pair.of(pathStr.substring(0, lastSlash), pathStr.substring(lastSlash + 1));
  }

  /**
   * Get the parent path of a string path: everything up to but excluding
   * the last "/" in the path.
   * @param pathStr path as a string
   * @return the parent or null if there is no parent.
   */
  public static String getParent(String pathStr) {
    int lastSlash = pathStr.lastIndexOf('/');
    if (lastSlash >= 0) {
      return pathStr.substring(0, lastSlash);
    }
    return null;
  }

  /**
   * Get hte filename of a path: the element after any "/", or, if there
   * is no "/" in the string, the whole path string.
   * @param pathStr path as a string
   * @return the final element in the path string
   */
  public static String getFilename(String pathStr) {
    int lastSlash = pathStr.lastIndexOf('/');
    if (lastSlash >= 0) {
      return pathStr.substring(lastSlash + 1);
    }
    return pathStr;
  }

  public static String getRelativePath(Path basePath,
                                       Path fullPath) {
    // TODO: test this thoroughly
    // Use URI.create(Path#toString) to avoid URI character escape bugs
    URI relative = URI.create(basePath.toString())
        .relativize(URI.create(fullPath.toString()));
    return relative.getPath();
  }


  /**
   * Varags constructor of paths. Not very efficient.
   * @param parent parent path
   * @param child child entries. "" elements are skipped.
   * @return the full child path.
   */
  public static Path path(Path parent, String ...child) {
    Path p = parent;
    for (String c : child) {
      if (!c.isEmpty()) {
        p = new Path(p, c);
      }
    }
    return p;
  }

  public static Path getLocalTaskAttemptTempDir(Configuration conf,
      String uuid, TaskAttemptID attempt) throws IOException {
    int taskId = attempt.getTaskID().getId();
    int attemptId = attempt.getId();
    return path(localTemp(conf, taskId, attemptId),
        uuid,
        Integer.toString(getAppAttemptId(conf)),
        attempt.toString());
  }

  /**
   * Try to come up with a good temp directory for different filesystems.
   * @param fs filesystem
   * @return a path under which temporary work can go.
   */
  public static Path tempDirForFileSystem(FileSystem fs) {
    Path temp;
    switch (fs.getScheme()) {
    case "file":
      temp = fs.makeQualified(new Path(System.getProperty(
          StagingCommitterConstants.JAVA_IO_TMPDIR)));
      break;
    case "s3a":
      temp = new Path("/tmp");
      break;

    // here assume that /tmp is valid
    case "hdfs":
    default:
      temp = fs.makeQualified(new Path("/tmp"));
    }
    return temp;
  }

  /**
   * Get the Application Attempt Id for this job.
   * @param conf the config to look in
   * @return the Application Attempt Id for a given job.
   */
  private static int getAppAttemptId(Configuration conf) {
    return conf.getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }

  /**
   * Build a temporary path for the multipart upload commit information
   * in the filesystem.
   * @param conf configuration defining default FS.
   * @param uuid uuid of job
   * @return a path which can be used for temporary work
   * @throws IOException on an IO failure.
   */
  public static Path getMultipartUploadCommitsDirectory(Configuration conf,
                                                        String uuid)
      throws IOException {
    Path userTmp = new Path(tempDirForFileSystem(FileSystem.get(conf)),
        UserGroupInformation.getCurrentUser().getShortUserName());
    Path work = new Path(userTmp, uuid);
    return new Path(work, StagingCommitterConstants.STAGING_UPLOADS);
  }

  // TODO: verify this is correct, it comes from dse-storage
  private static Path localTemp(Configuration conf, int taskId, int attemptId)
      throws IOException {
    String[] dirs = conf.getStrings(
        StagingCommitterConstants.MAPREDUCE_CLUSTER_LOCAL_DIR);
    Random rand = new Random(Objects.hashCode(taskId, attemptId));
    String dir = dirs[rand.nextInt(dirs.length)];

    return FileSystem.getLocal(conf).makeQualified(new Path(dir));
  }

  public static String removeStartingAndTrailingSlash(String path) {
    int start = 0;
    if (path.startsWith("/")) {
      start = 1;
    }

    int end = path.length();
    if (path.endsWith("/")) {
      end -= 1;
    }

    return path.substring(start, end);
  }

  /**
   * path filter.
   */
  public static final class HiddenPathFilter implements PathFilter {
    private static final HiddenPathFilter INSTANCE = new HiddenPathFilter();
  
    public static HiddenPathFilter get() {
      return INSTANCE;
    }
  
    private HiddenPathFilter() {
    }
  
    @Override
    public boolean accept(Path path) {
      return !path.getName().startsWith(".")
          && !path.getName().startsWith("_");
    }
  }

}
