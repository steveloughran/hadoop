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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

/**
 * Implement task and job operations through the filesystem API.
 */
public class StoreOperationsThroughFileSystem implements StoreOperations {

  private static final Logger LOG = LoggerFactory.getLogger(
      StoreOperationsThroughFileSystem.class);

  private final FileSystem fs;

  /**
   * Constructor.
   * @param fs filesystem to write through.
   */
  public StoreOperationsThroughFileSystem(final FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return fs.getFileStatus(path);
  }

  @Override
  public boolean delete(Path path, boolean recursive)
      throws IOException {
    return fs.delete(path, recursive);
  }

  @Override
  public boolean mkdirs(Path path)
      throws IOException {
    return fs.mkdirs(path);
  }

  @Override
  public boolean renameFile(Path source, Path dest)
      throws IOException {
    return fs.rename(source, dest);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path path)
      throws IOException {
    return fs.listStatusIterator(path);
  }

  @Override
  public TaskManifest loadTaskManifest(FileStatus st) throws IOException {
    return TaskManifest.load(fs, st);
  }

  @Override
  public void save(final AbstractManifestData manifestData,
      final Path path, boolean overwrite) throws IOException {
    manifestData.save(fs, path, overwrite);
  }

  @Override
  public boolean moveToTrash(String jobId, Path path) {

    Path trashRoot = fs.getTrashRoot(path);
    Path subdir = new Path(trashRoot, jobId);
    try {
      return fs.rename(path, subdir);
    } catch (IOException e) {
      LOG.info("Failed to move {} to trash at {}: {}",
          path, subdir, e.toString());
      LOG.debug("Full stack", e);
      return false;
    }
  }
}