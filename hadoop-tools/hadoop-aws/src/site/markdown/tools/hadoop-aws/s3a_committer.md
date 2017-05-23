<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# S3A Committers

<!-- DISABLEDMACRO{toc|fromDepth=0|toDepth=5} -->

This page covers the S3A Committers, which can commit work directly
to an S3 object store which supports consistent metadata.

These committers are designed to solve a fundamental problem which 
the standard committers of work cannot do to S3: consistent, high performance,
reliable commitment of work done by individual workers into the final set of
results of a job.


## Choosing a committer

The choice of which committer to use for writing data via `FileOutputFormat`
is set in the configuration option `mapreduce.pathoutputcommitter.factory.class`.
This declares a the classname of a class which creates committers for jobs and tasks.


| factory | description |
|--------|---------|
| `org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory` |  The default file output committer |
| `org.apache.hadoop.fs.s3a.commit.DynamicCommitterFactory` | Dynamically choose the committer on a per-bucket basis |
| `org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory` |  Use the Directory Staging Committer|
| `org.apache.hadoop.fs.s3a.commit.staging.PartitonedStagingCommitterFactory` |  Partitioned Staging Committer|
| `org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory` | Use the magic committer (not yet ready for production use) |

All of the s3a committers revert to provide a classic FileOutputCommitter instance
when a commmitter is requested for any filesystem other than an S3A one.
This allows any of these committers to be declared in an application configuration,
without worrying about the job failing for any queries using `hdfs://` paths as
the final destination of work.

The Dynamic committer factory is different in that it allows any of the other committer
factories to be used to create a committer, based on the specific value of theoption `fs.s3a.committer.name`:

| value of `fs.s3a.committer.name` |  meaning |
|--------|---------|
| `file` | the original File committer; (not safe for use with S3 storage) |
| `directory` | directory staging committer |
| `partition` | partition staging committer |
| `magic` | the "magic" committer |

The dynamic committer was originally written to allow for easier performance
testing of the committers from applications such as Apache Zeppelin notebooks:
different buckets can be given a different committer with S3A's per-bucket
configuration, and the performance of the operations compared.




### Staging Committer Options

The initial option set:

| option | meaning |
|--------|---------|
| `fs.s3a.committer.staging.conflict-mode` | how to resolve directory conflicts during commit: `fail`, `append`, or `replace`; defaults to `fail`. |
| `fs.s3a.committer.staging.unique-filenames` | Should the committer generate unique filenames by including a unique ID in the name of each created file? |
| `fs.s3a.committer.staging.uuid` | a UUID that identifies a write; `spark.sql.sources.writeJobUUID` is used if not set |
| `fs.s3a.committer.tmp.path` | Directory in the cluster filesystem used for storing information on the uncommitted files. |
| `fs.s3a.committer.staging.upload.size` | size, in bytes, to use for parts of the upload to S3; defaults: `10M` |
| `fs.s3a.committer.threads` | number of threads to use to complete S3 uploads during job commit; default: `8` |
| `mapreduce.fileoutputcommitter.marksuccessfuljobs` | flag to control creation of `_SUCCESS` marker file on job completion. Default: `true` |
| `fs.s3a.multipart.size` | Size in bytes of each part of a multipart upload. Default: `100M` |
| `fs.s3a.buffer.dir` | Directory in local filesystem under which data is saved before being uploaded. Example: `/tmp/hadoop/s3a/` |

Generated files are initially written to a local directory underneath one of the temporary
directories listed in `fs.s3a.buffer.dir`.

Temporary files are saved in HDFS (or other cluster filesystem )under the path
`${fs.s3a.committer.tmp.path}/${user}` where `user` is the name of the user running the job.
The default value of `fs.s3a.committer.tmp.path` is `/tmp`, so the temporary directory
for any application attempt will be a path `/tmp/${user}.
In the special case in which the local `file:` filesystem is the cluster filesystem, the
location of the temporary directory is that of the JVM system property
`java.io.tmpdir`.

The application attempt ID is used to create a unique path under this directory,
resulting in a path `/tmp/${user}/${application-attempt-id}/` under which
summary data of each task's pending commits are managed using the standard
`FileOutputFormat` committer.



### Background: The S3 multi-part PUT mechanism

In the [S3 REST API](http://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html),
multipart uploads allow clients to upload a series of "Parts" of a file,
then commit the upload with a final call.

1. Caller initiates a multipart request, including the destination bucket, key
and metadata.

        POST bucket.s3.aws.com/path?uploads

    An UploadId is returned

1. Caller uploads one or more parts.

        PUT bucket.s3.aws.com/path?partNumber=PartNumber&uploadId=UploadId

    The part number is used to declare the ordering of the PUTs; they
    can be uploaded in parallel and out of order.
    All parts *excluding the final part* must be 5MB or larger.
    Every upload completes with an etag returned

1. Caller completes the operation

        POST /ObjectName?uploadId=UploadId
        <CompleteMultipartUpload>
          <Part><PartNumber>(number)<PartNumber><ETag>(Tag)</ETag></Part>
          ...
        </CompleteMultipartUpload>

    This final call lists the etags of all uploaded parts and the actual ordering
    of the parts within the object.

The completion operation is apparently `O(1)`; presumably the PUT requests
have already uploaded the data to the server(s) which will eventually be
serving up the data for the final path; all that is needed to complete
the upload is to construct an object by linking together the files in
the server's local filesystem can add/update an entry the index table of the
object store.

In the S3A client, all PUT calls in the sequence and the final commit are
initiated by the same process. *This does not have to be the case*.
It is that fact, that a different process may perform different parts
of the upload, which make this algorithm viable.


### Integration with MapReduce


In order to support the ubiquitous `FileOutputFormat` and subclasses,
S3A Committers will need somehow be accepted as a valid committer by the class,
a class which explicity expects the output committer to be `FileOutputCommitter`

```java
public Path getDefaultWorkFile(TaskAttemptContext context,
                               String extension) throws IOException{
  PathOutputCommitter committer =
    (PathOutputCommitter) getOutputCommitter(context);
  return new Path(committer.getWorkPath(), getUniqueFile(context,
    getOutputName(context), extension));
}

```

Here are some options which have been considered, explored and discarded

1. Adding more of a factory mechanism to create `FileOutputCommitter` instances;
subclass this for S3A output and return it. The complexity of `FileOutputCommitter`
and of supporting more dynamic consturction makes this dangerous from an implementation
and maintenance perspective.

1. Add a new commit algorithmm "3", which actually reads in the configured
classname of a committer which it then instantiates and then relays the commit
operations, passing in context information. Ths new committer interface would
add methods for methods and attributes. This is viable, but does still change
the existing Committer code in a way which may be high-maintenance.

1. Allow the `FileOutputFormat` class to take any task/job context committer
which implemented the `getWorkPath()` method —that being the sole
specific feature which it needs from the `FileOutputCommitter`.


Option 3, make `FileOutputFormat` support more generic committers, is the
current design. It relies on the fact that the sole specific method of
`FileOutputCommitter` which `FileOutputFormat` uses is `getWorkPath()`.

This can be pulled up into a new abstract class, `PathOutputCommitter`, which
`FileOutputCommitter` and `S3ACommitter` can implement:

```java
public abstract class PathOutputCommitter extends OutputCommitter {

  /**
   * Get the directory that the task should write results into.
   * @return the work directory
   */
  public abstract Path getWorkPath() throws IOException;
}
```

The sole change needed for `FileOutputFormat`  is to change what it casts
the context committer to:

```java
PathOutputCommitter committer =
  (PathOutputCommitter) getOutputCommitter(context);
```

Provided that `getWorkPath()` remains the sole method which `FileOutputFormat`
uses, these changes will allow an S3A committer to replace the `FileOutputCommitter`,
with minimal changes to the codebase.


### Failure cases

**Network Partitioning**

The job/task commit protocol is expected to handle this with the task
only committing work when the job driver tells it to. A network partition
should trigger the task committer's cancellation of the work (this is a protcol
above the committers).

**Job Driver failure**

The job will be restarted. When it completes it will delete all
outstanding requests to the destination directory which it has not
committed itself.

**Task failure**

The task will be restarted. Pending work of the task will not be committed;
when the job driver cleans up it will cancel pending writes under the directory.

**Multiple jobs targeting the same destination directory**

This leaves things in an inderminate state.


**Failure during task commit**

Pending uploads will remain, but no changes will be visible. 

If the `.pendingset` file has been saved to the job attempt directory, the
task has effectively committed, it has just failed to report to the
controller. This will cause complications during job commit, as there
may be two task pendingset committing the same files, or committing
files with 

*Proposed*: track task ID in pendingsets, recognise duplicates on load
and then respond by cancelling one set and committing the other. (or fail?)

**Failure during job commit**

The destination will be left in an unknown state.

**Failure during task/job abort**

Failures in the abort process are not well handled in either the committers
or indeed in the applications which use these committers. If an abort
operation fails, what can be done?

While somewhat hypothetical for the use case of a task being aborted due
to the protocol (e.g. speculative jobs being aborted), the abort task/abort job
calls may be made as part of the exception handling logic on a failure to commit.
As such, the caller may assume that the abort does not fail: if it does,
the newly thrown exception may hide the original problem.

Two options present themselves

1. Catch, log and swallow failures in the `abort()`
1. Throw the exceptions, and expect the callers to handle them: review, fix
and test that code as appropriate.

Fixing the calling code does seem to be the best strategy, as it allows the
failure to be explictly handled in the commit protocol, rather than hidden
in the committer.::OpenFile

**Preemption**

Preemption is the explicit termination of work at the behest of the cluster
scheduler. It's a failure, but a special one: pre-empted tasks must not be counted
as a failure in any code which only allows a limited number of trackers, and the
Job driver can assume that the task was successfully terminated.

Job drivers themselves may be preempted.


### Testing

This algorithm can only be tested against an S3-compatible object store.
Although a consistent object store is a requirement for a production deployment,
it may be possible to support an inconsistent one during testing, simply by
adding some delays to the operations: a task commit does not succeed until
all the objects which it has PUT are visible in the LIST operation. Assuming
that further listings from the same process also show the objects, the job
committer will be able to list and commit the uploads.

* Single file commit/abort operations can be tested in isolation.
* the job commit protocol including various failure sequences can be explicitly
 executed in a JUnit test suite.
* MiniMRCluster can be used for testing the use in a real MR job, setting
the destination directory to an s3a:// path; maybe even the entire defaultFS to
s3a. This is potentially a slow test; we may need to add a new test profile, `slow`
to only run the slow tests, the way `scale` is used to run the existing (slow)
scalablity tests.
* Downstream applications can have their own tests.


### MRv1 support via `org.apache.hadoop.mapred.FileOutputFormat`

This is going to be fairly complex, in coding and testing



## Integrating with Spark

Spark defines a commit protocol `org.apache.spark.internal.io.FileCommitProtocol`,
implementing it in `HadoopMapReduceCommitProtocol` a subclass `SQLHadoopMapReduceCommitProtocol`
which supports the configurable declaration of the underlying Hadoop committer class,
and the `ManifestFileCommitProtocol` for Structured Streaming. The latter
is best defined as "a complication" —but without support for it, S3 cannot be used
as a reliable destination of stream checkpoints.

One aspect of the Spark commit protocol is that alongside the Hadoop file committer,
there's an API to request an absolute path as a target for a commit operation,
`newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String`;
each task's mapping of temp-> absolute files is passed to the Spark driver
in the `TaskCommitMessage` returned after a task performs its local
commit operations (which includes requesting permission to commit from the executor).
These temporary paths are renamed to the final absolute paths are renamed
in `FileCommitProtocol.commitJob()`. This is currently a serialized rename sequence
at the end of all other work. This use of absolute paths is used in writing
data into a destination directory tree whose directory names is driven by
partition names (year, month, etc).

Supporting that feature is going to be challenging; either we allow each directory in the partition tree to
have its own staging directory documenting pending PUT operations, or (better) a staging directory
tree is built off the base path, with all pending commits tracked in a matching directory
tree.

Alternatively, the fact that Spark tasks provide data to the job committer on their
completion means that a list of pending PUT commands could be built up, with the commit
operations being excuted by an S3A-specific implementation of the `FileCommitProtocol`.
As noted earlier, this may permit the reqirement for a consistent list operation
to be bypassed. It would still be important to list what was being written, as
it is needed to aid aborting work in failed tasks, but the list of files
created by successful tasks could be passed directly from the task to committer,
avoid that potentially-inconsistent list.


#### Spark, Parquet and the Spark SQL Commit mechanism

Spark's `org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat`
Parquet output format wants a subclass of`org.apache.parquet.hadoop.ParquetOutputCommitter`,
the option being defined by the classname in the configuration
key `spark.sql.parquet.output.committer.class`;
this is then patched in to the value `spark.sql.sources.outputCommitterClass`
where it is picked up by `SQLHadoopMapReduceCommitProtocol` and instantiated
as the committer for the work.

This is presumably done so the user has the option of requesting a metadata
summary file by setting the option `"parquet.enable.summary-metadata"`.
Creating the summary file requires scanning every single file in the destination
directory on the job commit, so is *very* expensive, and not something which
we recommend when working with S3.


To use a s3guard committer, it must also be identified as the parquet committer.
The fact that instances are dynamically instantiated somewhat complicates the process.

In early tests; we can switch committers for ORC output without making any changes
to the Spark code or configuration other than configuring the factory
for Path output committers.  For Parquet support, it may be sufficient to also declare
the classname of the specific committer (i.e not the factory).

This is unfortunate as it complicates dynamically selecting a committer protocol
based on the destination filesystem type or any per-bucket configuration. Some
possible solutions are

* Have a dynamic output committer which relays to another `PathOutputCommitter`;
it chooses the actual committer by way of the new factory mechanism.
* Add a new spark output committer.


The short term solution of a dynamic wrapper committer could postpone the need for this.



## Alternate Design, the Netflix "Staging" Committer

Ryan Blue, of Netflix, has submitted an alternate committer, one which has a
number of appealing features

* Doesn't have any requirements of the destination object store, not even
a need for a consistency layer.
* Overall a simpler design.
* Known to work.

The final point is not to be underestimated, especially given the need to
be resilient to the various failure modes which may arise.


These S3 committers work by writing task outputs to a temporary directory on the local FS.
Task outputs are directed to the local FS by `getTaskAttemptPath` and `getWorkPath`.
On task commit, the committers look for files in the task attempt directory (ignoring hidden files).
Each file is uploaded to S3 using the [multi-part upload API](http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html),

It works by writing all data to the local filesystem, uploading as multipart
PUT requests at the end of each task, finalizing the PUT in the job commit.

### Commit logic

The core algorithm is as follows:

1. The destination directory for output (e.g. `FileOutputFormat` and subclasses)
is a local `file://` reference.
1. Task commit initiates the multipart PUT to the destination object store.
1. A list of every pending PUT for task is persisted to a single file
within a consistent, cluster-wide filesystem. For Netflix, that is HDFS.
1. The Standard `FileOutputCommitter` (algorithm 1) is used to manage the commit/abort of these
files. That is: it copies only those lists of files to commit from successful tasks
into a (transient) job commmit directory.
1. The S3 job committer reads the pending file list for every task committed
in HDFS, and completes those put requests.

By using `FileOutputCommmitter` to manage the propagation of the lists of files
to commit, the existing commit algorithm implicitly becomes that defining which
files will be committed at the end of the job.


The Netflix contribution has Hadoop `OutputCommitter` implementations for S3.

There are 3 main classes:
* `S3MultipartOutputCommitter` is a base committer class that handles commit logic. This should not be used directly.
* `S3DirectoryOutputCommitter` for writing unpartitioned data to S3 with conflict resolution.
* `S3PartitionedOutputCommitter` for writing partitioned data to S3 with conflict resolution.

Callers should use `S3DirectoryOutputCommitter` for single-directory outputs,
or `S3PartitionedOutputCommitter` for partitioned data.


These S3 committers work by writing task outputs to a temporary directory on the local FS.
Task outputs are directed to the local FS by `getTaskAttemptPath` and `getWorkPath`.


### Conflict resolution

The single-directory and partitioned committers handle conflict resolution by
checking whether target paths exist in S3 before uploading any data.
There are 3 conflict resolution modes, controlled by setting `fs.s3a.committer.staging.conflict-mode`:

* `fail`: Fail a task if an output directory or partition already exists. (Default)
* `append`: Upload data files without checking whether directories or partitions already exist.
* `replace`: If an output directory exists, delete it so the new data replaces the current content.

The partitioned committer enforces the conflict mode when a conflict is detected with output data, not before the job runs.
Conflict resolution differs from an output mode because it does not enforce the mode when there is no conflict.
For example, overwriting a partition should remove all sub-partitions and data it contains, whether or not new output is created.
Conflict resolution will only replace partitions that have output data.

When the conflict mode is `replace`, conflicting directories are removed during
job commit. Data is only deleted if all tasks have completed successfully.

A UUID that identifies a write is added to filenames that are uploaded to S3.
This allows rolling back data from jobs that fail during job commit (see failure cases below) and avoids
file-level conflicts when appending data to existing directories.


*Note* the checks for existence are made via `S3AFileSystem.getFileStatus()` requests of the destination paths.
Unless the view of the S3 store is consistent, it may be that a newly-deleted object
is still discovered in the probe, so a commit fail, even when there is no longer any actual conflict.

### Performance

Compared to the previous proposal, henceforth the "magic" committer, this
committer, the "staging committer", adds the extra overhead of uploading
each file at the end of every task. This is an `O(data)` operation; it can be
parallelized, but is bounded by the bandwidth from compute node to S3, as
well as the write/IOP capacity of the destination shard of S3. If many tasks
complete at or near the same time, there may be a peak of bandwidth load
slowing down the upload.

Time to commit will be the same, and, given the Netflix committer has already
implemented the paralellization logic here, a time of `O(files/threads)`.

### Resilience

There's already a lot of code in the task and job commits to handle failure.

Any failure in a commit triggers a best-effort abort/revert of the commit
actions for a task/job.

Task commits delegate to the `FileOutputCommitter` to ensure that only one task's
output reaches the job commit.

Similarly, if a task is aborted, temporary output on the local FS is removed.

If a task dies while the committer is running, it is possible for data to be 
eft on the local FS or as unfinished parts in S3.
Unfinished upload parts in S3 are not visible to table readers and are cleaned
up following the rules in the target bucket's life-cycle policy.

Failures during job commit are handled by deleting any files that have already
been completed and aborting the remaining uploads.
Because uploads are completed individually, the files that are deleted were visible to readers.

If the process dies while the job committer is running, there are two possible failures:

1. Some directories that would be replaced have been deleted, but no new data is visible.
2. Some new data is visible but is not complete, and all replaced directories have been removed.
 Only complete files are visible.

If the process dies during job commit, cleaning up is a manual process.
File names include a UUID for each write so that files can be identified and removed.


#### Failure during task execution

All data is written to local temporary files; these need to be cleaned up.

The job must ensure that the local (pending) data is purged. *TODO*: test this


#### Failure during task commit


A process failure during the upload process will result in the
list of pending multipart PUTs to *not* be persisted to the cluster filesystem.
This window is smaller than the entire task execution, but still potentially
significant, at least for large uploads.

Per-file persistence, or incremental overwrites of the upload list may
reduce the problems here, but there would still be a small risk of
an outstanding multipart upload not being recorded

#### Explicit Task abort before task commit.

Task will delete all local data; no uploads will be initiated.

#### Failure to communicate with S3 during data upload

If an upload fails, tasks will

* attempt to abort PUT requests already uploaded to S3
* remove temporary files on the local FS.


#### Explicit Job Abort

All in-progress tasks are aborted and cleaned up. The pending commit data
of all completed tasks can be loaded, the PUT requests aborted.


#### Executor failure before Job Commit

Consider entire job lost; rerun required. All pending requests for the job
will need to be identified and cancelled;

#### Executor failure during Job Commit

PUT requests which have been finalized will be persisted, those which
have not been finalized will remain outstanding. As the data for all the
commits will be in the cluster FS, it will be possible for a cleanup to
load these and abort them.

#### Job failure prior to commit


* Consider the entire job lost.
* Executing tasks will not complete, and in aborting, delete local data.
* Tasks which have completed will have pending commits. These will need
to be identified and cancelled.

#### Entire application failure before any task commit

Data is left on local systems, in the temporary directories.

#### Entire application failure after one or more task commits, before job commit

* A multipart PUT request will be outstanding for every pending write.
* A temporary directory in HDFS will list all known pending requests.

#### Job complete/abort after >1 task failure

1. All pending put data listed in the job completion directory needs to be loaded
and then cancelled.
1. Any other pending writes to the dest dir need to be enumerated and aborted.
This catches the situation of a task failure before the output is written.
1. All pending data in local dirs need to be deleted.

Issue: what about the destination directory: overwrite or not? It could well
depend upon the merge policy.



#### Overall Resilience

1. The only time that incomplete work will appear in the destination directory
is if the job commit operation fails partway through.
1. There's a risk of leakage of local filesystem data; this will need to
be managed in the response to a task failure.
1. There's a risk of uncommitted multipart PUT operations remaining outstanding,
operations which will run up bills until cancelled. (as indeed, so does the Magic Committer).


For cleaning up PUT commits, as well as scheduled GC of uncommitted writes, we
may want to consider having job setup list and cancel all pending commits
to the destination directory, on the assumption that these are from a previous
incomplete operation.

We should adds command to the s3guard CLI to probe for, list and abort pending requests under
a path, e.g. `--has-pending <path>`, `--list-pending <path>`, `--abort-pending <path>`.



### Integration


The initial import will retain access to the Amazon S3 client which can be
obtained from an instance of `S3AFileSystem`, so will share authentication
and other configuration options.

Full integration must use an instance of `S3AFileSystem.WriteOperationHelper`,
which supports the operations needed for multipart uploads. This is critical
to keep ensure S3Guard is included in the operation path, alongside our logging
and metrics.

The committer should be able to persist data via an array'd version of the
single file JSON data structure `org.apache.hadoop.fs.s3a.commit.SinglePendingCommit`.
Serialized object data has too many vulnerabilities to be trusted when someone
party could potentially create a malicious object stream read by the job committer.


### Testing

The code contribution came with a set of mock tests which simulate failure conditions,
as well as one using the MiniMR cluster. This put it ahead of the "Magic Committer"
in terms of test coverage.

Since then the protocol integration test lifted from `org.apache.hadoop.mapreduce.lib.output.TestFileOutputCommitter`
to test various state transitions of the commit mechanism has been extended
to support the variants of the staging committer.

The Mock MR job test was converted to an integration test, executing
a simple MR job in forked processes, using the chosen committer


One feature added during this testing is that the `_SUCCESS` marker file saved is
no-longer a 0-byte file, it is a JSON manifest file, as implemented in
`org.apache.hadoop.fs.s3a.commit.files.SuccessData`. This file includes
the committer used, the hostname performing the commit, timestamp data and
a list of paths committed.

```
SuccessData{
  committer='PartitionedStagingCommitter',
  hostname='devbox.local',
  description='Task committer attempt_1493832493956_0001_m_000000_0',
  date='Wed May 03 18:28:41 BST 2017',
  filenames=[test/testMRJob/part-m-00000, test/testMRJob/part-m-00002, test/testMRJob/part-m-00001]
}
```

This was useful a means of verifying that the correct
committer had in fact been invoked in those forked processes: a 0-byte `_SUCCESS`
marker implied the classic `FileOutputCommitter` had been used; if it could be read
then it provides some details on the commit operation which are then used
in assertions in the test suite.

Without making any stability guarantees, it may be useful to extend this with
more information, including aggregate metrics of work, filesystem metrics, etc.


## Troubleshooting

### `Filesystem does not have support for 'magic' committer`

```
org.apache.hadoop.fs.s3a.commit.PathCommitException: `s3a://landsat-pds': Filesystem does not have support for 'magic' committer enabled
in configuration option fs.s3a.committer.magic.enabled
```

The Job is configured to use the magic committer, but the S3A bucket has not been explicitly
called out as supporting it,

The destination bucket **must** be declared as supporting the magic committer.
 
 
This can be done for those buckets which are known to be consistent, either
because the [S3Guard](s3guard.html) is used to provide consistency,

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.committer.magic.enabled</name>
  <value>true</value>
</property>

```

*IMPORTANT*: only enable the magic committer against object stores which
offer consistent listings. By default, Amazon S3 does not do this -which is
why the option `fs.s3a.committer.magic.enabled` is disabled by default. 

### `Directory for intermediate work cannot be on S3`

`org.apache.hadoop.fs.PathIOException: s3a://landsat-pds/tmp/alice/local-1495211753375/staging-uploads': Directory for intermediate work cannot be on S3`

The Staging committer uses Hadoop's original committer to manage the commit/abort
protocol for the files listing the pending write operations. Tt uses
the cluster filesystem for this. This must be HDFS or a similar distributed
filesystem with consistent data and renames as O(1) atomic renames.
