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

# Work in Progress: S3A Committer

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->
This page covers the S3A Committer, which can commit work directly
to an S3 object store which supports consistent metadata.

## Usage: TODO

## Terminology

* *Job*: a potentially parallelized query/operation to execute. The execution
of a job: the division of work into tasks and the management of their completion,
is generally executed in a single process.

The output of a Job is made visible to other stages in a larger operation
sequence or other applications if the job *completes successfully*.

* *Job Driver*. Not sure quite what term to use here. Whatever process schedules
task execution, tracks success/failures and, determines when all the work has been
processed and then commits the output. It may also determine that a job
has failed and cannot be recovered, in which case the job is aborted.
In MR and Tez, this is inside the YARN application master.
In Spark it is the driver, which can run in the AM, the YARN client, or other
places (e.g Livy?).

* *Final directory*: the directory into which the output of a job is placed
so as to be visible.

* *Task* a single operation within a job, on a single process, one which generates
one or more files.
After a successful job completion, the data MUST be visible in the final directory.
A task completes successfully if it generates all the output it expects to without
failing in some way (error in processing; network/process failure).

* *Job Context* an instance of the class `org.apache.hadoop.mapreduce.JobContext`,
which provides a read-only view of the Job for the Job Driver and tasks.

* *Task Attempt Context* an instance of the class
`org.apache.hadoop.mapreduce.TaskAttemptContext extends JobContext, Progressable`,
which provides operations for tasks, such as getting and setting status,
progress and counter values.

* *Task Working Directory*: a directory for exclusive access by a single task,
into which uncommitted work may be placed.

* *Task Commit* The act of taking the output of a task, as found in the
Task Working Directory, and making it visible in the final directory.
This is traditionally implemented via a `FileSystem.rename()` call.

  It is useful to differentiate between a *task-side commit*: an operation performed
  in the task process after its work, and a *driver-side task commit*, in which
  the Job driver perfoms the commit operation. Any task-side commit work will
  be performed across the cluster, and may take place off the critical part for
  job execution. However, unless the commit protocol requires all tasks to await
  a signal from the job driver, task-side commits cannot instantiate their output
  in the final directory. They may be used to promote the output of a successful
  task into a state ready for the job commit, addressing speculative execution
  and failures.

* *Job Commit* The act of taking all successfully completed tasks of a job,
and committing them. This process is generally non-atomic; as it is often
a serialized operation at the end of a job, its performance can be a bottleneck.

* *Task Abort* To cancel a task such that its data is not committed.

* *Job Abort* To cancel all work in a job: no task's work is committed.

* *Speculative Task Execution/ "Speculation"* Running multiple tasks against the same
input dataset in parallel, with the first task which completes being the one
which is considered successful. Its output SHALL be committed; the other task
SHALL be aborted. There's a requirement that a task can be executed in parallel,
and that the output of a task MUST NOT BE visible until the job is committed,
at the behest of the Job driver. There is the expectation that the output
SHOULD BE the same on each task, though that MAY NOT be the case. What matters
is if any instance of a speculative task is committed, the output MUST BE
considered valid.

There is an expectation that the Job Driver and tasks can communicate: if a task
perform any operations itself during the task commit phase, it shall only do
this when instructed by the Job Driver. Similarly, if a task is unable to
communicate its final status to the Job Driver, it MUST NOT commit is work.
This is very important when working with S3, as some network partitions could
isolate a task from the Job Driver, while the task retains access to S3.


## Problem: Efficient, reliable commits of work to consistent S3 buckets


The standard commit algorithms (the `FileOutputCommitter` and its v1 and v2 algorithms)
rely on directory rename being an `O(1)` atomic operation: callers output their
work to temporary directories in the destination filesystem, then
rename these directories to the final destination as way of committing work.
This is the perfect solution for commiting work against any filesystem,
implementing the full semantics of the `FileSystem.rename()` command.
Using rename allows individual tasks to work in temporary directories, with the
rename as the atomic operation can be used to explicitly commit tasks and
ultimately the entire job. Because the cost of the rename is low, it can be
performed during task and job commits with minimal delays. Note that HDFS
will lock the namenode metadata during the rename operation, so all rename() calls
will be serialized. However, as they only update the metadata of two directory
entries, the duration of the lock is low.

In contrast to a "real" filesystem, Amazon's S3A object store, similar to
most others, does not support `rename()` at all. A hash operation on the filename
determines the location of of the data —there is no separate metadata to change.
To mimic renaming, the Hadoop S3A client has to copy the data to a new object
with the destination filename, then delete the original entry. This copy
can be executed server-side, but as it does not complete until the in-cluster
copy has completed, it takes time proportional to the amount of data.

### Hadoop MR Commit algorithm "1"


The "v1" MR commit algorithm is the default commit algorithm in Hadoop 2.x;
it was implemented as part of [MAPREDUCE-2702](https://issues.apache.org/jira/browse/MAPREDUCE-2702).

This algorithm is designed to handle a failure and restart of the Job driver,
with the restarted job driver only rerunning the incomplete tasks; the
output of the completed tasks is recovered for commitment when the restarted
job completes.

The

### Hadoop MR Commit algorithm "2"



### Requirements of an S3A Committer

1. Support S3 as a reliable direct destination of work through the S3A filesystem
   client.
1. Efficient: implies no rename, and a minimal amount of delay in the job driver's
task and job commit phases,
1. Support task failure and speculation.
1. Can be used by existing code: Hadoop MapReduce, Spark, Hive.
1. Retrofittable to existing subclasses of FileOutputFormat and/or compatible
with committers which expect a specific FileOutputFormat.


### Features of S3 and the S3A Client


A core problem is that
[object stores are not filesystems](../../../hadoop-project-dist/hadoop-common/filesystem/introduction.html);
how `rename()` has been emulated in the S3A client means that both the existing
MR committer algorithms have significant performance problems.

1. Single-object renames are implemented as a copy and delete sequence.
1. COPY is atomic, but overwrites cannot be prevented.
1. Amazon S3 is eventually consistent on listings, deletes and updates.
1. Amazon S3 has create consistency, however, the negative response of a HEAD/GET
performed on a path before an object was created can be cached, unintentionally
creating a create inconsistency. The S3A client library does perform such a check,
on `create()` and `rename()` to check the state of the destination path, and
so, whether the operation is permitted.
1. multi-object renames are sequential or parallel single object COPY+DELETE operations:
non atomic, `O(data)` and, on failure, can leave the filesystem in an unknown
state.
1. There is a PUT operation, capable of uploading 5GB of data in one HTTP request.
1. The PUT operation is atomic, but there is no PUT-no-overwrite option.
1. There is a multipart POST/PUT sequence for uploading larger amounts of data
in a sequence of PUT requests.


The Hadoop S3A Filesystem client supports PUT and multipart PUT for uploading
data, with the `S3ABlockOutputStream` of HADOOP-13560 uploading written data
as parts of a multipart PUT once the threshold set in the configuration
parameter `fs.s3a.multipart.size` (default: 100MB).

The S3Guard work, HADOOP-13345, adds a consistent view of the filesystem
to all processes using the shared DynamoDB table as the authoritative store of
metadata. Other implementations of the S3 protocol are fully consistent; the
proposed algorithm is designed to work with such object stores without the
need for any DynamoDB tables.

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

### Related work: Spark's `DirectOutputCommitter`

One implementation to look at is the
[`DirectOutputCommitter` of Spark 1.6](https://github.com/apache/spark/blob/branch-1.6/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/DirectParquetOutputCommitter.scala).

This implements a zero rename commit by subclassing the `ParquetOutputCommitter` and

1. Returning the final directory as the task working directory.
1. Subclassing all the task commit/abort operations to be no-ops.

With the working directory as the destination directory, there is no need
to move/rename the task output on a successful commit. However, it is flawed.
There is no notion of "committing" or "aborting" a task, hence no ability to
handle speculative execution or failures. This is why the committer
was removed from Spark 2 [SPARK-10063](https://issues.apache.org/jira/browse/SPARK-10063)

There is also the issue that work-in-progress data is visible; this may or may
not be a problem.

## Proposed zero rename algorithm


Our proposal for commiting work without rename is: delayed completion of
multi-part PUT operations

That is: tasks write all data as multipart uploads, *but delay the final
commit action until until the final, single job commit action.* Only that
data committed in the job commit action will be made visible; work from speculative
and failed tasks will not be instiantiated. As there is no rename, there is no
delay while data is copied from a temporary directory to the final directory.
The duration of the commit will be the time needed to determine which commit operations
to construct, and to execute them.



#### The workers upload the data —but the job committer finalizes all uploads

This is the key point of the algorithm. The data is uploaded, awaiting
instantiation, but it doesn't appear in the object store until the final
job commit operation completes the outstanding multipart uploads. At this
point the new files become visible, which each objects instantiation
being atomic.




# Core feature: A new/modified output stream for delayed PUT commits


This algorithm implies a new/modified S3A Output stream, which, rather
than commit any active multipart upload in the final `close()` operation,
it must instead save enough information into the S3 repository for an independent
process to be able to complete or abort the upload.

`S3ABlockOutputStream` will need to be modified/extended to support a different
final action in the `close()` operation, and

Currently, in `close()`, it chooses whether to perform a single PUT or to
complete an ongoing multipart write.

If a multipart PUT is in progress, then the stream waits for the ongoing uploads
to complete (including any final block submitted), and then builds and PUTs
the final multipart commit operation. The list of parts (and their ordering)
has been built up during the opt

In contrast, when writing to a delayed-commit file

1. A multipart write MUST always be initiated, even for small writes. This write
MAY be initiated during the creation of the stream.

1. Instead of committing the write in the `close()` call, perform a PUT to
a path in the S3A repository with all the information needed to commit the operation.
That is: the final path, the multipart upload ID, and the ordered list of etags
for the uploaded parts.

Supporting code will read in this data in order to abort or commit the write.


Recognising when a file is "special" is problematic; the normal `create(Path, Boolean)`
call must recognize when the file being created is to be a delayed-commit file,
so returning the special new stream.

For this we propose


1. A "Special" temporary directory name, `__magic`, to indicate that all files
created under this path are that, "ending commits". Directories created under
the path will still be created —this allows job and task specific directories to
be created for individual job and task attempts.
For example, the pattern `__magic/${jobID}/${taskId}` could be used to
store pending commits to the final directory for that specific task. If that
task is committed, all pending commit files stored in that path will be loaded
and used to commit the final uploads.

In the latter example, consider a job with the final directory `/results/latest`

1. The intermediate directory for the task 01 attempt 01 of job `job_400_1` would be

        /results/latest/__magic/job_400_1/_task_01_01

    This would be returned as the temp directory.

1. When a client attempted to create the file
`/results/latest/__magic/job_400_1/task_01_01/latest.orc.lzo` , the S3A FS would initiate
a multipart request with the final destination of `/results/latest/latest.orc.lzo`.

1. As data was written to the output stream, it would be incrementally uploaded as
individual multipart PUT opreations

1. On `close()`, summary data would be written to the file
`/results/latest/__magic/job400_1/task_01_01/latest.orc.lzo.pending`.  This would list: upload Id
and all the parts and etags of uploaded data.

1. The task commit operation would do nothing.

1. The job commit operation would identify all `__magic_` directories
under the destination directory.

1. Those directories of successful task attempts would have their contents
read, and for each file with summary data, the multipart upload completed.

1. Those associated with failed/incomplete task attempts would have the
summary data of their written parts used to manage the abort
of the associated multipart puts and the pending data.

It should also be possible to support a full directory tree of generated data.
Paths would just be created under the temporary directory to match
that of the final data; `/results/latest/__magic/job_400/task_01_01/2017/2017-01-01.orc.lzo.pending`
would be mapped to a final path of `/results/latest/2017/2017-01-01.orc.lzo`.

(Why have a new suffix, `.pending` rather than just use the original filename?

1. This avoids anything getting confused between the commit data and the actual data.
1. It guarantees that any attempt to query or read the path written to will raise
a `FileNotFoundException` immediately (assuming this is what we want).
1. If any actual files somehow end up in the directory (e.g through a rename) then
they can be identified. (maybe the task committer should check for that and signal a problem)


**Issues**

* What if there are some non-.pending files in the task directory?
* What if the multipart commit fails?

### Aborting a task

When a task is aborted, all of its pending commits must be cancelled.

1. List all pending commit files for that task.
1. Cancel them.
1. Delete the commit files.
1. Delete the directory (for completeness).

**Issues**

* What if there are some non-.pending files in the task directory?
* What if the multipart abort fails?

### Aborting a job

Aborting a job is similar to aborting a task:

1. Enumerate all task directories under a job directory.
1. Abort all pending commits in them.
1. delete the pending directory for that job.

### Cleaning up after complete job failure

One failure case is that the entire execution framework failed; a new process
must identify outstanding jobs with pending work, and abort them, then delete
the appropriate `__magic` directories.

This can be done either by scanning the directory tree for `__magic` directories
and scanning underneath them, or by using the `listMultipartUploads()` call to
list multipart uploads under a path, then cancel them. The most efficient solution
may be to use `listMultipartUploads` to identify all outstanding request, and use that
to identify which requests to cancel, and where to scan for `__magic` directories.
This strategy should address scalability problems when working with repositories
with many millions of objects —rather than list all keys searching for those
with `/__magic/**/*.pending` in their name, work backwards from the active uploads to
the directories with the data.

We may also want to consider having a cleanup operationn in the S3 CLI to
do the full tree scan and purge of pending items; give some statistics on
what was found. This will keep costs down and help us identify problems
related to cleanup.

### Performance

The time to upload would be that of today's block upload (`s3a.fast.upload=true`)
output stream; ongoing through the write, and in the `close()` operation,
a delay to upload any pending data and await all outstanding uploads to complete.
There wouldn't be any overhead of the final completion request. If no
data had yet been uploaded, the `close()` time would be that of the initiate
multipart request and the final put. This could perhaps be simplified by always
requesting a multipart ID on stream creation.

The time to commit will be `O(files)`:

* An `O(children/5000)` bulk listing of the destination directory will enumerate all
children of all pending commit directories.
(If there were many other child entries of the same destination directory, costs
would be higher).

* Every file to be committed will require a GET of the summary data, and a POST
of the final commit. This could be parallelized.

* Similarly, every task being aborted will require a GET and abort request.


Note that it is the bulk listing of all children which is where full consistency
is required. If instead, the list of files to commit could be returned from
tasks to the job committer, as the Spark commit protocol allows, it would be
possible to commit data to an inconsistent object store.

### Cost

Uncommitted data in an incomplete multipart upload is billed at the storage
cost of the S3 bucket. To keep costs down, outstanding data from
failed jobs must be deleted. This can be done through S3 bucket lifecycle policies,
or some command tools which we would need to write.

### Limitations of this algorithm

1. Files will not be visible after the `close()` call, as they will not exist.
Any code which expected pending-commit files to be visible will fail.
1. Failures of tasks and jobs will leave outstanding multipart PUT blocks. These
will need to be garbage collected. S3 now supports automated cleanup; S3A has
the option to do it on startup, and we plan for the `hadoop s3` command to
allow callers to explicitly do it. If tasks were to explicitly write the upload
ID of writes as a write commenced, cleanup by the job committer may be possible.
1. The time to write very small files may be higher than that of PUT and COPY.
We are ignoring this problem as not relevant in production; any attempt at optimizing
small file operations will only complicate development, maintenance and testing.
1. The files containing temporary information could be mistaken for actual
data.
1. It could potentially be harder to diagnose what is causing problems. Lots of
logging can help, especially with debug-level listing of the directory structure
of the temporary directories.
1. The committer will need access to low-level S3 operations. It will need
to be implemented inside the hadoop-aws library, with methods explicitly
exposed for it.
1. If a new Spark committer is also added, that will need access to the same
private/unstable methods, and be very brittle against Hadoop versions.
1. To reliably list all PUT requests outstanding, we need list consistency
In the absence of a means to reliably identify when an S3 endpoint is consistent, people
may still use eventually consistent stores, with the consequent loss of data.
1. If there is more than one job simultaneously writing to the same destination
directories, the output may get confused. This appears to hold today with the current
commit algorithms.
1. It would be possible to create >1 client writing to the same destination
file within the same S3A client/task, either sequentially or in parallel.
1. Even with a consistent metadata store, if a job overwrites existing
files, then old data may still be visible to clients reading the data, until
the update has propagated to all replicas of the data.
1. If the operation is attempting to completely overwrite the contents of
a directory, then it is not going to work: the existing data will not be cleaned
up. A cleanup operation would need to be included in the job commit, deleting
all files in the destination directory which where not being overwritten.
1. It requires a path element, such as `__magic` which cannot be used
for any purpose other than for the storage of pending commit data.
1. Unless extra code is added to every FS operation, it will still be possible
to manipulate files under the `__magic` tree. That's not necessarily bad.
1. As written data is not materialized until the commit, it will not be possible
for any process to read or manipulated a file which it has just created.
1. As written data is not materialized until the commit, if a task
fails before the `close()` operation, there will be nothing to recover.
This is the exact outcome that surfaces when writing to S3A today, so it
cannot be described as a regression.



## Implementation


There's a foundational body of code needed to save the multipart put information
to the objects store, then read it in to commit or cancel the upload.
This can be aggregated to form the commit operation of a task, then finally
the commit operation of an entire job.

These commit operations can then be used in a committer which can be declared
as the committer for work. That committer could either be a whole new committer,
or one which somehow modifies the standard `FileOutputCommitter` to write
data this way.

### Changes to `S3ABlockOutputStream`

We can avoid having to copy and past the `S3ABlockOutputStream` by
having it take some input as a constructor parameter, say a
`OutputUploadTracker` which will be called at appropriate points.

* Initialization, returning a marker to indicate whether or not multipart
upload is commence immediately.
* Multipart PUT init.
* Single put init (not used in this algorithm, but useful for completeness).
* Block upload init, failure and completion (from the relevant thread).
* `close()` entered; all blocks completed —returning a marker to indicate
whether any outstanding multipart should be committed.
* Multipart abort in `abort()` call (maybe: move core logic elsewhere).

The base implementation, `DefaultUploadTracker` would do nothing
except declare that the MPU must be executed in the `close()` call.

The S3ACommitter version, `S3ACommitterUploadTracker` would
1. Request MPU started during init.
1. In `close()` operation stop the Blockoutput stream from committing
the upload -and instead save all the data required to commit later.

### Changes to `S3AFileSystem`

* Export low level PUT, DELETE, LIST, GET commands for upload tracker & committer,
with some added instrumentation to count direct invocation of these.
* Pass down `UploadTracker` to block output stream, normally `DefaultUploadTracker` .
* If support for S3A Commit is enabled (default?), then if a matching path
is identified, determine final path and temp dir (some magic is going to
be needed here); instantiate `S3ACommitterUploadTracker` with destination
information.

### New class:`FileCommitActions`

This is created, bonded to an S3FS instance and is used to commit a single
file. Having it standalone allows for isolated testing of functionality
and failure resilience

**`commitPendingFile(Path)`**

1. Takes a working file path under `__magic`,
1. verifies its referring to an MPU,
1. lists and reads in all info on the MPU commit
1. builds final commit, submits it,
1. DELETE working data

What to do if the upload ID is unknown? Fail the commit?

**`abortPendingFile(Path)`**

Cancels the MPU of a file. Reads in a path, gets the MPU ID, cancels
maybe also: calculate final destination, enum the list of MPUs pending
there and verify that the one about to be cancelled exists/is for it.

Maybe/maybe not delete the file entry.

It must not be an error if an abort fails because the upload ID is not
known.


**`commitAllPendingFilesInPath(Path pendingDir, boolean recursive)`**

Enumerate all entries in a directory/directory tree: commits them.

Because of the risk of multiple job/task attempts in the `__magic` tree,
this should no be called directly against that tree, only against
those tasks known to have complete

**`abortAllPendingFilesInPath(Path pendingDir, boolean recursive)`**

Enumerate all entries in a directory/directory tree: abort the MPUs, then
delete the entries in the path.

### New class:`S3AJobCommitter`

Commit the output of all tasks which are known to have completed, aborts
those which have not. It is the core of the commit process.

If a job is committed, it must commit the output of all tasks which have completed.

Those tasks known to have failed will be aborted

Presumably it will have to abort those tasks whose state is unknown: they
must be considered failures.

### New class:`S3AJobAborter`

Abort all tasks of a job, using `S3ATaskDirectoryAborter` in series or
parallel.


### class `S3AOutputCommitter extends PathOutputCommitter`

This is the class which will use the S3A task and job commit/aborter classes
to manage the task and job output commitment process. It is the integration
point with the Hadoop MR framework.


### Integration with Hadoop and Spark code.


In order to support the ubiquitous `FileOutputFormat` and subclasses,
the S3A Committer will need somehow be accepted as a valid committer by the class,
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

**Job Recovery**

**Network Partitioning**

**Job Driver failure**

**Task failure**

**Multiple jobs targeting the same destination directory**

**Failure during task commit**

**Failure during job commit**

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

This is going to have to be treated similarly to the MRv2 version.

Specifically, an MRv1 equivalent of `PathOutputCommitter` will be needed,
with `getTaskOutputPath()` returning a path in the `__magic` directory,
and `mapred.FileOutputFormat` casting to that class to access.

This will also need an S3a equivalent of `org.apache.hadoop.mapred.FileOutputCommitter`,
one which forwards to the MRv2 committer.

Given the MRv1 implementation wraps and forwards to its MRv2 equivalent,
it is clear that the MRv2 commit sequence is compatible with the MRv1 sequence,
it is merely a matter of bridging the classes.
We plan to wait until the MRv2 implementation is working before starting this retrofitting
process, to simplify the development.


### Integrating with Spark

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

#### Outstanding issues


**Name of pending directory**

The design proposes the name `__magic` for the directory. HDFS and
the various scanning routines always treat files and directories starting with `_`
as temporary/excluded data.

There's another option, `_temporary`, which is used by `FileOutputFormat` for its
output. If that was used, then the static methods in `FileOutputCommitter`
to generate paths, for example `getJobAttemptPath(JobContext, Path)` would
return paths in the pending directory, so automatically be treated as
delayed-completion files.

**Subdirectories of a pending directory**

It is legal to create subdirectories in a task work directory, which
will then be moved into the destination directory, retaining that directory
tree.

That is, a if the task working dir is `dest/__magic/app1/task1/`, all files
under `dest/__magic/app1/task1/part-0000/` must end up under the path
`dest/part-0000/`.

This behavior is relied upon for the writing of intermediate map data in an MR
job.

This means it is not simply enough to strip off all elements of under `__magic`,
it is critical to determine the base path.

Proposed: use the special name `__base` as a marker of the base element for
committing. Under task attempts a `__base` dir is created and turned into the
working dir. All files created under this path will be committed to the destination
with a path relative to the base dir.

More formally: the last parent element of a path which is `__base` sets the
base for relative paths created underneath it.


## Alternate Design, the Netflix "Staging" Committer

Ryan Blue, of Netflix, Submitted an alternate committer, one which has a
number of appealing features

* Doesn't have any requirements of the destination object store, not even
a need for a consistency layer.
* Overall a simpler design.
* Known to work.

The final point is not to be underestimated, especially given the need to
be resilient to the various failure modes which may arise.

It works by writing all data to the local filesystem, uploading as multipart
PUT requests at the end of each task, finalizing the PUT in the job commit.

The core algorithm is as follows:

1. The destination directory for output (e.g. `FileOutputFormat` and subclasses)
is a local file:// reference.
1. Task commit initiates the multipart PUT to the destination object store.
1. A list of every pending PUT for task is persisted to a single file
within a consistent, cluster-wide filesystem. For Netflix, that is HDFS.
1. The job committer reads the pending file list for every committed task, and
finalizes the commits.


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

As far as the original code is concerned. the failures which can arise are

#### Failure during task execution

All data is written to local temporary files;
these need to be cleaned up.

#### Failure during task commit

A task failure during the upload process will result in the 
list of pending multipart PUTs to *not#### be persisted to the cluster filesystem.
This window is smaller than the entire task execution, but still potentially
significant, at least for large uploads. If the data for each pending PUT
were to be saved to a separate file, then they would be discoverable and more
easily cancelled. This would still leave the incompleted uploads still in
progress at the time of the PUT.

#### Explicit Task abort

Task will delete all local data, rather than upload to the object store.

#### Failure to communicate with S3 during data upload

If the upload to S3 fails for one or more files, and retries do not succeed,
then a best-effor cleanup could ber attempted for any successful S3 PUTs; 
all local data call be deleted. It's hard to envisage a failure in PUTs which
do not also translate into a failure of DELETE operations.

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
incomplete operation. (It may seem pessimistic to assume a previous failure, but
this is why PCs always turn their fan on on booting: they assume they crashed
and need to cool the CPUs down).


### Integration


The initial import will retain access to the Amazon S3 client which can be
obtained from an instance of `S3AFileSystem`, so will share authentication
and other configuration options.
 
Full integration must use an instance of `S3AFileSystem.WriteOperationHelper`,
which supports the operations needed for multipart uploads. This is critical
to keep ensure S3Guard is included in the operation path, alongside our logging
and metrics.

The committer should be able to persist data via an array'd version of the
single file JSON data structure `org.apache.hadoop.fs.s3a.commit.PersistentCommitData`.
Serialized object data has too many vulnerabilities to be trusted when someone
party could potentially create a malicious object stream read by the job committer.


### Testing

The code contribution already has a set of mock tests which simulate failure conditions,
as well as one using the MiniMR cluster. This puts it ahead of the "Magic Committer"
in terms of test coverage.

We can extend the protocol integration test lifted from `org.apache.hadoop.mapreduce.lib.output.TestFileOutputCommitter`
to test various state transitions of the commit mechanism.

No doubt we will need to implement some more integration tests; as usual a focus
on execution time and cost and cost will be constraints.

 
