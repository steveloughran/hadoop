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

# S3A Committer Architecture

<!-- DISABLEDMACRO{toc|fromDepth=0|toDepth=5} -->

This document covers the architecture and implementation details of the S3A committers.


## Problem: Efficient, reliable commits of work to consistent S3 buckets


The standard commit algorithms (the `FileOutputCommitter` and its v1 and v2 algorithms)
rely on directory rename being an `O(1)` atomic operation: callers output their
work to temporary directories in the destination filesystem, then
rename these directories to the final destination as way of committing work.
This is the perfect solution for commiting work against any filesystem with
consistent listing operations and where the `FileSystem.rename()` command
is an atomic `O(1)` operation.

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

The rename overhead is the most visible issue, but it is not the most dangerous.
That is the fact that path listings have no consistency guarantees, and may
lag the addition or deletion of files.
If files are not listed, the commit operation will *not* copy them, and
so they will not appear in the final output.



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

## The execution workflow


**setup**:

* A job is created, assigned a Job ID (YARN?).
* For each attempt, and attempt ID is created, to build the job attempt ID.
* `Driver`: a `JobContext` is created/configured
* A committer instance is instantiated with the `JobContext`; `setupJob()` invoked.


## The `FileOutputCommitter`

The standard commit protocols are implemented in `org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter`.

These offer

### Hadoop MR Commit algorithm "1"


The "v1" MR commit algorithm is the default commit algorithm in Hadoop 2.x;
it was implemented as part of [MAPREDUCE-2702](https://issues.apache.org/jira/browse/MAPREDUCE-2702).

This algorithm is designed to handle a failure and restart of the Job driver,
with the restarted job driver only rerunning the incomplete tasks; the
output of the completed tasks is recovered for commitment when the restarted
job completes.


### Hadoop MR Commit algorithm "2"


TBD



### Hadoop MRv1 Protocol

Adding support for the original Hadoop MRv1 Protocos would take a lot of effort.


### Requirements of an S3A Committer

1. Support an eventually consistent S3 object store as a reliable direct
destination of work through the S3A filesystem client.
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

## Proposed zero rename algorithm, "The Magic Committer"

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

For this we have

A "Special" temporary directory name, `__magic`, to indicate that all files
created under this path are that, "ending commits". Directories created under
the path will still be created —this allows job and task specific directories to
be created for individual job and task attempts.
For example, the pattern `__magic/${jobID}/${taskId}` could be used to
store pending commits to the final directory for that specific task. If that
task is committed, all pending commit files stored in that path will be loaded
and used to commit the final uploads.

In the latter example, consider a job with the final directory `/results/latest`

 The intermediate directory for the task 01 attempt 01 of job `job_400_1` would be

    /results/latest/__magic/job_400_1/_task_01_01

This would be returned as the temp directory.

When a client attempted to create the file
`/results/latest/__magic/job_400_1/task_01_01/latest.orc.lzo` , the S3A FS would initiate
a multipart request with the final destination of `/results/latest/latest.orc.lzo`.

 As data was written to the output stream, it would be incrementally uploaded as
individual multipart PUT operations

 On `close()`, summary data would be written to the file
`/results/latest/__magic/job400_1/task_01_01/latest.orc.lzo.pending`.
This would contain the upload ID and all the parts and etags of uploaded data.


#### Task commit

The information needed to commit a task is moved from the task attempt
to the job attempt.

1. The task commit operation lists all `.pending` files in its attempt directory.
1. The contents are loaded into a list of single pending uploads.
1. These are merged into to a single `Pendingset` structure.
1. Which is saved to a `.pendingset` file in the job attempt directory.
1. Finally, the task attempt directory is deleted. In the example, this
would be to `/results/latest/__magic/job400_1/task_01_01.pendingset`; 


A failure to load any of the single pending upload files (i.e. the file
could not load or was considered invalid, the task is considered to
have failed. All successfully loaded pending commits will be aborted, then
the failure reported.

Similarly, a failure to save the `.pendingset` file will trigger an
abort of all its pending uploads.


#### Job Commit

The job committer will load all `.pendingset` files in its job
attempt directory.

A failure to load any of these files is considered a job failure; all
pendingsets which could be loaded will be aborted.

If all pendingsets were loaded, then every
pending commit in the job will be committed. If any one of these commits
failed, then all successful commits will be reverted by deleting the destination
file.

#### Supporting directory trees

To allow tasks to generate data in subdirectories, a special filename `__base`
will be used to provide an extra cue as to the final path. When mapping an output
path  `/results/latest/__magic/job_400/task_01_01/__base/2017/2017-01-01.orc.lzo.pending`
to a final destination path, the path will become `/results/latest/2017/2017-01-01.orc.lzo`.
That is: all directories between `__magic` and `__base` inclusive will be ignored.


**Issues**

Q. What if there are some non-`.pending` files in the task attempt directory?

A. This can only happen if the magic committer is being used in an S3A client
which does not have the "magic path" feature enabled. This will be checked for
during job and task committer initialization.


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

The time to commit will be `files/threads`:

* An `O(children/5000)` bulk listing of the destination directory will enumerate all
children of all pending commit directories.
(If there were many other child entries of the same destination directory, costs
would be higher).

* Every file to be committed will require a GET of the summary data, and a POST
of the final commit. This is parallelized.

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

1. Failures of tasks and jobs will leave outstanding multipart uploads. These
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

1. To reliably list all PUT requests outstanding, we need list consistency
In the absence of a means to reliably identify when an S3 endpoint is consistent, people
may still use eventually consistent stores, with the consequent loss of data.

1. If there is more than one job simultaneously writing to the same destination
directories, the output may get confused. This appears to hold today with the current
commit algorithms.

1. It is possible to create more than one client writing to the
same destination file within the same S3A client/task, either sequentially or in parallel.

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
to manipulate files under the `__magic` tree. That's not bad, it just potentially
confusing.

1. As written data is not materialized until the commit, it will not be possible
for any process to read or manipulated a file which it has just created.


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
