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


# "Intermediate Manifest" Committer for Azure and GCS

## Problem:

The only committer of work from Spark to Azure ADLS Gen 2 "abfs://" storage
which is safe to use is the "v1 file committer".

This is "correct" in that if a task attempt fails, its output is guaranteed not
to be included in the final out. The "v2" commit algorithm cannot meet that
guarantee, which is why it is no longer the default.

But: it is slow, especially on jobs where deep directory trees of output are used.
Why is it slow? It's hard to point at a particular cause, primarily because of
the lack of any instrumentation in the `FileOutputCommitter`.
Stack traces of running jobs generally show `rename()`, though list operations
do surface too.

On Google GCS, neither the v1 nor v2 algorithm are _safe_ because the google
filesystem doesn't have the atomic directory rename which the v1 algorithm
requires.

A further issue is that both Azure and GCS storage may encounter scale issues
with deleting directories with many descendants.
This can trigger timeouts because the FileOutputCommitter assumes that
cleaning up after the job is a fast call to `delete("_temporary", true)`.

## Solution.

The _Intermediate Manifest_ committer is a new committer for
work which should deliver performance on ABFS
for "real world" queries, and performance and correctness on GCS.

This committer uses the extension point which came in for the S3A committers.
Users can declare a new committer factory for abfs:// and gcs:// URLs.
A suitably configured spark deployment will pick up the new committer.

Directory performance issues in job cleanup can be addressed by two options
1. The committer can be configured to move the temporary directory under `~/.trash`
in the cluster FS. This may benefit azure, but will not benefit GCS.
1. The committer will parallelize deletion of task attempt directories before
   deleting the `_temporary` directory.
   This is highly beneficial on GCS; may or may not be beneficial on ABFS.
   (More specifically: use it if you are using OAuth to authenticate).

Suitably configured MR and Spark deployments will pick up the new committer.

The committer can be used with any filesystem client which has a "real" file rename.





# How it works

The full details are covered in [Manifest Committer Architecture](manifest_committer_architecture.html).

#### Switching to the committer

The hooks put in to support the S3A committers were designed to allow every
filesystem schema to provide their own committer.
See [Switching To an S3A Committer](../../hadoop-aws/tools/hadoop-aws/committers.htmml#Switching_to_an_S3A_Committer)
https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/c

A factory for the abfs schema would be defined in
`mapreduce.outputcommitter.factory.scheme.abfs` ; and a similar one for `gcs`.

Some matching spark configuration changes, especially for parquet binding, will be required.
These can be done in
`core-site.xml` or `spark-default`

```
spark.sql.parquet.output.committer.class
    org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
spark.sql.sources.commitProtocolClass
    org.apache.spark.internal.io.cloud.PathOutputCommitProtocol

```

## Verifying that the committer was used

The new committer will write a JSON summary of the operation, including statistics, in the `_SUMMARY` file.

If this file exists and is zero bytes long: the classic `FileOutputCommitter` was used.

If this file exists and is greater than zero bytes wrong, either the manifest committer was used,
or in the case of S3A filesystems, one of the S3A committers. They all use the same JSON format.


