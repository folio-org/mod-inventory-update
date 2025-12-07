## Using MIU for importing to inventory storage (WiP, actual implementation is both ahead of and behind this documentation)

The importing component of MIU consists of 0 or more import "channels" equipped with file queues and processing pipelines.

The only existing pipeline implementation is an XSLT transformation pipeline, that takes an XML 'collection' of 'record'
elements and -- through one or more custom written XSLT style-sheets -- transforms them into "inventory XML" that can be converted
to batches of inventory upsert compatible JSON records and processed through MIU's inventory upsert APIs.

## The importing component

The main elements of the importing component are

  - a "channel" with an associated file queue
  - a processing pipeline, called a "transformation"
  - the "transformation"  has an ordered set of transformation style-sheets, called "steps"
  - the "transformation" also has a "target" for its results, which is a component that will persist the results to inventory storage

### Channels

The static parts of the channel itself are
  - a name for channel
  - a reference to the "transformation" that processes the incoming source files
  - two flags that indicates if the channel is deployed (or is available for deployment), and is actively listening

The dynamic parts of a channel are

  - a dedicated process (a worker verticle in Vert.x terms) that listens for incoming files
  - file queue: a set of filesystem directories that acts as a queue for incoming files
  - "importJob":  if there are any incoming files, and the channel is actively listening, it will automatically launch an "import job",
    using its associated "transformation", and the progress of that process will be logged in the objects "importJob",
    "logLines", and "failedRecords"
  - when the last file in the queue is processed, the job will finish, but the channel will keep listening
    until paused or decommissioned or until MIU is uninstalled or redeployed.

#### Requests operating on channels:

- POST `/inventory-import/channels` create a channel
- POST `/inventory-import/channels/<channel id>/commission`    launch a channel that is actively listening for source files to import
- POST `/inventory-import/channels/<channel id>/listen`        listen for source files in queue
- POST `/inventory-import/channels/<channel id>/pause-listen`  ignore source files in queue
- POST `/inventory-import/channels/<channel id>/decommission`  undeploy (disable) the channel
- PUT `/inventory-import/channels/<channel uuid>`                update properties of a channel
- POST `/inventory-import/channels/<channel id>/upload`        pushes a source file to the channel
- POST `/inventory-import/channels/<channel id>/init-queue'    deletes all the source files in a queue (or re-establishes an empty queue structure, in case the previous queue was deleted directly in the file system for example).
- DELETE `/inventory-import/channels/<channel uuid>` delete the channel configuration including the file queue but not its job history

All these operate on a single channel. There are two more requests that operates on multiple channels. When a module is
redeployed non of the channels are automatically deployed. The operator can choose to one of two operations after deploying the module:

The `../upload` API will accept source files up to a size of 100 MB.

- POST `/inventory-import/recommission-channels`    Will deploy all channels that are marked with `commission: true`
- POST `/inventory-import/do-not-recommission`      Will mark all channels with `commission: false`

#### Using `tag` for channel ID

The <channel id> in the paths can either be the UUID of the channel record (`channel.id`) or the value of the property
`channel.tag`. The tag is an optional, unique, max 24 character long string without spaces. If it's set on a channel,
that channel can be referenced by the tag in the various channel commands. The basic REST requests (GET, PUT, DELETE channel)
use the UUID like standard FOLIO APIs.

### The "import job"

A "job" is a continuous processing of source files that starts when a channel with an empty queue receives a new source
file, and the job ends when the file queue is once again empty. When the job ends, some metrics are calculated
covering the span of the job. There can thus only be zero or one job in a channel at a time.

A job is in other words a mostly automatic entity that primarily exists in order to organise the counting of record processes.
There are some ways for the operator to handle jobs, though. The start of a job can be controlled, if desired, by pausing the
channel listener while uploading source files to the channel. When the listener is restarted, this will trigger a
new job. A running job can also be paused and resumed if needed.

If a fatal error occurs while a job is running, the module will attempt to gracefully pause the job, so that it can potentially
be resumed.

Finally a job can be cancelled. This command will include

- stop the channel listener to prevent more files from entering the job
- calculating metrics for the duration of the job and mark the job cancelled
- empty the file queue

After cancelling the job, the operator must reactivate the listener to allow a new job to start. If some external process is still
uploading files to the queue, the queue will be populated when the listener is started even though the file queue was emptied
when cancelling the job.

#### Requests operating on jobs:

Channel operations will affect a current job in the channel. Besides that, there are following request that act on the
import job directly.

- POST `/inventory-import/channels/<channel id>/pause-job`
- POST `/inventory-import/channels/<channel id>/resume-job`
- POST `/inventory-import/channels/<channel id>/cancel-job`
