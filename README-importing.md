# Using MIU for importing to inventory storage (WiP, actual implementation is both ahead of and behind this documentation)

The importing component of MIU consists of 0 or more import "channels" equipped with file queues and processing pipelines.

The only existing pipeline implementation is an XSLT transformation pipeline, that takes an XML 'collection' of 'record'
elements and -- through one or more custom written XSLT style-sheets -- transforms them into "inventory XML" that can be converted
to batches of inventory upsert compatible JSON records and processed through MIU's inventory upsert APIs.

The main objects are

  - a "channel" with an associated file queue
  - a processing pipeline, called a "transformation"
  - the "transformation"  has an ordered set of transformation style-sheets, called "steps"

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


Requests operating on channels:

- POST `/inventory-import/channels` create a channel
- POST `/inventory-import/channels/<channel id>/commission`   launch a channel that is actively listening for source files to import
- POST `/inventory-import/channels/<channel id>/pause-listener`  stop actively listening for source files in queue
- POST `/inventory-import/channels/<channel id>/resume-listener`  resume active listening for source files
- POST `/inventory-import/channels/<channel id>/decommission`  undeploy (disable) the channel
- PUT `/inventory-import/channels/<channel id>`  update properties of a channel
- DELETE `/inventory-import/channels/<channel id>` delete the channel configuration including the file queue but not its job history
