# mod-inventory-update (MIU)

Copyright (C) 2019-2023 The Open Library Foundation

This software is distributed under the terms of the Apache License, Version 2.0. See the file "[LICENSE](LICENSE)" for
more information.

## Purpose

Mod-inventory-update (MIU) is an Okapi service that can be put in front of mod-inventory-storage (Inventory Storage) for
populating the storage with Instances, holdings and items according to one of multiple different update schemes.

## API

MIU so far supports two different update schemes implemented by two different end-points, which both
accept PUT requests with a payload of an [Inventory Record Set JSON body](ramls/inventory-record-set.json)
, `inventor-upsert-hrid` and `shared-inventory-upsert-matchkey`. An
inventory record set is a set of records including an Inventory Instance, and an array of holdings records with embedded
arrays of items.

Both upsert APIs have a batch equivalent, `inventory-batch-upsert-hrid`, and `shared-inventory-batch-upsert-matchkey`
respectively. The batch APIs take arrays of inventory record sets and utilizes Inventory Storage's batch upsert APIs
with
a significant improvement in overall data import performance compared to the record-by-record updates of the single
record APIs.

Refer to the [API documentation](#api-documentation) section, and to the following explanation sections:

### `/inventory-upsert-hrid`

Updates an Instance as well as its associated holdings and items based on incoming HRIDs on all three record types. If
an instance with the incoming HRID does not exist in storage already, the new Instance is inserted, otherwise the
existing instance is updated - thus the term 'upsert'.

This means that HRIDs are required to be present from the client side in all three record types.

The API will detect if holdings and/or items have disappeared from the Instance since last update and in that case
remove them from storage. Note, however, that there is a distinction between a request with no `holdingsRecords`
property and a request with an empty `holdingsRecords` property. If existing holdings and items should not be touched,
for example if holdings and items are maintained manually in Inventory, then no `holdingsRecords` property should appear
in the request JSON and existing records would be ignored. Providing an empty `holdingsRecords` property, on the other
hand, would cause all existing holdings and items to be deleted. The API will also detect if new holdings or items on
the Instance existed already on a different Instance in storage and then move them over to the incoming Instance. The
IDs (UUIDs) on any pre-existing Instances, holdings records and items will be preserved in this process, thus avoiding
breaking any external UUID based references to these records.

The Inventory Record Set, that is PUT to the end point, may contain relations to other Instances, for example the kind
of relationships that tie multipart monographs together or relations pointing to preceding or succeeding titles. Based
on a comparison with the set of relationships that may be registered for the Instance in storage already, relationships
will be created and/or deleted (updating relationships is obsolete).

#### Control record overlay on updates.

The default behavior of MIU is to simply replace the entire record on updates, for example override the entire
holdingsRecord, with the input JSON it receives from the client, except for the ID (UUID) and version.

The default behavior can be changed per request using structures
in [`processing`](ramls/instructions/processing-upsert.json).

##### Prevent MIU from override existing values

MIU can be instructed to leave certain properties in place when updating Instances, holdings records, and Items.

For example, to retain all existing Item properties that are not included in the request body to MIU, use
the `retainExistingValues` [schema](ramls/instructions/properties-retention.json).

```
"processing": {
   "item": {
     "retainExistingValues": {
       "forOmittedProperties": true
     }
   }
}
```

This is a way to have MIU only update the set of properties it should be concerned with and let other properties be the
responsibility of other processes if required.

If MIU sets certain properties on insert but should not touch them in subsequent updates -- even though they are
provided in the request body to MIU -- those properties can be explicitly turned off in updates:

```
"processing": {
   "item": {
     "retainExistingValues": {
       "forTheseProperties": [ "a", "b", "c" ]
     }
   }
}
```

The two settings can be combined to not touch neither omitted properties nor the explicitly listed properties.

If `forOmittedProperties` is used it requires the client to distinguish between sending an empty property vs not sending
the property at all. Say an Instance had `contributors` before, but now they were removed in the source catalog. If this
is communicated to MIU by an empty `contributors` property, then it's fine, it will become empty in Inventory Storage
too, but if the property is simply removed from the request body altogether, then the existing value of `contributors`
will be retained in storage if `forOmittedProperties` is set to true.

##### Instruct MIU to leave the item status unmodified under certain circumstance.

In case the Item status is being managed outside the MIU update process, likely by FOLIO Circulation, MIU can be
instructed to only touch the status under certain circumstances.

For example, to only overwrite a status of "On order" and retain any other statuses, do:

```
processing": {
   "item": {
     "status": {
       "policy": "overwrite",
       "ifStatusWas": [
         {"name": "On order"}
       ]
     }
   }
}
```

The default behavior is to overwrite all statuses.

##### Instruct MIU to avoid deleting items even though they are missing from the input

When MIU receives an Instance update, it will look for existing items on the holdings record that are not present in the
update and then delete them.

With this instruction, deletion can be prevented based on a regular expression matched against a specified property.
This could be used to preserve items created from other sources, provided that a regular expression can be written that
will identify such items
without at the same time matching items of the current data feed.

For example, to retain items that have HRIDs starting with non-digit characters:

```
processing": {
   "item": {
     "retainOmittedRecord": {
       "ifField": "hrid",
       "matchesPattern": "\\D+.*"
     }
   }
}
```

##### Retain omitted items if the status indicates that they are still circulating

Usually items that are omitted from the holdings record in the upsert will be removed from storage by MIU. The
exceptions are if they are delete protected as described in the previous section, or if they have a status that
indicates they might still be circulating.

MIU will avoid deleting items with following item statuses

* Awaiting delivery
* Awaiting pickup
* Checked out
* Aged to lost
* Claimed returned
* Declared lost
* Paged
* In transit

#### Provisional Instance created when related Instance doesn't exist yet

If an upsert request comes in with a relation to an Instance that doesn't already exist in storage, a provisional
Instance will be created provided that the request contains sufficient data as required for creating the provisional
Instance - like any mandatory Instance properties.

#### Deletion of Instance-to-Instance relations

Only existing relationships that are explicitly omitted in the request will be deleted. In FOLIO Inventory, a relation
will appear on both Instances of the relation, say, one Instance will have a parent relation, and the other will have a
child relation.

This may not be the case in the source system where, perhaps, the child record may declare its parent, but the parent
will not mention its child records.

To support deletion of relations for these scenarios, and not implicitly but unintentionally delete too many, following
rules apply:

Including an empty array of child instances will tell the API that if the Instance has any existing child relations,
they should be deleted.

```
"instanceRelations": {
  "childInstances": []
}
```

Leaving out any reference to child instances -- or as in this sample, any references to any related Instances at all --
means that any existing relationships will be left untouched by the update request.

```
"instanceRelations": {
}
```

#### Instance DELETE requests

The API supports DELETE requests, which would delete the Instance with all of its associated holdings records and items
and any relations it might have to other Instances. 

To delete an instance record with all its holdings and items, send a DELETE request to `/inventory-upsert-hrid` with a payload like this:

```
{
  "hrid": "001"
}
```

Note that deleting any relations that the Instance had to other instances only cuts those links between them but does not otherwise affect those other instances. 

##### Protecting certain items or holdings records from deletion in DELETE requests

Delete requests can be extended with a processing instruction that blocks deletion of holdings and/or items based on
pattern matching in select properties.

This can be used to avoid deletion of items that are created outside the MIU pipeline (for example through the
Inventory UI) provided that there is a pattern that can be applied to a property value of those records to identify
which items to protect.

For example, to protect items that have HRIDs starting with non-digit characters, following delete body for deletion of
the Instance with HRID "123456" could be used:

```
{
    "hrid":  "1234567",
    "processing": {
       "item": {
         "blockDeletion": {
           "ifField": "hrid",
           "matchesPattern": "\\D+.*"
         }
       }
    }
}
```

When a delete request is sent for an Instance that has protected Items, the deletion of the Instance, as well as the
holdings record for the Item, will be blocked as well. Other holdings records or Items that do not match the block
criteria will be deleted.

Deletion will likewise be blocked when one or more items under the instance have one of the statuses also listed in
"Retain omitted items if the status indicates that they are still circulating":

* Awaiting delivery
* Awaiting pickup
* Checked out
* Aged to lost
* Claimed returned
* Declared lost
* Paged
* In transit

#### Statistical coding of delete protection events

When records are prevented from being deleted, the delete or the upsert request can be configured to set specified
statistical codes on the record, which was up for deletion, and then update the existing record with those. This will not
count as a record update, and if the update fails -- for example due to use of invalid UUIDs -- it will write an error
to the module log but will not fail the overall request. If non-existing statistical codes are specified the storage
module will silently avoid setting them.

Here are some examples of statistical coding of skipped deletes, first in a delete requests

Set all available codes on all record types. This means not just setting the code on the item that was not delete but also marking it on the instance that consequently could also not be deleted:
```
{
  "hrid": "in001",
  "processing": {
    "item": {
      "blockDeletion": {
        "ifField": "hrid",
        "matchesPattern": "it.*"
      },
      "statisticalCoding": [
        {"if": "deleteSkipped", "becauseOf":  "ITEM_STATUS", "setCode": "1ce0a775-286f-45ae-8446-e26ba0687b61"},
        {"if": "deleteSkipped", "becauseOf":  "ITEM_PATTERN_MATCH", "setCode": "42b735fa-eb6f-4c53-b5d5-2d98500868c5"}
      ]
    },
    "holdingsRecord": {
      "blockDeletion": {
        "ifField": "hrid",
        "matchesPattern": "ho.*"
      },
      "statisticalCoding": [
        {"if": "deleteSkipped", "becauseOf":  "HOLDINGS_RECORD_PATTERN_MATCH", "setCode": "d11fd9d8-b234-4159-b7b1-61b531bb1405"},
        {"if": "deleteSkipped", "becauseOf":  "ITEM_STATUS", "setCode": "1ce0a775-286f-45ae-8446-e26ba0687b61"},
        {"if": "deleteSkipped", "becauseOf":  "ITEM_PATTERN_MATCH", "setCode": "42b735fa-eb6f-4c53-b5d5-2d98500868c5"}
      ]
    },
    "instance": {
      "statisticalCoding": [
        {"if": "deleteSkipped", "becauseOf":  "PO_LINE_REFERENCE", "setCode": "98993c92-c5c9-414b-b12c-a82836b0dbf6"},
        {"if": "deleteSkipped", "becauseOf":  "HOLDINGS_RECORD_PATTERN_MATCH", "setCode": "d11fd9d8-b234-4159-b7b1-61b531bb1405"},
        {"if": "deleteSkipped", "becauseOf":  "ITEM_STATUS", "setCode": "1ce0a775-286f-45ae-8446-e26ba0687b61"},
        {"if": "deleteSkipped", "becauseOf":  "ITEM_PATTERN_MATCH", "setCode": "42b735fa-eb6f-4c53-b5d5-2d98500868c5"}
      ]
    }
  }
}
```

Lift all codes up on the instance level, even if the primarily protected record was a holdings record or an item:

```{
  "hrid": "in001",
  "processing": {
    "item": {
      "blockDeletion": {
        "ifField": "hrid",
        "matchesPattern": "it.*"
      }
    },
    "holdingsRecord": {
      "blockDeletion": {
        "ifField": "hrid",
        "matchesPattern": "ho.*"
      }
    },
    "instance": {
      "statisticalCoding": [
        {"if":  "deleteSkipped", "becauseOf":  "PO_LINE_REFERENCE", "setCode": "98993c92-c5c9-414b-b12c-a82836b0dbf6"},
        {"if":  "deleteSkipped", "becauseOf":  "HOLDINGS_RECORD_PATTERN_MATCH", "setCode": "d11fd9d8-b234-4159-b7b1-61b531bb1405"},
        {"if":  "deleteSkipped", "becauseOf":  "ITEM_STATUS", "setCode": "1ce0a775-286f-45ae-8446-e26ba0687b61"},
        {"if":  "deleteSkipped", "becauseOf":  "ITEM_PATTERN_MATCH", "setCode": "42b735fa-eb6f-4c53-b5d5-2d98500868c5"}
      ]
    }
  }
}
```

Only set the code on the record that was directly protected from deletion (not on the indirectly delete protected records) :

```
{
  "hrid": "in001",
  "processing": {
    "item": {
      "blockDeletion": {
        "ifField": "hrid",
        "matchesPattern": "it.*"
      },
      "statisticalCoding": [
        {"if": "deleteSkipped", "becauseOf": "ITEM_STATUS", "setCode": "1ce0a775-286f-45ae-8446-e26ba0687b61"},
        {"if": "deleteSkipped", "becauseOf": "ITEM_PATTERN_MATCH", "setCode": "42b735fa-eb6f-4c53-b5d5-2d98500868c5"}
      ]
    },
    "holdingsRecord": {
      "blockDeletion": {
        "ifField": "hrid",
        "matchesPattern": "ho.*"
      },
      "statisticalCoding": [
        {"if": "deleteSkipped", "becauseOf": "HOLDINGS_RECORD_PATTERN_MATCH", "setCode": "d11fd9d8-b234-4159-b7b1-61b531bb1405"}
      ]
    },
    "instance": {
      "statisticalCoding": [
        {"if": "deleteSkipped", "becauseOf": "PO_LINE_REFERENCE", "setCode": "98993c92-c5c9-414b-b12c-a82836b0dbf6"}
      ]
    }
  }
}
```

Statistical codes can be set on holdings and items in upserts (deleting the instance is not an option in the upsert,
and preventing delete due to PO_LINE_REFERENCE does not apply).

For example:
```
"processing": {
  "item": {
    "retainOmittedRecord": {
      "ifField": "hrid",
      "matchesPattern": "it.*"
    },
    "statisticalCoding": [
      { "if": "deleteSkipped", "becauseOf": "ITEM_STATUS", "setCode": "2b750461-5368-4a4e-9484-4e1eea2bc384" },
      { "if": "deleteSkipped", "becauseOf": "ITEM_PATTERN_MATCH", "setCode": "6f143d4c-75fe-4987-ae3e-3d7c2a4ccca2" },
    ]
  },
  "holdingsRecord": {
    "retainOmittedRecord": {
      "ifField": "hrid",
      "matchesPattern": "ho.*"
    }
    "statisticalCoding": [
      { "if": "deleteSkipped", "becauseOf": "ITEM_STATUS", "setCode": "b2452cc2-7024-41fa-a5c7-b1736280d781" },
      { "if": "deleteSkipped", "becauseOf": "ITEM_PATTERN_MATCH", "setCode": "a8e70d5e-2861-4a89-93cc-88679c74e592" },
      { "if": "deleteSkipped", "becauseOf": "HOLDINGS_RECORD_PATTERN_MATCH", "setCode": "e24f4a4e-8d8f-4ca2-ab05-dd497a8379e3" }
    ]
  },
  "instance": {
  }
}
```

### `/shared-inventory-upsert-matchkey`

Inserts or updates an Instance based on whether an Instance with the same matchKey exists in storage already. The
matchKey is typically generated from a combination of metadata in the bibliographic record, and the API has logic for
that, but if an Instance comes in with a ready-made `matchKey`, the end-point will use that instead.

This API will replace (not update) existing holdings and items on the Instance, when updating the Instance. Clients
using this end-point must in other words expect the UUIDs of previously existing holdings records and items to be lost
on Instance update. The scheme updates a so-called shared Inventory, that is, an Inventory shared by multiple
institutions that have agreed on this matchKey mechanism to identify "same" Instances. The end-point will mark the
shared Instance with an identifier for each Institution that contributed to the Instance. When updating an Instance from
one of the institutions, the end-point will take care to replace only those existing holdings records and items that
came from that particular institution before.

This API does not support Instance-to-Instance relationships.

The API supports DELETE requests as well, but the shared Instance is not deleted on DELETE requests; rather the data
coming from a given library that contributed to that Instance are removed - like the local record identifier from that
library on the Instance as well as any holdings and items previously attached to the Instance from that library.

#### Details of the matching mechanism using a match key

Based on select properties of the incoming Instance, the API will construct a match key and query Inventory Storage for
it to determine if an instance with that key already exists.

The match logic currently considers title, year, publisher, pagination, edition and SUDOC classification.

If it does not find a matching title, a new Instance will be created. If it finds one, the existing Instance will be
replaced by the incoming Instance, except, the HRID (human-readable ID) of the original Instance will be retained, as
well as the resource identifiers from any of the other libraries that contributed that Instance.

### Batch APIs `/inventory-batch-upsert-hrid` and `/shared-inventory-batch-upsert-matchkey`

A client can send arrays of inventory record sets to the batch APIs with significant improvement to overall throughput
compared to the single record APIs.

These APIs utilize Inventory Storage's batch upsert APIs for Instances, holdings records and Items.

In case of data problems in either type of entity (referential constraint errors, missing mandatory properties, etc),
the entire batch of the given entity will fail. For example if an Item fails all Items fail. At this point all the
Instances and
holdings records are presumably persisted. The module aims to recover from such inconsistent states by
switching from batch processing to record-by-record updates in case of errors so that, in this example, all the good
Items can be persisted. The
response on a request with one or more errors, that didn't prevent the entire request from being processed, will be
a `207` `Multi-Status`, and the one or more error that were found will be returned with the
response JSON, together with the summarized update statistics ("metrics).

In a feed with many errors the throughput will be close to that of the single record APIs since many batches will be
processed record-by-record.

If the client needs to pair up error reports in the response with any original records it holds itself, the client can
set an identifier in the inventory record set property "processing". The name and content of that property is entirely
up to the client, MIU will simply return the data it gets, so it could be a simple sequence number for the
batch, like

```
{
 "inventoryRecordSets":
  [
   {
    "instances": ...
    "holdingsRecords": ...
    "processing": {
      ...
      "batchIndex": 1
    }
   },
    "instances": ...
    "holdingsRecords": ...
    "processing": {
      ...
      "batchIndex": 2
    }
}
```

A response of a batch upsert of 100 Instances where the 50th failed, the error message is tagged with the clients batch
index.

```
{
    "metrics": {
        "INSTANCE": {
            "CREATE": {
                "COMPLETED": 99,
                "FAILED": 1,
                "SKIPPED": 0,
                "PENDING": 0
            },
            "UPDATE": {
                "COMPLETED": 0,
                "FAILED": 0,
                "SKIPPED": 0,
                "PENDING": 0
            },
            "DELETE": {
                "COMPLETED": 0,
                "FAILED": 0,
                "SKIPPED": 0,
                "PENDING": 0
            }
        },
        "HOLDINGS_RECORD": {
            "CREATE": {
                "COMPLETED": 0,
                "FAILED": 0,
                "SKIPPED": 0,
                "PENDING": 0
            },
            "UPDATE": {
                "COMPLETED": 0,
                "FAILED": 0,
                "SKIPPED": 0,
                "PENDING": 0
            },
            "DELETE": {
                "COMPLETED": 0,
                "FAILED": 0,
                "SKIPPED": 0,
                "PENDING": 0
            }
        },
        "ITEM": {
            "CREATE": {
                "COMPLETED": 0,
                ...
                ...
                ...
            }
        }
    },
    "errors": [
        {
            "category": "STORAGE",
            "message": {
                "message": {
                    "errors": [
                        {
                            "message": "must not be null",
                            "type": "1",
                            "code": "javax.validation.constraints.NotNull.message",
                            "parameters": [
                                {
                                    "key": "source",
                                    "value": "null"
                                }
                            ]
                        }
                    ]
                }
            },
            "shortMessage": "One or more errors occurred updating Inventory records",
            "entityType": "INSTANCE",
            "entity": {
                "title": "New title 50",
                "instanceTypeId": "12345",
                "matchKey": "new_title_50__________________________________________________________0000_____________________________________________________________________________________________p",
                "id": "782d5015-147d-433d-beaf-06a47bde6be5"
            },
            "statusCode": 422,
            "requestJson": {
                "instance": {
                    "title": "New title 50",
                    "instanceTypeId": "12345",
                    "matchKey": "new_title_50__________________________________________________________0000_____________________________________________________________________________________________p"
                },
                "processing": {
                    "batchIndex": 50
                }
            },
            "details": {

            }
        }
    ]
}
```

Note that any given batch cannot touch the same records twice, since that would require a certain order of processing,
something that batching will not be able to guarantee. For the upsert by HRID for example, if any HRID appears twice in
the
batch, the module will fall back to record-by-record updates and process the record sets in the order it receives them.
Same
for the upsert by match-key, if any match-key appears twice in the batch.

### APIs for fetching an Inventory record set

There are two REST paths for retrieving single Inventory record sets by ID: `/inventory-upsert-hrid/fetch/{id}`
and `/shared-inventory-upsert-matchkey/fetch/{id}`. Both APIs will return a record set with an Instance record,
potentially an array of holdings records, each holdings-record potentially with an array of Item records, and finally a
set of arrays of external relations that the Instance has with other Instances.

#### Fetching an Inventory record set from `inventory-upsert-hrid/fetch`

The ID provided on the API path is the Instance HRID. A request like
`GET /inventory-upsert-hrid/fetch/inst000000000017` would give a response like this (shortened):

```
{
  "instance" : {
    "_version" : 1,
    "hrid" : "inst000000000017",
    "source" : "FOLIO",
    "title" : "Interesting Times",
    "identifiers" : [ {
      "value" : "0552142352",
      "identifierTypeId" : "8261054f-be78-422d-bd51-4ed9f33c3422"
    } ],
    "contributors" : [ {
      "name" : "Pratchett, Terry",
      "contributorNameTypeId" : "2b94c631-fca9-4892-a730-03ee529ffe2a"
    } ],
    "subjects" : [ ],
    ... etc
    "statusUpdatedDate" : "2021-11-01T23:31:36.026+0100",
    "metadata" : {
      "createdDate" : "2021-11-01T22:31:36.025+00:00",
      "updatedDate" : "2021-11-01T22:31:36.025+00:00"
    },
  },
  "holdingsRecords" : [ {
    "_version" : 1,
    "hrid" : "hold000000000007",
    "permanentLocationId" : "f34d27c6-a8eb-461b-acd6-5dea81771e70",
    ... etc
    "metadata" : {
      "createdDate" : "2021-11-01T22:31:38.030+00:00",
      "updatedDate" : "2021-11-01T22:31:38.030+00:00"
    },
    "items" : [ {
      "_version" : 1,
      "hrid" : "item000000000012",
      "barcode" : "326547658598",
      ... etc
      "status" : {
        "name" : "Checked out",
        "date" : "2021-11-01T22:31:38.587+00:00"
      },
      "materialTypeId" : "1a54b431-2e4f-452d-9cae-9cee66c9a892",
      "metadata" : {
        "createdDate" : "2021-11-01T22:31:38.587+00:00",
        "updatedDate" : "2021-11-01T22:31:38.587+00:00"
      }
    } ]
  } ],
  "instanceRelations" : {
    "parentInstances" : [ ],
    "childInstances" : [ ],
    "precedingTitles" : [ ],
    "succeedingTitles" : [ ]
  }
}
(Note: it's possible to use the Instance UUID instead of the HRID in the GET request)
```

It's possible to take the response from the `/inventory-upsert-hrid/fetch` and PUT it back to
the `/inventory-upsert-hrid` API.

There may not be obvious use cases for it but for what it's worth, the response JSON can be edited by, say, setting
"editions" to ["First edition"] or adding one more Item, and the record set JSON can then be PUT back
to `/inventory-upsert-hrid` to perform the updates.

The response JSON above contains none of the primary key fields, `id`, or referential fields,
`instanceId` and `holdingsRecordId`, for the three main entities of the Inventory record set. This is because the
`inventory-upsert-hrid` API is entirely HRID based (at least when viewed from the outside. Internally the module of
course deals with the UUIDs).

The client of the API is responsible for knowing what the HRIDs for the records are and for ensuring that the
provided IDs are indeed unique.

#### Fetching an Inventory record set from `shared-inventory-upsert-matchkey/fetch`

For consistency, it is also possible to fetch a record set from the shared inventory API like from the HRID based API.
Similarly, it's possible to PUT the record set back to the API, though in reality, it probably will not make sense to
update a shared Inventory like that. With a shared Inventory, updates should probably always come from the catalogs that
participate in the shared index.

#### Avoiding cross-PUTting between the two APIs

If a GET request is issued to an Inventory that is in fact not a shared Inventory and therefore has no matchKeys in the
instances, the GET will fail. This is basically just to separate the two update and fetch schemes some.

Generally speaking, it does not make sense to mix the two APIs even though it's technically possible to fetch from one
and put to the other. If the module is used with a regular Inventory (non-shared) it could be feasible to disable the
shared Inventory APIs by not giving users the permissions required to use it. For a regular Inventory, one or both of
the permissions `inventory-upsert-hrid.item.get`
and `inventory-upsert-hrid.item.put` might be assigned, while the permissions for the shared
Inventory, `shared-inventory-upsert-matchkey.item.put` and `shared-inventory-upsert-matchkey.item.get`, could be left
out.

#### The _version fields and optimistic locking

The `_version` fields for optimistic locking can be seen in the output above. These values would have no effect in a PUT
to the upsert API. As the service receives the record set JSON in a PUT request, it will pull new versions of the
entities from storage and get the latest version numbers from that anyway.

       | 2.2.0                          |

## Planned developments

* Support handling of bound-with and analytics relationships. This is currently being developed with German GBV as the
  primary stakeholder.

* Possibly check for dependent records in other FOLIO modules, specifically before deleting Inventory records. This
  could be implemented by declaring optional dependencies of those external modules, meaning that dependency checks
  would only be performed if those modules were present in the installation. It might additionally be required to have a
  configuration setting to turn off the dependency checks entirely, for the performance of an initial data load for
  example, where it's already known that no dependent records exist yet.

More Inventory update schemes might be added, specifically an end-point that support Instance identification by
matchKey _and_ holdings records and items identification by HRID for shared-inventory libraries that can provide such
unique local identifiers for their records, for example:

* `/shared-inventory-upsert-matchkey-and-hrid`

## Prerequisites

- Java 17 JDK
- Maven 3.3.9

## Git Submodules

There are some common RAML definitions that are shared between FOLIO projects via Git submodules.

To initialise these please run `git submodule init && git submodule update` in the root directory.

If these are not initialised, the module will fail to build correctly, and other operations may also fail.

More information is available on
the [FOLIO developer site](https://dev.folio.org/guides/developer-setup/#update-git-submodules).

## Building

run `mvn install` from the root directory.

## Additional information

### Other documentation

Other [modules](https://dev.folio.org/source-code/#server-side) are described, with further FOLIO Developer
documentation at [dev.folio.org](https://dev.folio.org/)

### Code of Conduct

Refer to the Wiki [FOLIO Code of Conduct](https://wiki.folio.org/display/COMMUNITY/FOLIO+Code+of+Conduct).

### Issue tracker

See project [MODINVUP](https://issues.folio.org/browse/MODINVUP)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

### ModuleDescriptor

See the [ModuleDescriptor](descriptors/ModuleDescriptor-template.json)
for the interfaces that this module requires and provides, the permissions, and the additional module metadata.

### API documentation

API descriptions:

* [RAML](ramls/)
* [Schemas](ramls/)

Generated [API documentation](https://dev.folio.org/reference/api/#mod-inventory-update).

### Code analysis

[SonarQube analysis](https://sonarcloud.io/dashboard?id=org.folio%3Amod-inventory-update).

### Download and configuration

The built artifacts for this module are available. See [configuration](https://dev.folio.org/download/artifacts) for
repository access, and the [Docker image](https://hub.docker.com/r/folioorg/mod-inventory-update/).

