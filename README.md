# mod-inventory-update

Copyright (C) 2019-2020 The Open Library Foundation

This software is distributed under the terms of the Apache License, Version 2.0. See the file "[LICENSE](LICENSE)" for
more information.

## Purpose

Mod-inventory-update is an Okapi service that can be put in front of mod-inventory-storage (Inventory Storage) for
populating the storage with Instances, holdings and items according to one of multiple different update schemes.

## API

Inventory Update so far supports two different update schemes implemented by two different end-points, which both
accepts PUT requests with a payload of an [Inventory Record Set JSON body](ramls/inventory-record-set.json). An
inventory record set is a set of records including an Inventory Instance, and an array of holdings records with embedded
arrays of items

### `/inventory-upsert-hrid`

Updates an Instance as well as its associated holdings and items based on incoming HRIDs on all three record types. If
an instance with the incoming HRID does not exist in storage already, the new Instance is inserted, otherwise the
existing instance is updated - thus the term 'upsert'.

This means that HRIDs are required to be present from the client side in all three record types.

The API will detect if holdings and/or items have disappeared from the Instance since last update and in that case
remove them from storage. It will also detect if new holdings or items on the Instance existed already on a different
Instance in storage and then move them over to the incoming Instance. The IDs (UUIDs) on any pre-existing Instances,
holdings records and items will be preserved in this process, thus avoiding breaking any external UUID based references
to these records.

The Inventory Record Set, that is PUT to the end point, may contain relations to other Instances, for example the kind
of relationships that tie multipart monographs together or relations pointing to preceding or succeeding titles. Based
on a comparison with the set of relationships that may be registered for the Instance in storage already, relationships
will be created and/or deleted (updating relationships is obsolete).

#### Provisional Instance created when related Instance doesn't exist yet

If an upsert request comes in with a relation to an Instance that doesn't already exist in storage, a provisional
Instance will be created provided that the request contains sufficient data as required for creating the provisional
Instance - like any mandatory Instance properties.

#### Deletion of Instance-to-Instance relations

Only existing relationships that are explicitly omitted in the request will be deleted. In FOLIO Inventory, a relation
will appear on both Instances of the relation, say, one Instance will have a parent relation and the other will have a
child relation. 

This may not be the case in the source system where, perhaps, the child record may declare its parent but the parent 
will not mention its child records. 

To support deletion of relations for these scenarios, and not implicitly but unintentionally delete too many, following rules apply:

Including an empty array of child instances will tell the API that if the Instance has any existing child relations, 
they should be deleted. 

```
"instanceRelations": {
  "childInstances": []
}
```

Leaving out any reference to child instances -- or as in this sample, any references to any related Instances at all -- means 
that any existing relationships will be left untouched by the update request. 

```
"instanceRelations": {
}
```


#### Instance DELETE requests

The API supports DELETE requests, which would delete the Instance with all of its associated holdings records and items
and any relations it might have to other Instances.

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

Based on select properties of the incoming Instance, Inventory Match will construct a match key and query Inventory
Storage for it to determine if an instance with that key already exists.

The match logic currently considers title, year, publisher, pagination, edition and SUDOC classification.

If it does not find a matching title, a new Instance will be created. If it finds one, the existing Instance will be
replaced by the incoming Instance, except, the HRID (human readable ID) of the original Instance will be retained.

Inventory Match will return the resulting Instance to the caller as a JSON string.

## Planned developments

Both of the provided APIs are work in progress. Error handling, Inventory update feedback (counts and performance
metrics), and unit testing is outstanding.

* Support handling of bound-with and analytics relationships

There is a legacy end-point for back-wards compatibility with the module that was the basis for this module (
mod-inventory-match). This end-point will eventually be deprecated.

* `/instance-storage-match/instances`  -- matches based on combination of meta data in instance

More Inventory update schemes might be added, specifically an end-point that support Instance identification by
matchKey _and_ holdings records and items identification by HRID for shared-inventory libraries that can provide such
unique local identifiers for their records, for example:

* `/shared-inventory-upsert-matchkey-and-hrid`

## Prerequisites

- Java 11 JDK
- Maven 3.3.9

## Git Submodules

There are some common RAML definitions that are shared between FOLIO projects via Git submodules.

To initialise these please run `git submodule init && git submodule update` in the root directory.

If these are not initialised, the module will fail to build correctly, and other operations may also fail.

More information is available on
the [FOLIO developer site](https://dev.folio.org/guides/developer-setup/#update-git-submodules).

## Building

run `mvn install` from the root directory.

