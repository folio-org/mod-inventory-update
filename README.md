# mod-inventory-update

Copyright (C) 2019-2021 The Open Library Foundation

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

### API for fetching an Inventory record set

There are two REST paths for retrieving single Inventory record sets by ID: `/inventory-upsert-hrid/fetch/{id}`
and `/shared-inventory-upsert-matchkey/fetch/{id}`. The two APIs will currently return the same response, which is a set
with an Instance record, potentially an array of holdings records, each holdings-record potentially with an array of
Item records, and finally an array of any external relations that the Instance has with other Instances.

The provided ID can be the Instance HRID or UUID - for example: 

`GET /inventory-upsert-hrid/fetch/inst000000000017`

could fetch
```
{
  "instance" : {
    "id" : "a89eccf0-57a6-495e-898d-32b9b2210f2f",
    "_version" : 1,
    "hrid" : "inst000000000017",
    "source" : "FOLIO",
    "title" : "Interesting Times",
    "alternativeTitles" : [ ],
    "editions" : [ ],
    "series" : [ ],
    "identifiers" : [ {
      "value" : "0552142352",
      "identifierTypeId" : "8261054f-be78-422d-bd51-4ed9f33c3422"
    }, {
      "value" : "9780552142352",
      "identifierTypeId" : "8261054f-be78-422d-bd51-4ed9f33c3422"
    } ],
    "contributors" : [ {
      "name" : "Pratchett, Terry",
      "contributorNameTypeId" : "2b94c631-fca9-4892-a730-03ee529ffe2a"
    } ],
    "subjects" : [ ],
    "classifications" : [ ],
    "publication" : [ ],
    "publicationFrequency" : [ ],
    "publicationRange" : [ ],
    "electronicAccess" : [ ],
    "instanceTypeId" : "6312d172-f0cf-40f6-b27d-9fa8feaf332f",
    "instanceFormatIds" : [ ],
    "instanceFormats" : [ ],
    "physicalDescriptions" : [ ],
    "languages" : [ ],
    "notes" : [ ],
    "previouslyHeld" : false,
    "discoverySuppress" : false,
    "statisticalCodeIds" : [ ],
    "statusUpdatedDate" : "2021-10-31T21:34:07.179+0100",
    "metadata" : {
      "createdDate" : "2021-10-31T20:34:07.178+00:00",
      "updatedDate" : "2021-10-31T20:34:07.178+00:00"
    },
    "holdingsRecords2" : [ ],
    "natureOfContentTermIds" : [ ]
  },
  "holdingsRecords" : [ {
    "id" : "67cd0046-e4f1-4e4f-9024-adf0b0039d09",
    "_version" : 1,
    "hrid" : "hold000000000007",
    "formerIds" : [ ],
    "instanceId" : "a89eccf0-57a6-495e-898d-32b9b2210f2f",
    "permanentLocationId" : "f34d27c6-a8eb-461b-acd6-5dea81771e70",
    "effectiveLocationId" : "f34d27c6-a8eb-461b-acd6-5dea81771e70",
    "electronicAccess" : [ ],
    "callNumber" : "D15.H63 A3 2002",
    "notes" : [ ],
    "holdingsStatements" : [ {
      "statement" : "Line 1b"
    }, {
      "statement" : "Line 2b"
    } ],
    "holdingsStatementsForIndexes" : [ ],
    "holdingsStatementsForSupplements" : [ ],
    "statisticalCodeIds" : [ ],
    "holdingsItems" : [ ],
    "bareHoldingsItems" : [ ],
    "metadata" : {
      "createdDate" : "2021-10-31T20:34:08.430+00:00",
      "updatedDate" : "2021-10-31T20:34:08.430+00:00"
    },
    "items" : [ {
      "id" : "bb5a6689-c008-4c96-8f8f-b666850ee12d",
      "_version" : 1,
      "hrid" : "item000000000012",
      "holdingsRecordId" : "67cd0046-e4f1-4e4f-9024-adf0b0039d09",
      "formerIds" : [ ],
      "barcode" : "326547658598",
      "effectiveShelvingOrder" : "D 215 H63 A3 42002",
      "effectiveCallNumberComponents" : {
        "callNumber" : "D15.H63 A3 2002"
      },
      "yearCaption" : [ ],
      "notes" : [ ],
      "circulationNotes" : [ ],
      "status" : {
        "name" : "Checked out",
        "date" : "2021-10-31T20:34:08.961+00:00"
      },
      "materialTypeId" : "1a54b431-2e4f-452d-9cae-9cee66c9a892",
      "permanentLoanTypeId" : "2b94c631-fca9-4892-a730-03ee529ffe27",
      "effectiveLocationId" : "f34d27c6-a8eb-461b-acd6-5dea81771e70",
      "electronicAccess" : [ ],
      "statisticalCodeIds" : [ ],
      "metadata" : {
        "createdDate" : "2021-10-31T20:34:08.960+00:00",
        "updatedDate" : "2021-10-31T20:34:08.960+00:00"
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
```
The output from the fetch APIs can be PUT back to the `/inventory-upsert-hrid` API. For example, it would be possible to update the record set above, say set "editions" to \["First edition"\] or add another Item and PUT the record set JSON back to the `/inventory-upsert-hrid` API (warning: `/shared-inventory-upsert-matchkey` is special purpose and few if any would want to use its API like described here). 

Regarding the `_version` fields for optimistic locking seen in the output, they would have no effect in the PUT to the upsert API. When the service receives the updated JSON, it will pull new versions of the entities from storage and get latest versions from them anyway. 

## Planned developments

* Support handling of bound-with and analytics relationships. This is currently being developed with German GBV as the
  primary stakeholder.

* Possibly check for dependent records in other FOLIO modules, specifically before deleting Inventory records. This
  could be implemented by declaring optional dependencies of those external modules, meaning that dependency checks
  would only be performed if those modules were present in the installation. It might additionally be required to have a
  configuration setting to turn off the dependency checks entirely, for the performance of an initial data load for
  example, where it's already known that no dependent records exist yet.

There is a legacy end-point for back-wards compatibility with the module that was the basis for this module (
mod-inventory-match). This end-point will eventually be deprecated.

* `/instance-storage-match/instances`  -- matches based on combination of metadata in instance

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

