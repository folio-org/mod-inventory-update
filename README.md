# mod-inventory-update


## Purpose
Mod-inventory-update is an Okapi service that can be put in front of mod-inventory-storage (Inventory Storage) for populating the storage with Instances, holdings and items according to one of multiple different update schemes.


## API
Inventory Update so far supports two different update schemes implemented by two different end-points, which both accepts PUT requests with a payload of an [Inventory Record Set JSON body](ramls/inventoryRecordSet.json). An inventory records set is a set of records including an Inventory Instance and an array of holdings records with embedded arrays of items

* `/inventory-upsert-hrid`  _updates an Instance as well as its associated holdings and items based on incoming HRIDs on all three record types. If an instance with the incoming HRID does not exist in storage already, the new Instance is inserted otherwise the existing instance is updated - thus the term 'upsert'. This means that HRIDs are required to be present from the client side in all three record types. The API will detect if holdings and/or items have disappeared from the instance since last update and remove them. It will also detect if new holdings or items on the Instance existed already on a different Instance in storage and then move them over to the incoming Instance. The IDs (UUIDs) on any pre-existing Instances, holdings records and items will be preserved in this process, thus avoiding to break any external UUID based references to these records, except in the case of actual deletes of holdings/items._

* `/shared-inventory-upsert-matchkey`  _inserts or updates an Instance based on whether an Instance with the same matchkey  exists in storage already. The matchkey is typically generated from a combination of metadata in the bibliographic record, and the API has logic for that, but if an Instance comes in with a readymade `matchKey`, the end-point will used that instead. This API will replace (not update) existing holdings and items on the Instance when updating the Instance. Clients using this end-point must in other words expect the UUIDs of previously existing holdings records and items to be lost on Instance update. The scheme updates a so called shared Inventory, that is, an Inventory shared by multiple institutions that have agreed on this matchkey mechanism to identify "same" Instances. The end-point will mark the shared Instance with an identifier for each Institution that contributed to the Instance. When updating an Instance from one of the institutions, the end-point will take care to replace only those existing holdings records and items that came from that particular institution before._

### Details of the matching mechanism using a match key
Based on select properties of the incoming Instance, Inventory Match will construct a match key and query Inventory
Storage for it to determine if an instance with that key already exists.

The match logic currently considers title, year, publisher, pagination, edition and SUDOC classification.

If it does not find a matching title, a new Instance will be created. If it finds one, the existing Instance will be
replaced by the incoming Instance, except, the HRID (human readable ID) of the original Instance will be retained.

Inventory Match will return the resulting Instance to the caller as a JSON string.

## Planned developments

Both of the provided APIs are work in progress. Error handling, Inventory update feedback (counts and performance metrics), and unit testing is outstanding.

They both need to have their different schemes for Instance deletes implemented.

There is a legacay end-point for back-wards compatibility with the module that was the basis for this module (mod-inventory-match). This end-point will eventually be deprecated.

* `/instance-storage-match/instances`  -- matches based on combination of meta data in instance

More Inventory update schemes might be added, specifically an end-point that support Instance identification by matchKey _and_ holdings records and items identification by HRID for shared-inventory libraries that can provide such unique local identifiers for their records, for example:

* `/shared-inventory-upsert-matchkey-and-hrid`


## Prerequisites

- Java 8 JDK
- Maven 3.3.9

## Git Submodules

There are some common RAML definitions that are shared between FOLIO projects via Git submodules.

To initialise these please run `git submodule init && git submodule update` in the root directory.

If these are not initialised, the module will fail to build correctly, and other operations may also fail.

More information is available on the [FOLIO developer site](https://dev.folio.org/guides/developer-setup/#update-git-submodules).

## Building

run `mvn install` from the root directory.

