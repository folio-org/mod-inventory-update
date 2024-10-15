## 3.4.2

* Provides `instance-storage 11.0`
* Provides `holdings-storage 8.0`
* Provides `instance-storage-batch-sync 3.0`

## 3.4.1 2024-08-06

* [MODINVUP-106](https://issues.folio.org/browse/MODINVUP-106) Works around inventory storage deadlock when updating instance records plus holdings records

## 3.4.0 2024-07-22

* Requires `holdings-storage 5.0 6.0 7.0`
* Requires `holdings-storage-batch-sync 1.1 2.0`
* [MODINVUP-104](https://issues.folio.org/browse/MODINVUP-104) Adds missing module permissions for Inventory's batch storage APIs
* [MODINVUP-18](https://issues.folio.org/browse/MODINVUP-18) Prevents deletion of certain inventory records when in use or referenced by other modules
* [MODINVUP-99](https://issues.folio.org/browse/MODINVUP-99) Tweaks reporting of skipped deletes (protected holdings/items)

## 3.3.1 2024-04-08

* [MODINVUP-93](https://issues.folio.org/browse/MODINVUP-93) Configurable retention of 'external' holdings/items on delete request


## 3.3.0 2024-03-21

* [MODINVUP-61](https://issues.folio.org/browse/MODINVUP-61) Configurable retention of 'external' holdings/items by pattern matching
* [MODINVUP-90](https://issues.folio.org/browse/MODINVUP-90) New 'virtual' holdings properties from storage, remove them before PUT
* Maintenance:
* [MODINVUP-95](https://issues.folio.org/browse/MODINVUP-95) POM: Dependency upgrade
* [MODINVUP-91](https://issues.folio.org/browse/MODINVUP-91) POM: Reorder some dependencies
* [MODINVUP-88](https://issues.folio.org/browse/MODINVUP-88) Schema: Documentation-only changes


## 3.2.1 2023-10-13

* Upgrade vertx-stack-depchain to 4.4.5 (MODINVUP-78)

## 3.2.0 2023-10-11

* Upgrade mod-inventory-update to Java 17 (MODINVUP-71)

## 3.1.0 2023-05-01

* Provides `inventory-upsert-hrid 2.1`
* Provides `inventory-batch-upsert-hrid 2.1`
* Offers per-record configuration of overlays/retention (MODINVUP-54)
* Offers backwards compatibility with Inventory Storage 25.x (MODINVUP-59)

## 3.0.1 2023-03-07

* Updates Instance even if holdings create fails (MODINVUP-62)

## 3.0.0 2023-02-17

* Adapts to schema changes in Inventory Storage (MODINVUP-57)
* Provides `inventory-upsert-hrid 2.0` (MODINVUP-57)
* Provides `inventory-batch-upsert-hrid 2.0` (MODINVUP-57)
* Provides `shared-inventory-upsert-matchkey 2.0` (MODINVUP-57)
* Provides `shared-inventory-batch-upsert-matchkey 2.0` (MODINVUP-57)
* Requires `instance-storage 10.0` (MODINVUP-57)
* Requires `instance-storage-batch-sync 2.0` (MODINVUP-57)
* Requires `inventory-view-instance-set 2.0` (MODINVUP-57)
* Requires `holdings-storage-batch-sync 1.1`
* Requires `item-storage-batch-sync 1.0`
* Fixes order of item deletes and creates to avoid "intermittent" duplicate IDs (MODINVUP-46)

## 2.3.1 2022-09-05

* Bug-fix for a provisional instance duplication scenario with batch (MODINVUP-51)

## 2.3.0 2022-08-31

* Requires interface `instance-storage` `7.4 8.0 9.0`
* Requires interface `holdings-storage` `4.1 5.0 6.0`
* Requires interface `item-storage` `8.2 9.0 10.0`
* Performance enhancement in fetching from storage (MODINVUP-45)
* Bug-fix for provisional instance duplication scenarios with batch (MODINVUP-48)

## 2.2.0 2022-07-27

* Provides `inventory-batch-upsert-hrid 1.0`  (MODINVUP-41)
* Provides `shared-inventory-batch-upsert-matchkey 1.0` (MODINVUP-41)


## 2.1.0 2022-05-13

* Provides option for controlling overwrite of Item statuses (MODINVUP-6)

## 2.0.0 2022-02-17

* No longer provides `instance-storage-match` API (MODINVUP-16)
* Provides `shared-inventory-upsert-matchkey 1.2` with GET API (MODINVUP-15)
* Provides `inventory-upsert-hrid 1.1` with GET API (MODINVUP-15)
* Upgrades Netty, Vert.x, log4j, drops RMB (MODINVUP-26)

## 1.3.0 2021-10-19

* Provides `/admin/health` end-point (MODINVUP-12)

## 1.2.0 2021-10-13

* Handles (transfers) holdings/items in case of match-key changes (MODINVUP-9)
* Provides `shared-inventory-upsert-matchkey 1.1`  (MODINVUP-9)

## 1.1.0 2021-10-06

* Implements the optimistic locking protocol of Inventory Storage (MODINVUP-10)
* Requires interface `instance-storage` `7.4 8.0`
* Requires interface `holdings-storage` `4.1 5.0`
* Requires interface `item-storage` `8.2 9.0`

## 1.0.0 2021-09-25

* `shared-inventory-upsert-matchkey`: fix `number-of-part-section-of-work` for match key (MODINVUP-3)

## 0.0.4 2021-04-15

* `inventory-upsert-hrid` responds with 422 if `instance.hrid` is missing in request
* Changes semantics for deletion of holdings; no `holdingsRecord` property means 'ignore existing holdings' (FOL-28)
* Supports remove of record from shared instance by local identifier

## 0.0.3 2021-02-14

* Extend HRID upsert API with instance relations handling GBV-106
* Accept incoming UUIDs for new records (instances, holdings, items)
* Shared Inventory upsert: Stop populating match key to `indexTitle`
* Respond with details about HTTP 500 errors from Inventory Storage
* POM, Dockerfile upgraded to Java 11
* Added Jenkinsfile

## 0.0.2

* Provides DELETE methods for `inventory-upsert-hrid` and `shared-inventory-upsert-matchkey`
* Upgrade core dependencies

## 0.0.1

* Initial version of `mod-inventory-update` based on (disconnected) clone of `mod-inventory-match` `v3.1.0`

* Provides interface `inventory-upsert-hrid` 1.0
* Provides interface `shared-inventory-upsert-matchkey` 1.0







Just for reference, the history of `mod-inventory-match` up until the point of cloning.

## 3.1.0

 * Uses newly introduced Inventory (forks) Instance field `matchKey` (PR-466)

## 3.0.0

 * Multiple updates to match-key, aligning more closely with CoAlliance (PR-155)

## 2.4.3

 * Fix concurrency issue introduced with 2.4.1

## 2.4.2

 * Fix NPEs in match key generation

## 2.4.1

 * Fix suspected memory leak: close Okapi clients after use

## 2.4.0

* Add `edition` to match key
* Make medium (GMD) fixed length in match key
* Fix bug in the 70 character section of the match key containing the title
* Apply Unicode normalization to the match key

## 2.3.0

* Merges lists of resource identifiers from the existing and the new instance.

## 2.2.0

* Match key: fixed length title, strip more spec chars, only rightmost 4 digits of date

## 2.1.0

* Provides `instance-storage-match` interface 2.1
* Extends `Instance` schema with `matchKey` property
* Constructs and uses a match key string for comparing instances

