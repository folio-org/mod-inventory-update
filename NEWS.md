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

