## 0.0.3-SNAPSHOT

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

