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

