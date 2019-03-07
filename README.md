# mod-inventory-match


## Purpose
Mod-inventory-match (Inventory Match) is an Okapi service that can be put in front of mod-inventory-storage 
(Inventory Storage) when posting Instances (bibliographic records) to Inventory Storage. 

A client uploading Instances directly to Inventory Storage must decide on its own whether to create (POST) a new 
Instance or update (PUT) an existing Instance in Inventory Storage. By uploading to Inventory Match instead, the client
can delegate that responsibility to Inventory Match.

## API
Inventory Match exposes one end-point, which accepts PUT requests with an [Instance JSON body](ramls/instance.json):

* /instance-storage-match/instances

### Details of the matching mechanism
Based on select properties of the incoming Instance, Inventory Match will query Inventory Storage and determine 
whether to create or update an Instance in Inventory Storage. 

This prototype for an Instance matching mechanism is currently hard-coded to look at just one field of the Instance -- 
the title -- and compare it verbatim with titles already in Inventory Storage. 

If it does not find a matching title, a new Instance will be created. If it finds one, the existing Instance will be 
replaced by the incoming Instance, except, the HRID (human readable ID) of the original Instance will be retained.

Inventory Match will return the resulting Instance to the caller as a JSON string. 

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

## Some development to-dos (certain, likely, and potential)

* Include more fields in the matching mechanism
* Make selection of match fields configurable
* Implement a GET end-point for clients that need to find a matching Instance
* Add tests
* Determine how to update Instances (leave the original? overwrite the original? it depends? merge them? configurable?)
