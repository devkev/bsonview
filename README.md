bsonview
========

**Interactive terminal-based viewer for [BSON](http://bsonspec.org/) files.**

Copyright (C) 2019-present MongoDB, Inc.

**DISCLAIMER:**
* bsonview is **_NOT_ an official MongoDB, Inc product**.
* **No warranty or support of any kind** is provided by MongoDB, Inc.
* **MongoDB, Inc has no relationship with this code**, other than owning the copyright.

Distributed under the [Server Side Public License, version 1 (SSPLv1)](http://www.mongodb.com/licensing/server-side-public-license).


Overview
--------

Like [`less`](https://en.wikipedia.org/wiki/Less_(Unix)), but for BSON files.

The code is pretty rough, and could use a lot of cleanup, but basic functionality works.


Dependencies
------------

* Requires [libtickit](http://www.leonerd.org.uk/code/libtickit/) (which currently depends on [libtermkey](http://www.leonerd.org.uk/code/libtermkey/)) to be installed externally.  [Build and install info.](src/mongo/bsonview/libtickit-install)


Usage
-----

```
bv name-of-bson-file.bson
```

Key Commands
------------

(Coming soon.  For now, refer to the source.)


Building
--------

The code is based on and uses the [MongoDB server codebase](https://github.com/mongodb/mongo).

```
python3 buildscripts/scons.py CC=clang CXX=clang++ CCFLAGS='-Wa,--compress-debug-sections -gsplit-dwarf' MONGO_VERSION='0.0.0' MONGO_GIT_HASH='unknown' --allocator=system bv
```


Known Issues
------------

* `tcmalloc` and `libtickit` don't get along, so `bv` has to be built with the system allocator.  Since bsonview is (currently) single threaded, this is minor.
* Using `$ne`, `$in`, `$nin`, and other similar MQL query predicate operators currently causes `bv` to segfault.


Contact
-------

Kevin Pulo, kev@mongodb.com, kev@pulo.com.au
