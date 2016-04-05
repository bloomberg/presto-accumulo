<!---
Copyright 2016 Bloomberg L.P.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Presto Accumulo!

A Presto connector for reading and writing data backed by Apache Accumulo.

This repository contains four sub-projects:

1. _presto_ - A patched version of Presto 0.142 containing support for the ANY clause.  This is similar to the __contains__ UDF that can be used to check if an element is in an array.  Users can use this clause instead of contains to enable predicate pushdown support -- and therefore the secondary index capability of the connector.  This folder also contains the `presto-accumulo` connector code.  See the [presto-docs page](https://github.com/bloomberg/presto-accumulo/blob/master/presto/presto-docs/src/main/sphinx/connector/accumulo.rst) for more information on the Accumulo connector.
2. _presto-accumulo-iterators_ - A collection of Accumulo iterators to be installed on the TabletServers.  These iterators are required to user the connector.
3. _presto-accumulo-benchmark_ - An implementation of the TPC-H benchmarking suite for testing the connector.
4. _presto-accumulo-tools_ - A Java project with some tools to help out with metadata management tasks that could not otherwise be done using SQL.