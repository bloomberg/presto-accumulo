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

1. _presto-accumulo-iterators_ - A collection of Accumulo iterators to be installed on the TabletServers.  These iterators are required to user the connector.
2. _presto-accumulo-benchmark_ - An implementation of the TPC-H benchmarking suite for testing the connector.
3. _presto-accumulo-tools_ - A Java project with some tools to help out with metadata management tasks that could not otherwise be done using SQL.
4. _presto-accumulo-examples_ - Usage examples of the tools

