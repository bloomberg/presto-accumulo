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

This repository contains tooling and examples for the Presto Accumulo connector built against Presto version 0.156. It is no longer being maintained.

Looking for the `PrestoBatchWriter`? Check out the 0.178 branch.

This repository contains three sub-projects:

1. _presto-accumulo-iterators_ - A collection of Accumulo iterators to be installed on the TabletServers.  These iterators are required to user the connector.
2. _presto-accumulo-tools_ - A Java project with some tools to help out with metadata management tasks that could not otherwise be done using SQL.
3. _presto-accumulo-examples_ - Usage examples of the tools
