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
# Presto/Accumulo Iterators for JDK 1.7
The Accumulo connector uses custom Accumulo iterators in
order to push various information in a SQL WHERE clause to Accumulo for
server-side filtering, known as *predicate pushdown*. In order
for the server-side iterators to work, you need to add the `presto-accumulo`
jar file to Accumulo's `lib/ext`directory on every Accumulo TabletServer node.

This Maven build uses JDK v1.7. If you are using JDK v1.8, just use the
``presto-accumulo jar`` file in ``$PRESTO_HOME/plugins/accumulo/`` instead
of this jar file.

```bash
git clone https://github.com/bloomberg/presto-accumulo.git
cd presto-accumulo/presto-accumulo-iterators/

mvn clean install -DskipTests

# For each TabletServer node:
scp target/presto-accumulo-iterators-*.jar [tabletserver_address]:$ACCUMULO_HOME/lib/ext

# Tablet Server should pick up new JAR files in ext directory, but may require restart
```