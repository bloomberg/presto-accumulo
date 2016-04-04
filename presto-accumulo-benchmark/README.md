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
# presto-accumulo benchmark
This folder contains scripts for executing TPC-H queries leveragint the `presto-accumulo` connector.

### Dependencies
* presto-accumulo connector, installed and configured
    * Which required the presto-accumulo-iterator JAR on all TabletServer hosts
* tpch connector, configured

### Installing TPCH connector
Presto ships with a TPCH connector that generates tables of data for executing the TPCH queries.
Details on installing the TPCH connector is located in the [Presto documentation](https://prestodb.io/docs/current/connector/tpch.html).

### Creating the Tables
Creating the Accumulo tables is done by issuing CTAS statements against the Presto TPCH tables, inserting the data into the Accumulo tables.

The benchmark directory contains a folder `templates` which has several scripts that contain variables to be substitued -- specifically the TPCH schema you'd like to use.  All the tables in the schema are the same, but the larger numbers generate more rows of data.  Use the `set_vars.sh` script to set any variables, generating a `scripts` directory that contains the scripts you'll want to use to interact with the tables.

After cloning the repository:
```bash
$ cd benchmark/
$ ./set_vars.sh tiny
$ presto -f scripts/create_tables.sql 
CREATE TABLE: 1500 rows
CREATE TABLE: 60175 rows
CREATE TABLE: 25 rows
CREATE TABLE: 15000 rows
CREATE TABLE: 2000 rows
CREATE TABLE: 8000 rows
CREATE TABLE: 5 rows
CREATE TABLE: 100 rows
$ presto -f scripts/counts.sql 
"1500"
"15000"
"25"
"15000"
"2000"
"2000"
"5"
"100"
$ presto -f scripts/drop_tables.sql 
DROP TABLE
DROP TABLE
DROP TABLE
DROP TABLE
DROP TABLE
DROP TABLE
DROP TABLE
```

### Running the Queries
There is a bash script in the `benchmark` directory called `run_scripts.sh`.  This script assumes the variables have been set and the tables have been created.  Simply execute this script and all of the TPC-H queries that can be executed by Presto will be run.  Non-compliant SQL scripts (as well as the create/drop/count scripts) will be skipped.  See the comments within the bash script to view why some queries will not execute.Each script is printed to `stdout` before running the query.  The results will be dumped to `stdout` as well, along with the execution time.
```bash
$ cd benchmark/
$ ./run_scripts.sh
Executing 1.sql
-- $ID$
-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998

select
    returnflag,
    linestatus,
    sum(quantity) as sum_qty,
    sum(extendedprice) as sum_base_price,
    sum(extendedprice * (1 - discount)) as sum_disc_price,
    sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,
    avg(quantity) as avg_qty,
    avg(extendedprice) as avg_price,
    avg(discount) as avg_disc,
    count(*) as count_order
from
    tiny.lineitem
where
    shipdate <= date '1998-12-01' - interval '90' day
group by
    returnflag,
    linestatus
order by
    returnflag,
    linestatus;

"A","F","92447","1.2970121190000004E8","1.2325002970349996E8","1.2829896244477099E8","25.404506732618852","35641.99282769993","0.050065952184666355","3639"
"N","F","2255","2943302.3300000005","2790509.914999999","2906601.049755001","25.055555555555557","32703.35922222223","0.052222222222222205","90"
"N","O","185499","2.599671000099986E8","2.4702218530249965E8","2.5687762937960002E8","25.40734146007396","35607.05382961219","0.050026023832351","7301"
"R","F","96145","1.3523031803000015E8","1.2852640191730024E8","1.3380723709176506E8","25.638666666666666","36061.41814133337","0.04978933333333361","3750"

real    0m1.876s
user    0m1.655s
sys 0m0.218s
```
