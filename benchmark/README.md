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
