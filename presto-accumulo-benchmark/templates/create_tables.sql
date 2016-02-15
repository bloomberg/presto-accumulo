
CREATE TABLE accumulo.${SCHEMA}.customer 
WITH (
	column_mapping = 'name:md:name,address:md:address,nationkey:md:nationkey,phone:md:phone,acctbal:md:acctbal,mktsegment:md:mktsegment,comment:md:comment',
	internal = true,
	index_columns = 'mktsegment'
) AS 
SELECT * FROM tpch.${SCHEMA}.customer;

CREATE TABLE accumulo.${SCHEMA}.lineitem 
WITH (
	column_mapping = 'orderkey:md:orderkey,partkey:md:partkey,suppkey:md:suppkey,linenumber:md:linenumber,quantity:md:quantity,extendedprice:md:extendedprice,discount:md:discount,tax:md:tax,returnflag:md:returnflag,linestatus:md:linestatus,shipdate:md:shipdate,commitdate:md:commitdate,receiptdate:md:receiptdate,shipinstruct:md:shipinstruct,shipmode:md:shipmode,comment:md:comment',
	internal = true,
	index_columns = 'discount,quantity,receiptdate,returnflag,shipdate,shipinstruct,shipmode'
) AS 
SELECT UUID() AS uuid, * FROM tpch.${SCHEMA}.lineitem;


CREATE TABLE accumulo.${SCHEMA}.nation 
WITH (
	column_mapping = 'name:md:name,regionkey:md:regionkey,comment:md:comment',
	internal = true
) AS 
SELECT * FROM tpch.${SCHEMA}.nation;


CREATE TABLE accumulo.${SCHEMA}.orders 
WITH (
	column_mapping ='custkey:md:custkey,orderstatus:md:orderstatus,totalprice:md:totalprice,orderdate:md:orderdate,orderpriority:md:orderpriority,clerk:md:clerk,shippriority:md:shippriority,comment:md:comment',
	internal = true,
	index_columns = 'orderdate'
) AS 
SELECT * FROM tpch.${SCHEMA}.orders;


CREATE TABLE accumulo.${SCHEMA}.part 
WITH (
	column_mapping = 'name:md:name,mfgr:md:mfgr,brand:md:brand,type:md:type,size:md:size,container:md:container,retailprice:md:retailprice,comment:md:comment',
	internal = true,
	index_columns = 'brand,container,type,size'
) AS 
SELECT * FROM tpch.${SCHEMA}.part;


CREATE TABLE accumulo.${SCHEMA}.partsupp
WITH (
	column_mapping = 'partkey:md:partkey,suppkey:md:suppkey,availqty:md:availqty,supplycost:md:supplycost,comment:md:comment',
	internal = true
) AS 
SELECT UUID() AS uuid, * FROM tpch.${SCHEMA}.partsupp;


CREATE TABLE accumulo.${SCHEMA}.region 
WITH (
	column_mapping = 'name:md:name,comment:md:comment',
	internal = true,
) AS 
SELECT * FROM tpch.${SCHEMA}.region;


CREATE TABLE accumulo.${SCHEMA}.supplier 
WITH (
	column_mapping = 'name:md:name,address:md:address,nationkey:md:nationkey,phone:md:phone,acctbal:md:acctbal,comment:md:comment',
	internal = true
) AS 
SELECT * FROM tpch.${SCHEMA}.supplier;


