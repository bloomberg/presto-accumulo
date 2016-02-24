CREATE TABLE accumulo.${SCHEMA}.customer (
	custkey BIGINT,
	name VARCHAR,
	address VARCHAR,
	nationkey BIGINT,
	phone VARCHAR,
	acctbal DOUBLE,
	mktsegment VARCHAR,
	comment VARCHAR
) WITH (
	column_mapping = 'name:md:name,address:md:address,nationkey:md:nationkey,phone:md:phone,acctbal:md:acctbal,mktsegment:md:mktsegment,comment:md:comment',
	internal = true,
	index_columns = 'mktsegment'
);

CREATE TABLE accumulo.${SCHEMA}.lineitem (
	uuid VARCHAR,
	orderkey BIGINT,
	partkey BIGINT,
	suppkey BIGINT,
	linenumber BIGINT,
	quantity BIGINT,
	extendedprice DOUBLE,
	discount DOUBLE,
	tax DOUBLE,
	returnflag VARCHAR,
	linestatus VARCHAR,
	shipdate VARCHAR,
	commitdate DATE,
	receiptdate DATE,
	shipinstruct VARCHAR,
	shipmode VARCHAR,
	comment VARCHAR
)
WITH (
	column_mapping = 'orderkey:md:orderkey,partkey:md:partkey,suppkey:md:suppkey,linenumber:md:linenumber,quantity:md:quantity,extendedprice:md:extendedprice,discount:md:discount,tax:md:tax,returnflag:md:returnflag,linestatus:md:linestatus,shipdate:md:shipdate,commitdate:md:commitdate,receiptdate:md:receiptdate,shipinstruct:md:shipinstruct,shipmode:md:shipmode,comment:md:comment',
	internal = true,
	index_columns = 'quantity,discount,returnflag,shipdate,receiptdate,shipinstruct,shipmode'
);


CREATE TABLE accumulo.${SCHEMA}.nation (
	nationkey BIGINT,
	name VARCHAR,
	regionkey BIGINT,
	comment VARCHAR
) WITH (
	column_mapping = 'name:md:name,regionkey:md:regionkey,comment:md:comment',
	internal = true,
	index_columns = 'name'
);


CREATE TABLE accumulo.${SCHEMA}.orders (
	orderkey BIGINT,
	custkey BIGINT,
	orderstatus VARCHAR,
	totalprice DOUBLE,
	orderdate DATE,
	orderpriority VARCHAR,
	clerk VARCHAR,
	shippriority BIGINT,
	comment VARCHAR
) WITH (
	column_mapping ='custkey:md:custkey,orderstatus:md:orderstatus,totalprice:md:totalprice,orderdate:md:orderdate,orderpriority:md:orderpriority,clerk:md:clerk,shippriority:md:shippriority,comment:md:comment',
	internal = true,
	index_columns = 'orderdate'
);


CREATE TABLE accumulo.${SCHEMA}.part (
	partkey BIGINT,
	name VARCHAR,
	mfgr VARCHAR,
	brand VARCHAR,
	type VARCHAR,
	size BIGINT,
	container VARCHAR,
	retailprice DOUBLE,
	comment VARCHAR
) WITH (
	column_mapping = 'name:md:name,mfgr:md:mfgr,brand:md:brand,type:md:type,size:md:size,container:md:container,retailprice:md:retailprice,comment:md:comment',
	internal = true,
	index_columns = 'brand,type,size,container'
);


CREATE TABLE accumulo.${SCHEMA}.partsupp (
	uuid VARCHAR,
	partkey BIGINT,
	suppkey BIGINT,
	availqty BIGINT,
	supplycost DOUBLE,
	comment VARCHAR
) WITH (
	column_mapping = 'partkey:md:partkey,suppkey:md:suppkey,availqty:md:availqty,supplycost:md:supplycost,comment:md:comment',
	internal = true,
	index_columns = 'partkey'
);


CREATE TABLE accumulo.${SCHEMA}.region (
	regionkey BIGINT,
	name VARCHAR,
	comment VARCHAR
) WITH (
	column_mapping = 'name:md:name,comment:md:comment',
	internal = true,
	index_columns = 'name'
);


CREATE TABLE accumulo.${SCHEMA}.supplier (
	suppkey BIGINT,
	name VARCHAR,
	address VARCHAR,
	nationkey BIGINT,
	phone VARCHAR,
	acctbal DOUBLE,
	comment VARCHAR
) WITH (
	column_mapping = 'name:md:name,address:md:address,nationkey:md:nationkey,phone:md:phone,acctbal:md:acctbal,comment:md:comment',
	internal = true,
	index_columns = 'name'
);


