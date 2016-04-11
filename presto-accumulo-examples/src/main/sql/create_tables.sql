
DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
    custkey BIGINT,
    name VARCHAR,
    address VARCHAR,
    nationkey BIGINT,
    phone VARCHAR,
    acctbal DOUBLE,
    mktsegment VARCHAR,
    comment VARCHAR
) WITH (
    index_columns = 'mktsegment'
);

DROP TABLE IF EXISTS lineitem;
CREATE TABLE lineitem (
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
    index_columns = 'quantity,discount,returnflag,shipdate,receiptdate,shipinstruct,shipmode'
);

DROP TABLE IF EXISTS nation;
CREATE TABLE nation (
    nationkey BIGINT,
    name VARCHAR,
    regionkey BIGINT,
    comment VARCHAR
);

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
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
    index_columns = 'orderdate'
);

DROP TABLE IF EXISTS part;
CREATE TABLE part (
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
    index_columns = 'brand,type,size,container'
);

DROP TABLE IF EXISTS partsupp;
CREATE TABLE partsupp (
    uuid VARCHAR,
    partkey BIGINT,
    suppkey BIGINT,
    availqty BIGINT,
    supplycost DOUBLE,
    comment VARCHAR
) WITH (
    index_columns = 'partkey'
);

DROP TABLE IF EXISTS region;
CREATE TABLE region (
    regionkey BIGINT,
    name VARCHAR,
    comment VARCHAR
);

DROP TABLE IF EXISTS supplier;
CREATE TABLE supplier (
    suppkey BIGINT,
    name VARCHAR,
    address VARCHAR,
    nationkey BIGINT,
    phone VARCHAR,
    acctbal DOUBLE,
    comment VARCHAR
) WITH (
    index_columns = 'name'
);
