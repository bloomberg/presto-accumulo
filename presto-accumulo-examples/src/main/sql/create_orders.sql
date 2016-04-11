
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
	index_columns = 'clerk'
);