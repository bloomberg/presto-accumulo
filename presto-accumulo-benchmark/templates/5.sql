-- $ID$
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- Functional Query Definition
-- Approved February 1998

select
	n.name,
	sum(l.extendedprice * (1 - l.discount)) as revenue
from
	${SCHEMA}.customer c,
	${SCHEMA}.orders o,
	${SCHEMA}.lineitem l,
	${SCHEMA}.supplier s,
	${SCHEMA}.nation n,
	${SCHEMA}.region r
where
	c.custkey = o.custkey
	and l.orderkey = o.orderkey
	and l.suppkey = s.suppkey
	and c.nationkey = s.nationkey
	and s.nationkey = n.nationkey
	and n.regionkey = r.regionkey
	and r.name = 'ASIA'
	and o.orderdate >= date '1994-01-01'
	and o.orderdate < date '1994-01-01' + interval '1' year
group by
	n.name
order by
	revenue desc