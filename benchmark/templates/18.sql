-- $ID$
-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- Function Query Definition
-- Approved February 1998

SET SESSION accumulo.optimize_column_filters_enabled = false;

select
	c.name,
	c.custkey,
	o.orderkey,
	o.orderdate,
	o.totalprice,
	sum(l.quantity)
from
	${SCHEMA}.customer c,
	${SCHEMA}.orders o,
	${SCHEMA}.lineitem l
where
	o.orderkey in (
		select
			l.orderkey
		from
			${SCHEMA}.lineitem l
		group by
			l.orderkey having
				sum(l.quantity) > 300
	)
	and c.custkey = o.custkey
	and o.orderkey = l.orderkey
group by
	c.name,
	c.custkey,
	o.orderkey,
	o.orderdate,
	o.totalprice
order by
	o.totalprice desc,
	o.orderdate;

