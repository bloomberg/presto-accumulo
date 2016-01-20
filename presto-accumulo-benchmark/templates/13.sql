-- $ID$
-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- Functional Query Definition
-- Approved February 1998

SET SESSION accumulo.optimize_column_filters_enabled = false;
SET SESSION accumulo.optimize_range_predicate_pushdown_enabled = true;
SET SESSION accumulo.optimize_range_splits_enabled = true;
SET SESSION accumulo.secondary_index_enabled = true;

select
	c_count,
	count(*) as custdist
from
	(
		select
			c.custkey,
			count(o.orderkey)
		from
			${SCHEMA}.customer c left outer join ${SCHEMA}.orders o on
				c.custkey = o.custkey
				and o.comment not like '%special%requests%'
		group by
			c.custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc;

