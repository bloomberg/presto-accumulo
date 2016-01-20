-- $ID$
-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998

SET SESSION accumulo.optimize_column_filters_enabled = false;
SET SESSION accumulo.optimize_range_predicate_pushdown_enabled = true;
SET SESSION accumulo.optimize_range_splits_enabled = true;
SET SESSION accumulo.secondary_index_enabled = true;

select
	ps.partkey,
	sum(ps.supplycost * ps.availqty) as value
from
	${SCHEMA}.partsupp ps,
	${SCHEMA}.supplier s,
	${SCHEMA}.nation n
where
	ps.suppkey = s.suppkey
	and s.nationkey = n.nationkey
	and n.name = 'GERMANY'
group by
	ps.partkey having
		sum(ps.supplycost * ps.availqty) > (
			select
				sum(ps.supplycost * ps.availqty) * 0.0001
			from
				partsupp,
				supplier,
				nation
			where
				ps.suppkey = s.suppkey
				and s.nationkey = n.nationkey
				and n.name = 'GERMANY'
		)
order by
	value desc;
