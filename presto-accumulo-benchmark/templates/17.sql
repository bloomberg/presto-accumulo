-- $ID$
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998

SET SESSION accumulo.optimize_column_filters_enabled = false;
SET SESSION accumulo.optimize_range_predicate_pushdown_enabled = true;
SET SESSION accumulo.optimize_range_splits_enabled = true;
SET SESSION accumulo.secondary_index_enabled = true;

select
	sum(l.extendedprice) / 7.0 as avg_yearly
from
	${SCHEMA}.lineitem l,
	${SCHEMA}.part p
where
	p.partkey = l.partkey
	and p.brand = 'Brand#23'
	and p.container = 'MED BOX'
	and l.quantity < (
		select
			0.2 * avg(l.quantity)
		from
			lineitem
		where
			l.partkey = p.partkey
	);

