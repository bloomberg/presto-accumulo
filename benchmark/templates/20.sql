-- $ID$
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998

SET SESSION accumulo.optimize_column_filters_enabled = false;

select
	s.name,
	s.address
from
	${SCHEMA}.supplier s,
	${SCHEMA}.nation n
where
	s.suppkey in (
		select
			ps.suppkey
		from
			${SCHEMA}.partsupp ps
		where
			ps.partkey in (
				select
					p.partkey
				from
					${SCHEMA}.part p
				where
					p.name like 'forest%'
			)
			and ps.availqty > (
				select
					0.5 * sum(l.quantity)
				from
					${SCHEMA}.lineitem l
				where
					l.partkey = ps.partkey
					and l.suppkey = ps.suppkey
					and l.shipdate >= date '1994-01-01'
					and l.shipdate < date '1994-01-01' + interval '1' year
			)
	)
	and s.nationkey = n.nationkey
	and n.name = 'CANADA'
order by
	s.name;

