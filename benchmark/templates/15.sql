-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

create view ${SCHEMA}.revenue as
	select
		l.suppkey AS number,
		sum(l.extendedprice * (1 - l.discount)) AS revenue
	from
		${SCHEMA}.lineitem l
	where
		l.shipdate >= date '1996-01-01'
		and l.shipdate < date '1996-01-01' + interval '3' month
	group by
		l.suppkey;

select
	s.suppkey,
	s.name,
	s.address,
	s.phone,
	r.revenue
from
	${SCHEMA}.supplier s,
	${SCHEMA}.revenue r
where
	s.suppkey = r.number
	and r.revenue = (
		select
			max(r.revenue)
		from r
	)
order by
	s.suppkey;

drop view revenue;

