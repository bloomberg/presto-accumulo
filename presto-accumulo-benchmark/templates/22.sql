-- $ID$
-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- Functional Query Definition
-- Approved February 1998

SET SESSION accumulo.optimize_column_filters_enabled = false;
SET SESSION accumulo.optimize_range_predicate_pushdown_enabled = true;
SET SESSION accumulo.optimize_range_splits_enabled = true;
SET SESSION accumulo.secondary_index_enabled = true;

select
	cntrycode,
	count(*) as numcust,
	sum(c.acctbal) as totacctbal
from
	(
		select
			substring(c.phone from 1 for 2) as cntrycode,
			c.acctbal
		from
			${SCHEMA}.customer c
		where
			substring(c.phone from 1 for 2) in
				('13', '31', '23', '29', '30', '18', '17')
			and c.acctbal > (
				select
					avg(c.acctbal)
				from
					${SCHEMA}.customer c
				where
					c.acctbal > 0.00
					and substring(c.phone from 1 for 2) in
						('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists (
				select
					*
				from
					${SCHEMA}.orders o
				where
					o.custkey = c.custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;

