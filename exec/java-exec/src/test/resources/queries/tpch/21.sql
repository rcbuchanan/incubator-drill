explain plan including all attributes for select
  s.s_name,
  count(*) as numwait
from
  dfs.tmp.`supplier.parquet` s,
  dfs.tmp.`lineitem.parquet` l1,
  dfs.tmp.`orders.parquet` o,
  dfs.tmp.`nation.parquet` n
where
  s.s_suppkey = l1.l_suppkey
  and o.o_orderkey = l1.l_orderkey
  and o.o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
    select
      *
    from
      dfs.tmp.`lineitem.parquet` l2
    where
      l2.l_orderkey = l1.l_orderkey
      and l2.l_suppkey <> l1.l_suppkey
  )
  and not exists (
    select
      *
    from
      dfs.tmp.`lineitem.parquet` l3
    where
      l3.l_orderkey = l1.l_orderkey
      and l3.l_suppkey <> l1.l_suppkey
      and l3.l_receiptdate > l3.l_commitdate
  )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'BRAZIL'
group by
  s.s_name
order by
  numwait desc,
  s.s_name
limit 100;
