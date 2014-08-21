explain plan including all attributes for select
  c.c_name,
  c.c_custkey,
  o.o_orderkey,
  o.o_orderdate,
  o.o_totalprice,
  sum(l.l_quantity)
from
  dfs.tmp.`customer.parquet` c,
  dfs.tmp.`orders.parquet` o,
  dfs.tmp.`lineitem.parquet` l
where
  o.o_orderkey in (
    select
      l_orderkey
    from
      dfs.tmp.`lineitem.parquet`
    group by
      l_orderkey having
        sum(l_quantity) > 300
  )
  and c.c_custkey = o.o_custkey
  and o.o_orderkey = l.l_orderkey
group by
  c.c_name,
  c.c_custkey,
  o.o_orderkey,
  o.o_orderdate,
  o.o_totalprice
order by
  o.o_totalprice desc,
  o.o_orderdate
limit 100;
