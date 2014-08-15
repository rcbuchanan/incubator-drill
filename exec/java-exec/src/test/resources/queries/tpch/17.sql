explain plan including all attributes for select
  sum(l.l_extendedprice) / 7.0 as avg_yearly
from
  dfs.tmp.`lineitem.parquet` l,
  dfs.tmp.`part.parquet` p
where
  p.p_partkey = l.l_partkey
  and p.p_brand = 'Brand#13'
  and p.p_container = 'JUMBO CAN'
  and l.l_quantity < (
    select
      0.2 * avg(l2.l_quantity)
    from
      dfs.tmp.`lineitem.parquet` l2
    where
      l2.l_partkey = p.p_partkey
  );
