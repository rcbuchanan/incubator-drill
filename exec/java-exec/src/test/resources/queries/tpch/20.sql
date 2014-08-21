explain plan including all attributes for select
  s.s_name,
  s.s_address
from
  dfs.tmp.`supplier.parquet` s,
  dfs.tmp.`nation.parquet` n
where
  s.s_suppkey in (
    select
      ps.ps_suppkey
    from
      dfs.tmp.`partsupp.parquet` ps
    where
      ps. ps_partkey in (
        select
          p.p_partkey
        from
          dfs.tmp.`part.parquet` p
        where
          p.p_name like 'antique%'
      )
      and ps.ps_availqty > (
        select
          0.5 * sum(l.l_quantity)
        from
          dfs.tmp.`lineitem.parquet` l
        where
          l.l_partkey = ps.ps_partkey
          and l.l_suppkey = ps.ps_suppkey
          and l.l_shipdate >= date '1993-01-01'
          and l.l_shipdate < date '1993-01-01' + interval '1' year
      )
  )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'KENYA'
order by
  s.s_name;
