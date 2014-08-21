explain plan including all attributes for select
  s.s_acctbal,
  s.s_name,
  n.n_name,
  p.p_partkey,
  p.p_mfgr,
  s.s_address,
  s.s_phone,
  s.s_comment
from
  dfs.tmp.`part.parquet` p,
  dfs.tmp.`supplier.parquet` s,
  dfs.tmp.`partsupp.parquet` ps,
  dfs.tmp.`nation.parquet` n,
  dfs.tmp.`region.parquet` r
where
  p.p_partkey = ps.ps_partkey
  and s.s_suppkey = ps.ps_suppkey
  and p.p_size = 41
  and p.p_type like '%NICKEL'
  and s.s_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'EUROPE'
  and ps.ps_supplycost = (

    select
      min(ps.ps_supplycost)

    from
      dfs.tmp.`partsupp.parquet` ps,
      dfs.tmp.`supplier.parquet` s,
      dfs.tmp.`nation.parquet` n,
      dfs.tmp.`region.parquet` r
    where
      p.p_partkey = ps.ps_partkey
      and s.s_suppkey = ps.ps_suppkey
      and s.s_nationkey = n.n_nationkey
      and n.n_regionkey = r.r_regionkey
      and r.r_name = 'EUROPE'
  )

order by
  s.s_acctbal desc,
  n.n_name,
  s.s_name,
  p.p_partkey
limit 100;
