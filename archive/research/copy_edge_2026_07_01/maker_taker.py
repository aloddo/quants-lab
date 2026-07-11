import pyarrow.parquet as pq, numpy as np, pandas as pd
W=set(l.split(',')[0].lower() for l in open("/tmp/beta_neutral_skilled.csv").read().splitlines()[1:] if l.strip())
print(f"checking maker/taker for {len(W)} skilled wallets")
cols=["wallet","signed_size","price","mark","is_liquidation"]
pf=pq.ParquetFile("app/data/v15/m02_actions.parquet")
from collections import defaultdict
imp=defaultdict(list)  # wallet -> list of fill improvement bps (+ = maker/passive, - = taker/pays spread)
for b in pf.iter_batches(batch_size=1_000_000, columns=cols):
    d=b.to_pydict()
    for i in range(len(d["wallet"])):
        w=d["wallet"][i]
        if w not in W or d["is_liquidation"][i]: continue
        px=d["price"][i]; mk=d["mark"][i]; ss=d["signed_size"][i]
        if px is None or mk is None or ss is None or mk<=1e-9 or px<=0: continue
        # buy (ss>0): improvement=(mark-price)/mark ; sell: (price-mark)/mark
        b_imp=((mk-px)/mk if ss>0 else (px-mk)/mk)*1e4
        imp[w].append(b_imp)
rows=[]
for w,v in imp.items():
    if len(v)<50: continue
    rows.append((w,len(v),float(np.mean(v)),float(np.median(v))))
df=pd.DataFrame(rows,columns=["w","n","mean_imp","med_imp"])
makers=df[df.med_imp>1]; takers=df[df.med_imp<-1]; neutral=df[(df.med_imp>=-1)&(df.med_imp<=1)]
print(f"\nfill improvement (bps; + = fills BETTER than mark = MAKER/passive; - = TAKER/pays spread):")
print(f"  MAKERS (med_imp>+1bps, earn spread, UNCOPYABLE): {len(makers)}")
print(f"  TAKERS (med_imp<-1bps, pay spread, copyable):     {len(takers)}")
print(f"  NEUTRAL (-1..+1):                                 {len(neutral)}")
print(f"\n  overall median fill improvement across all skilled wallets: {df.med_imp.median():+.1f}bps")
print(f"  distribution: p10={df.med_imp.quantile(.1):+.1f} p50={df.med_imp.median():+.1f} p90={df.med_imp.quantile(.9):+.1f}")
print("\nsample (wallet, n_fills, mean_imp, med_imp):")
for _,r in df.sort_values("med_imp").head(6).iterrows(): print(f"  TAKER-ish {r.w[:12]} n={int(r.n)} med_imp={r.med_imp:+.1f}")
for _,r in df.sort_values("med_imp",ascending=False).head(6).iterrows(): print(f"  MAKER-ish {r.w[:12]} n={int(r.n)} med_imp={r.med_imp:+.1f}")
