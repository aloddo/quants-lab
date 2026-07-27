"""Rank wallets on EDGE PER TURN net of our cost floor -- the metric nobody has screened on.

Total return hides the thing that kills us: a wallet can show a fine headline return while its edge
per unit of turnover sits below our 10.57 bps round-trip cost. Copy that wallet and turnover converts
its small edge into a large loss. This asks the only question that matters: does raw edge per turn
exceed cost per turn, with room to spare?
"""
import pandas as pd

s = pd.read_parquet('/tmp/m07_lat1000/m07_summary.parquet')
sl = pd.read_parquet('/tmp/copyscreen_shortlist.parquet')
m = sl.drop_duplicates('entity_id').set_index('entity_id').primary_wallet
s['wallet'] = s.entity_id.map(m)

s['slip'] = s.slip_bps_notional_sum / 1e4
# raw = net PnL with fees and slippage added back, funding removed
s['raw'] = s.realized_pnl_total + s.total_fees + s.slip - s.total_funding

g = s.groupby('wallet').agg(
    folds=('fold_id', 'nunique'),
    equity=('start_equity', 'sum'),
    notional=('notional_traded', 'sum'),
    raw=('raw', 'sum'),
    fees=('total_fees', 'sum'),
    slip=('slip', 'sum'),
    net=('realized_pnl_total', 'sum'),
    rt=('n_round_trips', 'sum'),
    rt_win=('n_round_trip_wins', 'sum'),
)
g = g[g.notional > 0]
g['turnover_x'] = g.notional / g.equity
g['edge_bps_per_turn'] = g.raw / g.notional * 1e4
g['cost_bps_per_turn'] = (g.fees + g.slip) / g.notional * 1e4
g['margin_bps'] = g.edge_bps_per_turn - g.cost_bps_per_turn
g['net_pct'] = g.net / g.equity * 100
g['rt_win_rate'] = (g.rt_win / g.rt.clip(lower=1) * 100)

print("=== EDGE PER TURN vs COST PER TURN (1s latency, %d wallets) ===" % len(g))
print()
print("edge_bps_per_turn distribution:")
print(g.edge_bps_per_turn.describe(percentiles=[.05, .25, .5, .75, .95, .99]).round(2).to_string())
print()
print("cost_bps_per_turn distribution:")
print(g.cost_bps_per_turn.describe(percentiles=[.05, .5, .95]).round(2).to_string())
print()
n_clear = (g.margin_bps > 0).sum()
print("wallets with edge > cost              : %d / %d (%.1f%%)" % (n_clear, len(g), n_clear / len(g) * 100))
for thr in (2, 5, 10):
    k = (g.margin_bps > thr).sum()
    print("wallets clearing cost by >%2d bps/turn  : %d" % (thr, k))
print()
strong = g[(g.margin_bps > 2) & (g.folds >= 6) & (g.rt >= 50)].sort_values('margin_bps', ascending=False)
print("=== CANDIDATES: margin >2bps/turn, >=6 folds, >=50 round trips ===")
print("%d wallets" % len(strong))
if len(strong):
    print(strong.head(15)[['folds', 'turnover_x', 'edge_bps_per_turn', 'cost_bps_per_turn',
                           'margin_bps', 'net_pct', 'rt', 'rt_win_rate']].round(2).to_string())
g.to_parquet('/tmp/edge_per_turn_ranked.parquet')
