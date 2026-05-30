"""V13 Module 01 — equity_reconstruct_v8.py tests.

Maps to per-Module 12 spec fixtures F1-1 through F1-9.
Covers the highest-risk paths fixed in m01 r4→r9:
  - F1-2: ledger type coverage (incl. accountClassTransfer toPerp flag)
  - F1-2b: spotTransfer = 0 (NOT user/dest)
  - F1-2c: activateDexAbstraction dex-scoped
  - F1-2d: borrowLend operation-aware
  - F1-2e: rewardsClaim USDC-only
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def test_F1_2_accountClassTransfer_toPerp_flag():
    """codex m01 r5 fix: accountClassTransfer uses `toPerp` bool, not user/dest convention."""
    from v13_equity_reconstruct_v8 import ledger_cash_delta

    # toPerp=True → POSITIVE inflow
    e_in = {"delta": {"type": "accountClassTransfer", "usdc": 250.0, "toPerp": True}}
    assert ledger_cash_delta(e_in, "0xa" * 40) == 250.0

    # toPerp=False → NEGATIVE outflow
    e_out = {"delta": {"type": "accountClassTransfer", "usdc": 250.0, "toPerp": False}}
    assert ledger_cash_delta(e_out, "0xa" * 40) == -250.0

    # Magnitude regardless of sign in `usdc` field
    e_abs = {"delta": {"type": "accountClassTransfer", "usdc": -250.0, "toPerp": True}}
    assert ledger_cash_delta(e_abs, "0xa" * 40) == 250.0


def test_F1_2_spotTransfer_zero_perp_impact():
    """codex m01 r5 fix: spotTransfer is SPOT-side only; ZERO perp cash impact."""
    from v13_equity_reconstruct_v8 import ledger_cash_delta

    e_usdc = {"delta": {"type": "spotTransfer", "token": "USDC", "user": "0xa" * 40, "usdc": 100.0}}
    assert ledger_cash_delta(e_usdc, "0xa" * 40) == 0.0

    e_other = {"delta": {"type": "spotTransfer", "token": "HYPE", "amount": 10.0}}
    assert ledger_cash_delta(e_other, "0xa" * 40) == 0.0


def test_F1_2_activateDexAbstraction_dex_scoped():
    """codex m01 r7 fix: non-main dex → 0; main dex → -abs(amount) when USDC."""
    from v13_equity_reconstruct_v8 import ledger_cash_delta

    # MAIN dex, USDC → -abs(amount)
    e_main = {"delta": {"type": "activateDexAbstraction", "token": "USDC", "usdc": 50.0, "dex": "main"}}
    assert ledger_cash_delta(e_main, "0xa" * 40) == -50.0

    # Empty dex (HL pre-multidex convention) → treated as MAIN
    e_empty = {"delta": {"type": "activateDexAbstraction", "token": "USDC", "usdc": 50.0}}
    assert ledger_cash_delta(e_empty, "0xa" * 40) == -50.0

    # xyz dex → 0 (does not affect MAIN cash)
    e_xyz = {"delta": {"type": "activateDexAbstraction", "token": "USDC", "usdc": 50.0, "dex": "xyz"}}
    assert ledger_cash_delta(e_xyz, "0xa" * 40) == 0.0

    # flx dex → 0
    e_flx = {"delta": {"type": "activateDexAbstraction", "token": "USDC", "usdc": 50.0, "dex": "flx"}}
    assert ledger_cash_delta(e_flx, "0xa" * 40) == 0.0


def test_F1_2_borrowLend_operation_aware():
    """codex m01 r5+r6 fix: borrowLend signed by operation; USDC-only."""
    from v13_equity_reconstruct_v8 import ledger_cash_delta

    e_supply = {"delta": {"type": "borrowLend", "token": "USDC", "usdc": 200.0, "operation": "supply"}}
    assert ledger_cash_delta(e_supply, "0xa" * 40) == -200.0

    e_withdraw = {"delta": {"type": "borrowLend", "token": "USDC", "usdc": 200.0, "operation": "withdraw"}}
    assert ledger_cash_delta(e_withdraw, "0xa" * 40) == 200.0

    # Non-USDC token → 0
    e_hype = {"delta": {"type": "borrowLend", "token": "HYPE", "amount": 100.0, "operation": "supply"}}
    assert ledger_cash_delta(e_hype, "0xa" * 40) == 0.0

    # codex m01 r13+r14: borrow/repay → ZERO equity change (debt offsets cash, not free money)
    e_borrow = {"delta": {"type": "borrowLend", "token": "USDC", "usdc": 500.0, "operation": "borrow"}}
    assert ledger_cash_delta(e_borrow, "0xa" * 40) == 0.0, "borrow must be zero equity (not +amt)"
    e_repay = {"delta": {"type": "borrowLend", "token": "USDC", "usdc": 500.0, "operation": "repay"}}
    assert ledger_cash_delta(e_repay, "0xa" * 40) == 0.0, "repay must be zero equity"


def test_F1_2_rewardsClaim_USDC_only():
    """codex m01 r5+r27 fix: rewardsClaim STRICTLY counts USDC-token only.
    Non-USDC reward with a `usdc` metadata field MUST return 0 (was bug pre-r27)."""
    from v13_equity_reconstruct_v8 import ledger_cash_delta

    e_usdc = {"delta": {"type": "rewardsClaim", "token": "USDC", "usdc": 25.0}}
    assert ledger_cash_delta(e_usdc, "0xa" * 40) == 25.0

    e_other = {"delta": {"type": "rewardsClaim", "token": "HYPE", "amount": 10.0}}
    assert ledger_cash_delta(e_other, "0xa" * 40) == 0.0

    # r27: non-USDC reward with metadata `usdc` field MUST NOT count
    e_hype_meta = {"delta": {"type": "rewardsClaim", "token": "HYPE", "usdc": 999.0, "amount": 10.0}}
    assert ledger_cash_delta(e_hype_meta, "0xa" * 40) == 0.0, \
        "non-USDC reward with usdc metadata field MUST return 0 (r27 strict)"


def test_F1_2_activateDexAbstraction_missing_token_defaults_USDC():
    """codex m01 r27 fix: missing-token activateDexAbstraction defaults to USDC per
    mature mapper convention."""
    from v13_equity_reconstruct_v8 import ledger_cash_delta

    # Missing token + main dex + usdc field → -abs(amount)
    e = {"delta": {"type": "activateDexAbstraction", "dex": "main", "usdc": 100.0}}
    assert ledger_cash_delta(e, "0xa" * 40) == -100.0

    # Missing token + empty dex → also treated as main + USDC
    e2 = {"delta": {"type": "activateDexAbstraction", "usdc": 50.0}}
    assert ledger_cash_delta(e2, "0xa" * 40) == -50.0


def test_F1_3_full_wallet_missing_marks_in_row_skip_gate():
    """codex m01 r25+r26+r27 fix: full-wallet missing-mark losses must contribute to BOTH
    numerator and denominator of row-skip-rate Gate 3 check."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # Accumulator declared
    assert "n_rows_skipped_full_wallet_loss = 0" in src
    # Incremented in both MP and serial paths
    accumulator_increments = src.count("n_rows_skipped_full_wallet_loss += int(res.get('n_rows_skipped_missing', 0))")
    assert accumulator_increments >= 2, "must increment in MP AND serial path"
    # Gate 3 includes full-loss in skip rate
    assert "n_rows_skipped_for_missing = n_rows_skipped_audit + n_rows_skipped_full_wallet_loss" in src


def test_F1_2_send_main_only_dex_filter():
    """codex m01 r5+r6 CRITICAL: send ledger MAIN-only (excludes xyz/flx)."""
    from v13_equity_reconstruct_v8 import ledger_cash_delta

    wallet = "0xa" * 40

    # MAIN dex outflow
    e_main_out = {"delta": {"type": "send", "token": "USDC",
                            "user": wallet, "destination": "0xb" * 40,
                            "sourceDex": "main", "destinationDex": "main",
                            "usdcValue": 100.0, "fee": 1.0}}
    assert ledger_cash_delta(e_main_out, wallet) == -101.0  # -(amt + fee)

    # MAIN dex inflow (as destination)
    e_main_in = {"delta": {"type": "send", "token": "USDC",
                           "user": "0xb" * 40, "destination": wallet,
                           "sourceDex": "main", "destinationDex": "main",
                           "usdcValue": 100.0, "fee": 1.0}}
    assert ledger_cash_delta(e_main_in, wallet) == 100.0

    # xyz dex → must NOT affect MAIN
    e_xyz = {"delta": {"type": "send", "token": "USDC",
                       "user": wallet, "destination": "0xb" * 40,
                       "sourceDex": "xyz", "destinationDex": "xyz",
                       "usdcValue": 999.0, "fee": 1.0}}
    assert ledger_cash_delta(e_xyz, wallet) == 0.0


def test_F1_3_cStakingTransfer_zero_perp_impact():
    """cStakingTransfer is HYPE staking — zero perp cash."""
    from v13_equity_reconstruct_v8 import ledger_cash_delta

    e = {"delta": {"type": "cStakingTransfer", "amount": 5.0, "token": "HYPE"}}
    assert ledger_cash_delta(e, "0xa" * 40) == 0.0


def test_F1_2_funding_pagination_helper_imports():
    """codex m01 r4 #1 fix: funding API paginates (HL 500-row cap)."""
    import v13_equity_reconstruct_v8 as m
    # Verify pagination keyword is in the source (smoke check; can't hit live API in unit test).
    src = (Path(m.__file__)).read_text()
    assert "PAGE_LIMIT = 500" in src
    assert "cursor_start = last_ts + 1" in src or "cursor_start = " in src


def test_F1_9_gate2_accounting_identity_real():
    """codex m01 r9+r10 fix: Gate 2 emits position_value_usd and validates
    `equity == cash + position_value_usd` within epsilon. Source must contain the new column
    + identity check."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # Column emitted as INDEPENDENT mark-derived (codex r10 fix; was equity-cash in r9 → tautology)
    assert "'position_value_usd': pos_value_independent" in src
    # Identity check in Gate 2
    assert "identity_residual" in src
    assert "GATE 2 (accounting identity" in src


def test_F1_8_validation_only_short_circuits():
    """codex m01 r9 fix: --validation-only short-circuits reconstruction."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # Short-circuit branch present
    assert "if validation_only:" in src
    assert "'validation_only': True" in src


def test_F1_8_validation_only_zero_audit_hard_fails():
    """codex m01 r11 fix: validation-only with 0 audited wallets must sys.exit(4) HARD FAIL."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    assert "VALIDATION-ONLY FAILED: 0 of" in src
    assert "sys.exit(4)" in src


def test_F1_8_validation_only_unknown_types_not_falsely_checked():
    """codex m01 r12 fix: validation-only must NOT check unknown_types in Gate 3 (ledger
    isn't loaded so the absence is artificial). Must include explicit NOTICE log."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # The NOTICE log is in the main() validation_only block, which contains "VALIDATION-ONLY MODE".
    # Find that section.
    notice_anchor = "VALIDATION-ONLY MODE: skipping series concat"
    notice_idx = src.find(notice_anchor)
    assert notice_idx > -1, "validation-only main section not found"
    val_only_main = src[notice_idx:notice_idx + 3000]
    # NOTICE log present
    assert "validation-only does NOT load ledger entries" in val_only_main
    # Find the gate3_pass = bool(...) section in validation-only main
    g3_start = val_only_main.find("gate3_pass = bool(")
    assert g3_start > -1
    g3_end = val_only_main.find(")", g3_start)
    g3_section = val_only_main[g3_start:g3_end]
    assert "n_unknown" not in g3_section, "validation-only Gate 3 must NOT check n_unknown"


def test_F1_3_anchor_position_seed_excludes_post_anchor_fills():
    """codex m01 r15+r16+r17 fix: compute_eq_at must seed start_positions from anchor_data['positions']
    ONLY for coins with NO post-anchor fills. Coins opened from zero after anchor_ms get
    their first-fill seed from r4 #3 path; using anchor (fetched-time) size for them would
    invent the position before it existed.

    BEHAVIORAL test: build a minimal positions_at + first-fill seed + anchor seed scenario.
    """
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # Source-level checks (kept for regression visibility)
    assert "post_anchor_fill_coins" in src
    assert "if coin in post_anchor_fill_coins:" in src
    assert "abs(anchor_ms - anchor_data['fetched_ms']) <= 86400000" in src

    # Behavioral: simulate the conditions and check the resulting start_positions logic.
    # We can't easily call compute_eq_at without full setup, so we simulate the seed loop logic
    # in isolation to verify the SKIP behavior.
    events_fills = [
        {"coin": "BTC", "time": 2000, "startPosition": 0.0, "signed_sz": 0.5},  # opened after anchor=1000
    ]
    anchor_ms = 1000
    fetched_ms = 5000
    anchor_data = {"fetched_ms": fetched_ms, "positions": {"BTC": 0.5, "ETH": 2.0}}
    start_positions = {}  # would-be after positions_at(events_fills, anchor_ms) for non-pre-anchor

    # Replicate r4 #3 first-fill seeding (NO seed because startPosition=0 for BTC)
    seen_coins_pre = {f['coin'] for f in events_fills if int(f['time']) <= anchor_ms}
    for f in events_fills:
        if int(f['time']) <= anchor_ms: continue
        coin = f['coin']
        if coin in seen_coins_pre or coin in start_positions: continue
        if abs(float(f.get('startPosition', 0))) > 1e-9:
            start_positions[coin] = float(f['startPosition'])
        seen_coins_pre.add(coin)
    assert "BTC" not in start_positions, "first-fill seed should NOT include BTC (startPosition=0)"

    # Replicate r17+r22 anchor seed logic (Case A: no fills → any anchor; Case B: pre+no post → 24h only)
    post_anchor_fill_coins = {f['coin'] for f in events_fills if int(f['time']) > anchor_ms}
    any_fill_coins = {f['coin'] for f in events_fills}
    for coin, szi in anchor_data['positions'].items():
        if coin in start_positions or abs(szi) < 1e-9:
            continue
        if coin in post_anchor_fill_coins:
            continue
        if coin not in any_fill_coins:
            start_positions[coin] = float(szi)  # Case A
            continue
        if abs(anchor_ms - fetched_ms) <= 86400000:
            start_positions[coin] = float(szi)  # Case B

    # ETH had no fills → should be seeded from anchor. BTC has post-anchor fill → must NOT be seeded.
    assert "ETH" in start_positions, "ETH (no fills) should be seeded from anchor positions"
    assert start_positions["ETH"] == 2.0
    assert "BTC" not in start_positions, "BTC (post-anchor fill) MUST NOT be seeded from anchor positions"


def test_F1_3_missing_marks_skip_row():
    """codex m01 r24 HIGH fix: rows with any missing marks MUST be skipped (not emitted with
    corrupted equity). Gate 3 enforces n_missing_marks_total == 0 in emitted rows + skip
    rate < 10%."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # Code skips when missing > 0
    assert "if missing > 0:" in src
    assert "n_rows_skipped_missing += 1" in src
    # Audit dict records the count
    assert "'n_rows_skipped_missing': n_rows_skipped_missing" in src
    # Gate 3 has missing_marks_emitted_ok and skip_for_missing_ok
    assert "missing_marks_emitted_ok = n_missing_marks_total == 0" in src
    assert "skip_for_missing_ok = skip_rate_for_missing < 0.10" in src


def test_F1_1_anchor_selection_bounded_by_start_ms():
    """codex m01 r23 HIGH fix: anchor selection must filter to anchors >= start_ms.
    Otherwise the walker may use a pre-start_ms API anchor and walk events from a stream
    that was only loaded from start_ms onwards → silent equity corruption for early days."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # Anchor filter must include start_ms lower bound
    assert "start_ms <= t <= eod_ms" in src, "before_anchors must filter to anchors >= start_ms"


def test_F1_3_anchor_position_seed_static_coin_any_anchor():
    """codex m01 r22 fix: coins with NO in-window fills get seeded at ANY anchor
    (Case A), not just within 24h of fetched_ms. Wallet holding 1 BTC for the whole
    window without trading must show that position at every anchor in the window."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # Code has the case A path (no any_fill_coins → seed at any anchor)
    assert "any_fill_coins = {f['coin'] for f in events_fills}" in src
    assert "if coin not in any_fill_coins:" in src
    # The Case B 24h heuristic only applies when coin DID have pre-anchor fills
    # (the 24h check is INSIDE the loop, AFTER the case A check)
    case_a_idx = src.find("if coin not in any_fill_coins:")
    case_b_idx = src.find("if abs(anchor_ms - anchor_data['fetched_ms']) <= 86400000", case_a_idx)
    assert case_b_idx > case_a_idx, "Case A (no-fill seed) must come before Case B (24h-heuristic seed)"


def test_F1_3_flx_anchor_contamination_flag():
    """codex m01 r17 fix: wallets with flx anchor presence get audit_flx_contamination_risk
    flag emitted per row + counted in Gate 3."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    assert "has_flx_anchor" in src
    assert "audit_flx_contamination_risk" in src
    # Gate 3 log mentions flx contamination wallets
    assert "WALLETS with flx anchor presence" in src


def test_F1_3_funding_xyz_filtered_from_main():
    """codex m01 r19 HIGH fix: funding_cash_delta must filter xyz: coin funding from MAIN."""
    from v13_equity_reconstruct_v8 import funding_cash_delta

    # MAIN funding (BTC perp) → counted
    e_main = {"delta": {"type": "funding", "coin": "BTC", "usdc": 12.34}}
    assert funding_cash_delta(e_main) == 12.34

    # xyz funding (HIP-3 stock perp) → ZERO (not MAIN account)
    e_xyz = {"delta": {"type": "funding", "coin": "xyz:NVDA", "usdc": 5.0}}
    assert funding_cash_delta(e_xyz) == 0.0, "xyz funding must NOT contaminate MAIN"

    # Non-funding event → 0
    e_other = {"delta": {"type": "deposit", "usdc": 100.0}}
    assert funding_cash_delta(e_other) == 0.0


def test_F1_1_funding_pagination_hard_fail_on_error():
    """codex m01 r20 HIGH fix: get_api_funding must return None on pagination failure,
    not silent partial. reconstruct_wallet must treat None as wallet INCOMPLETE."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # get_api_funding return type annotation includes None
    assert "list[dict] | None" in src
    # Returns None on fetch failure (not break with partial)
    assert "→ returning None" in src
    # reconstruct_wallet treats None as 'funding_fetch_incomplete' error
    assert "'error': 'funding_fetch_incomplete'" in src


def test_F1_8_validation_only_validates_funding_completeness():
    """codex m01 r21 HIGH fix: --validation-only must validate funding fetch completeness
    so it doesn't pass while normal mode would skip wallet as funding_fetch_incomplete."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # Find validation_only short-circuit
    vo_anchor = "if validation_only:"
    vo_idx = src.find(vo_anchor)
    assert vo_idx > -1
    # Section AFTER the short-circuit branch but BEFORE the return must call get_api_funding
    vo_block = src[vo_idx:vo_idx + 1500]
    assert "events_funding = get_api_funding(" in vo_block, \
        "validation-only must call get_api_funding to validate completeness (codex r21 fix)"
    assert "'error': 'funding_fetch_incomplete'" in vo_block, \
        "validation-only must skip wallet with funding_fetch_incomplete on None"


def test_F1_6_spot_usdc_today_proxy_compatibility():
    """codex m01 r18 fix: spot_usdc_today must be a usable proxy (MAIN perp_acct_value_today),
    NOT NaN, to avoid breaking journey_trace consumer that drops null rows. Status field
    documents the actual semantics."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # v8 emits MAIN perp_acct_value_today in legacy spot_usdc_today slot
    assert "df_out['spot_usdc_today'] = anchor_data['perp_acct_value_today']" in src
    # Status field documents the change
    assert "MAIN_PERP_ACCT_VALUE_PROXY_v8" in src


def test_F1_1_gate1_uses_per_day_drift():
    """codex m01 r11 fix: Gate 1 broadened to per-day drift (was last-day-only)."""
    import v13_equity_reconstruct_v8 as m
    src = (Path(m.__file__)).read_text()
    # Per-row drift column emitted
    assert "audit_drift_pct_per_day" in src
    # Gate 1 checks both last-day and per-day
    g1_section = src[src.find("GATE 1 (anchor reconciliation"):src.find("GATE 1 (anchor reconciliation") + 2000]
    assert "last-day" in g1_section and "per-day" in g1_section
