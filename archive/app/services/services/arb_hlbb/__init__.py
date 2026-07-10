"""
HL-Bybit Perp/Perp Spread Arb Engine (arb_hlbb).

Delta-neutral spread arb: SHORT on expensive venue, LONG on cheap venue.
Both legs are perpetual futures — no inventory risk, no spot holding.

Fee RT: HL taker 4.32bp + Bybit taker 5.5bp = 9.82bp/side × 2 = 19.64bp.
"""
