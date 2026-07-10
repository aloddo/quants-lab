"""
Formatting helpers for prices, durations, and PnL.
"""


def fp(value: float, min_sig: int = 4) -> str:
    """Format price with adaptive precision.
    
    Ensures at least `min_sig` significant digits are visible.
    Examples:
        fp(65432.10)   -> "65432.10"
        fp(0.0016382)  -> "0.001638"
        fp(0.00000123) -> "0.000001230"
        fp(1.23)       -> "1.2300"
    """
    if value is None:
        return "N/A"
    if value == 0:
        return "0.00"
    
    abs_val = abs(value)
    
    if abs_val >= 1.0:
        # Normal prices: 2-4 decimals
        if abs_val >= 100:
            return f"{value:.2f}"
        elif abs_val >= 1:
            return f"{value:.4f}"
    
    # Sub-dollar: count leading zeros after decimal, then show min_sig digits
    # e.g. 0.0016382 has 2 leading zeros -> need 2 + min_sig = 6 decimals
    import math
    leading_zeros = -math.floor(math.log10(abs_val)) - 1
    decimals = max(leading_zeros + min_sig, 2)
    return f"{value:.{decimals}f}"


def fmt_duration(seconds: float) -> str:
    """Format seconds into human-readable duration.

    Examples:
        fmt_duration(86400)  -> "24h"
        fmt_duration(23580)  -> "6h 33m"
        fmt_duration(135)    -> "2m 15s"
        fmt_duration(45)     -> "45s"
    """
    if seconds is None or seconds < 0:
        return "N/A"
    s = int(seconds)
    if s >= 86400:
        d, rem = divmod(s, 86400)
        h = rem // 3600
        return f"{d}d {h}h" if h else f"{d * 24}h"
    if s >= 3600:
        h, rem = divmod(s, 3600)
        m = rem // 60
        return f"{h}h {m}m" if m else f"{h}h"
    if s >= 60:
        m, rem = divmod(s, 60)
        return f"{m}m {rem}s" if rem else f"{m}m"
    return f"{s}s"


def fmt_pnl(pnl: float, amount_usd: float = None) -> str:
    """Format PnL with sign and optional percentage.

    Examples:
        fmt_pnl(17.3, 300)   -> "+$17.30 (+5.77%)"
        fmt_pnl(-3.2, 300)   -> "-$3.20 (-1.07%)"
        fmt_pnl(17.3)        -> "+$17.30"
    """
    if pnl is None:
        return "N/A"
    sign = "+" if pnl >= 0 else "-"
    base = f"{sign}${abs(pnl):.2f}"
    if amount_usd and amount_usd > 0:
        pct = abs(pnl / amount_usd * 100)
        return f"{base} ({sign}{pct:.2f}%)"
    return base


def fmt_pct(value: float, signed: bool = True) -> str:
    """Format a decimal as percentage.

    Examples:
        fmt_pct(0.03)        -> "+3.00%"
        fmt_pct(-0.015)      -> "-1.50%"
        fmt_pct(0.03, False) -> "3.00%"
    """
    if value is None:
        return "N/A"
    pct = value * 100
    if signed:
        sign = "+" if pct >= 0 else ""
        return f"{sign}{pct:.2f}%"
    return f"{pct:.2f}%"
