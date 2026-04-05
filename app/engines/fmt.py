"""
Smart price formatter for sub-penny tokens.
Adapts decimal places based on price magnitude.
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
