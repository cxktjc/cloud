"""Freeze which valid quote(s) satisfy this exact entry's distance ceiling."""
from ..entry_policy import at_most


def qualify_quote(quote, limit, ceiling, *, following=False):
    gaps = {key: (quote[key]-limit)/limit
            if quote.get(key) is not None and limit > 0 else None for key in ('tick', 'bid0')}
    valid = limit > 0 and any(quote.get(key) is not None for key in gaps)
    tick = gaps['tick'] is not None and ceiling is not None and at_most(gaps['tick'], ceiling)
    depth = gaps['bid0'] is not None and ceiling is not None and at_most(gaps['bid0'], ceiling)
    label = ('signal_follow' if valid else 'none') if following else (
        'all' if tick and depth else 'tick' if tick else 'depth' if depth else 'none')
    return dict(entry_qualified_by=label, entry_quote_source=quote.get('source'),
                entry_tick=quote.get('tick'), entry_bid0=quote.get('bid0'),
                entry_tick_gap=gaps['tick'], entry_depth_gap=gaps['bid0'],
                entry_gap_ceiling=None if following else ceiling)


QUOTE_FIELDS = ('entry_qualified_by', 'entry_quote_source', 'entry_tick', 'entry_bid0',
                'entry_tick_gap', 'entry_depth_gap', 'entry_gap_ceiling')
