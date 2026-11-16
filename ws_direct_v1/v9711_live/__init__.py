"""v9711 local-data signal-authoritative live engineering port.

Signal maintains its own strategy targets/PnL, without an exchange connection.
The separate execution adapter is not started by importing this package; it
keeps the schema-1 order reader; full target-position reconciliation requires
independent implementation and acceptance before claiming alignment.
"""
from . import config, contracts, market_data, publisher, signal_diagnostics, signal_logic, signal_portfolio, terminal_view

__all__ = ['config', 'contracts', 'market_data', 'publisher',
           'signal_diagnostics', 'signal_logic', 'signal_portfolio', 'terminal_view']

from . import storage
