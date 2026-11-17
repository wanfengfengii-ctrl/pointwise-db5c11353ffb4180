from dataclasses import dataclass


@dataclass(frozen=True)
class UnitDefinition:
    symbol: str
    quantity: str


UNIT_CATALOG = {
    'mm/s': UnitDefinition('mm/s', 'velocity'),
    'm/s2': UnitDefinition('m/s2', 'acceleration'),
}


def require_unit(symbol, quantity=None):
    try:
        unit = UNIT_CATALOG[symbol]
    except (KeyError, TypeError):
        raise ValueError('unknown unit') from None
    if quantity is not None and quantity != unit.quantity:
        raise ValueError('unit does not match quantity')
    return unit
