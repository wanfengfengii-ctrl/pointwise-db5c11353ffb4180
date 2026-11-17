from dataclasses import asdict, dataclass
import json
from capture.items import ItemRegistry, MonitoredItem
from capture.positions import validate_sequence


@dataclass(frozen=True)
class SubscriptionSpec:
    source_id: str
    subscription_id: str
    initial_sequence: int
    items: tuple
    version: int = 1


def validate_spec(value):
    if isinstance(value, SubscriptionSpec):
        value = asdict(value)
    if not isinstance(value, dict):
        raise ValueError('subscription spec must be an object')
    if set(value) - {'source_id', 'subscription_id', 'initial_sequence', 'items', 'version'}:
        raise ValueError('unknown subscription fields')
    try:
        source, subscription = value['source_id'], value['subscription_id']
        initial = validate_sequence(value['initial_sequence'])
        items = tuple(MonitoredItem.from_mapping(item) for item in value['items'])
    except (KeyError, TypeError) as exc:
        raise ValueError('invalid subscription spec') from exc
    if any(not isinstance(v, str) or not v.strip() for v in (source, subscription)):
        raise ValueError('source and subscription must be nonempty strings')
    if type(value.get('version', 1)) is not int or value.get('version', 1) != 1:
        raise ValueError('unsupported configuration version')
    registry = ItemRegistry(items)
    return SubscriptionSpec(source, subscription, initial,
                            tuple(sorted(registry.items, key=lambda item: item.item_id)))


def canonical_spec(spec):
    return json.dumps(asdict(validate_spec(spec)), ensure_ascii=False,
                      sort_keys=True, separators=(',', ':'))


def require_compatible_spec(existing, requested):
    if canonical_spec(existing) != canonical_spec(requested):
        raise ValueError('incompatible subscription configuration')
