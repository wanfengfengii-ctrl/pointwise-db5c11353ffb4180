from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
from capture.items import ItemRegistry


SOURCE_FIELDS = ('item_id', 'value', 'quality', 'timestamp')


@dataclass(frozen=True)
class Observation:
    item_id: str
    device: str
    channel: str
    unit: str
    value: str | None
    quality: str
    timestamp: str | None


def decode_source_item(value):
    """Validate one source item without consulting any monitored mapping."""
    if not isinstance(value, dict) or set(value) - set(SOURCE_FIELDS):
        raise ValueError('invalid observation fields')
    try:
        item_id = value['item_id']
        quality = value['quality']
        number = value['value']
    except (KeyError, TypeError) as exc:
        raise ValueError('incomplete observation') from exc
    if not isinstance(item_id, str) or not item_id.strip():
        raise ValueError('item identifiers must be nonempty')
    if quality not in ('Good', 'Bad', 'Uncertain'):
        raise ValueError('invalid quality')
    if number is None:
        if quality == 'Good':
            raise ValueError('Good observations need a value')
    else:
        if not isinstance(number, str) or not number or number.strip() != number:
            raise ValueError('decimal value must be text')
        try:
            decimal = Decimal(number)
        except InvalidOperation as exc:
            raise ValueError('invalid decimal') from exc
        if not decimal.is_finite():
            raise ValueError('nonfinite decimal')
    timestamp = value.get('timestamp')
    if timestamp is not None:
        if not isinstance(timestamp, str):
            raise ValueError('timestamp must be text or null')
        try:
            parsed = datetime.fromisoformat(timestamp)
        except ValueError as exc:
            raise ValueError('invalid timestamp') from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError('timestamp must include timezone')
    return {'item_id': item_id, 'value': number, 'quality': quality, 'timestamp': timestamp}


def decode_source_items(items):
    if not isinstance(items, (list, tuple)):
        raise ValueError('notification items must be an ordered list')
    return tuple(decode_source_item(item) for item in items)


def decode_observation(value, registry):
    source = decode_source_item(value)
    item = registry.require_item(source['item_id'])
    return Observation(item.item_id, item.device, item.channel, item.unit,
                       source['value'], source['quality'], source['timestamp'])


def decode_observations(source_items, config_items):
    registry = ItemRegistry(config_items)
    return tuple(decode_observation(item, registry) for item in source_items)


def source_from_observation(observation):
    return {'item_id': observation.item_id, 'value': observation.value,
            'quality': observation.quality, 'timestamp': observation.timestamp}


def encode_source_items(items):
    return json.dumps([{key: item[key] for key in SOURCE_FIELDS} for item in items],
                      ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def encode_observation(observation):
    return asdict(observation)
