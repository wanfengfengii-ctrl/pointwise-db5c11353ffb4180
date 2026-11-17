from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class Observation:
    item_id: str
    device: str
    channel: str
    unit: str
    value: str | None
    quality: str
    timestamp: str | None


def decode_observation(value, registry):
    if not isinstance(value, dict) or set(value) - {'item_id', 'value', 'quality', 'timestamp'}:
        raise ValueError('invalid observation fields')
    try:
        item = registry.require_item(value['item_id'])
        quality = value['quality']
        number = value['value']
    except (KeyError, TypeError) as exc:
        raise ValueError('incomplete observation') from exc
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
    return Observation(item.item_id, item.device, item.channel, item.unit,
                       number, quality, timestamp)


def encode_observation(observation):
    return asdict(observation)
