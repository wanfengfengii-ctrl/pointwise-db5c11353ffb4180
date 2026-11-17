"""Notification identity: source content, binding and first-text preservation."""
import json
from decimal import Decimal
from capture.values import SOURCE_FIELDS, encode_observation


class NotificationConflict(ValueError):
    pass


def canonical_observations(observations):
    return json.dumps([encode_observation(item) for item in observations],
                      ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def canonical_payload(notification):
    return canonical_observations(notification.observations)


def canonical_source_items(items):
    return json.dumps([{key: item[key] for key in SOURCE_FIELDS} for item in items],
                      ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def ordered_source_items(items):
    if isinstance(items, str):
        items = json.loads(items)
    return tuple({key: item[key] for key in SOURCE_FIELDS} for item in items)


def same_source_items(left, right):
    """Ordered identity: decimals compare by value; quality, text and order matter."""
    return _comparable_source_items(left) == _comparable_source_items(right)


def _comparable_source_items(items):
    return tuple(
        (item['item_id'],
         None if item['value'] is None else Decimal(item['value']),
         item['quality'], item['timestamp'])
        for item in ordered_source_items(items))


def same_content(left, right):
    def comparable(payload):
        result = []
        for item in json.loads(payload):
            fields = dict(item)
            if fields['value'] is not None:
                fields['value'] = Decimal(fields['value'])
            result.append(fields)
        return result
    return comparable(left) == comparable(right)
