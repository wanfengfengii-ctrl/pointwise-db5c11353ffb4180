import json
from decimal import Decimal
from capture.values import encode_observation


class NotificationConflict(ValueError):
    pass


def canonical_payload(notification):
    return json.dumps([encode_observation(item) for item in notification.observations],
                      ensure_ascii=False, sort_keys=True, separators=(',', ':'))


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
