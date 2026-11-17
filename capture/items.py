from dataclasses import dataclass
from capture.catalog import require_unit


@dataclass(frozen=True)
class MonitoredItem:
    item_id: str
    device: str
    channel: str
    unit: str
    quantity: str

    @classmethod
    def from_mapping(cls, value):
        if not isinstance(value, dict):
            raise ValueError('item must be an object')
        try:
            result = cls(**value)
        except TypeError as exc:
            raise ValueError('invalid item fields') from exc
        for field in (result.item_id, result.device, result.channel):
            if not isinstance(field, str) or not field.strip():
                raise ValueError('item identifiers must be nonempty')
        require_unit(result.unit, result.quantity)
        return result


class ItemRegistry:
    def __init__(self, items):
        self.items = tuple(items)
        self.by_id = {}
        channels = set()
        for item in self.items:
            if item.item_id in self.by_id or (item.device, item.channel) in channels:
                raise ValueError('duplicate item or device channel')
            self.by_id[item.item_id] = item
            channels.add((item.device, item.channel))
        if not self.items:
            raise ValueError('at least one monitored item is required')

    def require_item(self, item_id):
        try:
            return self.by_id[item_id]
        except (KeyError, TypeError):
            raise ValueError('unknown monitored item') from None

    def require_device(self, device):
        if device not in {item.device for item in self.items}:
            raise ValueError('unknown device')
        return device
