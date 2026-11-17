from dataclasses import dataclass
import json
from capture.config_versions import ensure_initial_version
from capture.configuration import canonical_spec, require_compatible_spec, validate_spec
from capture.items import ItemRegistry


@dataclass(frozen=True)
class SubscriptionHandle:
    db: object
    spec: object

    @property
    def subscription_id(self):
        return self.spec.subscription_id

    @property
    def registry(self):
        return ItemRegistry(self.spec.items)

    def require_source(self, port):
        if port.source_id != self.spec.source_id:
            raise ValueError('source identity mismatch')
        if port.subscription_id != self.subscription_id:
            raise ValueError('subscription identity mismatch')


def configure(db, subscription_spec):
    spec = validate_spec(subscription_spec)
    encoded = canonical_spec(spec)
    with db.transaction() as con:
        old = con.execute('SELECT config FROM subscriptions WHERE id=?',
                          (spec.subscription_id,)).fetchone()
        if old:
            require_compatible_spec(json.loads(old['config']), spec)
        else:
            con.execute('INSERT INTO subscriptions(id,source,config,initial) VALUES(?,?,?,?)',
                        (spec.subscription_id, spec.source_id, encoded, spec.initial_sequence))
        ensure_initial_version(con, spec.subscription_id, spec.items)
    return SubscriptionHandle(db, spec)


def load_subscription(db, subscription_id):
    row = db.connection.execute('SELECT config FROM subscriptions WHERE id=?',
                                (subscription_id,)).fetchone()
    if row is None:
        raise ValueError('unknown subscription')
    return SubscriptionHandle(db, validate_spec(json.loads(row['config'])))
