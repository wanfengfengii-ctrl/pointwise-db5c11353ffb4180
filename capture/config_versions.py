"""Immutable monitored-configuration versions and late-arrival candidates."""
from dataclasses import asdict
import json
from capture.acknowledgments import queue_data_ack
from capture.frontier import advance_frontier
from capture.identity import canonical_observations
from capture.items import ItemRegistry, MonitoredItem
from capture.positions import SourcePosition
from capture.values import decode_observations


INITIAL_CONFIG_ID = 'initial'


def normalize_config_items(items):
    if not isinstance(items, (list, tuple)):
        raise ValueError('configuration items must be an ordered list')
    parsed = []
    for value in items:
        if isinstance(value, MonitoredItem):
            parsed.append(value)
        else:
            parsed.append(MonitoredItem.from_mapping(value))
    registry = ItemRegistry(parsed)
    return tuple(sorted(registry.items, key=lambda item: item.item_id))


def canonical_items(items):
    return json.dumps([asdict(item) for item in normalize_config_items(items)],
                      ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def items_from_canonical(text):
    value = json.loads(text)
    if not isinstance(value, list):
        raise ValueError('stored configuration is malformed')
    return normalize_config_items(value)


def load_version_items(con, subscription_id, config_id):
    row = con.execute('SELECT items FROM config_versions WHERE sub=? AND config_id=?',
                      (subscription_id, config_id)).fetchone()
    return None if row is None else items_from_canonical(row['items'])


def ensure_initial_version(con, subscription_id, items):
    encoded = canonical_items(items)
    row = con.execute('SELECT items FROM config_versions WHERE sub=? AND config_id=?',
                      (subscription_id, INITIAL_CONFIG_ID)).fetchone()
    if row is None:
        con.execute('INSERT INTO config_versions(sub,config_id,items) VALUES(?,?,?)',
                    (subscription_id, INITIAL_CONFIG_ID, encoded))
        return
    if row['items'] != encoded:
        raise ValueError('initial configuration version is inconsistent')


def register_config(handle, config_id, items):
    """Register one immutable version and interpret its waiting candidates."""
    if not isinstance(config_id, str) or not config_id:
        raise ValueError('config_id must be a nonempty string')
    encoded = canonical_items(items)
    registered = items_from_canonical(encoded)
    sub = handle.subscription_id
    promoted = []
    invalid = []
    with handle.db.transaction() as con:
        row = con.execute('SELECT items FROM config_versions WHERE sub=? AND config_id=?',
                          (sub, config_id)).fetchone()
        if row is not None:
            if row['items'] != encoded:
                raise ValueError('conflicting configuration version')
            return {'promoted': [], 'invalid': []}
        con.execute('INSERT INTO config_versions(sub,config_id,items) VALUES(?,?,?)',
                    (sub, config_id, encoded))
        waiting = con.execute(
            "SELECT pos,seq,payload FROM candidates WHERE sub=? AND config_id=? "
            "AND state='waiting' ORDER BY pos", (sub, config_id)).fetchall()
        for candidate in waiting:
            source_items = tuple(json.loads(candidate['payload']))
            try:
                observations = decode_observations(source_items, registered)
            except ValueError:
                con.execute("UPDATE candidates SET state='invalid' WHERE sub=? AND pos=?",
                            (sub, candidate['pos']))
                invalid.append(candidate['seq'])
                continue
            con.execute('INSERT INTO notifications(sub,pos,seq,payload,config_id) '
                        'VALUES(?,?,?,?,?)',
                        (sub, candidate['pos'], candidate['seq'],
                         canonical_observations(observations), config_id))
            con.execute('DELETE FROM candidates WHERE sub=? AND pos=?',
                        (sub, candidate['pos']))
            queue_data_ack(con, sub, SourcePosition(candidate['pos'], candidate['seq']))
            promoted.append(candidate['seq'])
        advance_frontier(con, sub)
    return {'promoted': promoted, 'invalid': invalid}


def stored_versions(con, subscription_id):
    return tuple((row['config_id'], items_from_canonical(row['items']))
                 for row in con.execute(
                     'SELECT config_id,items FROM config_versions WHERE sub=? '
                     'ORDER BY config_id', (subscription_id,)))


def load_all_versions(con, subscription_id, initial_items):
    """Return config_id -> ordered items, always including the initial version."""
    versions = {
        row['config_id']: items_from_canonical(row['items'])
        for row in con.execute('SELECT config_id,items FROM config_versions WHERE sub=?',
                               (subscription_id,))
    }
    stored_initial = versions.get(INITIAL_CONFIG_ID)
    if stored_initial is not None and canonical_items(stored_initial) != canonical_items(initial_items):
        raise ValueError('initial configuration version is inconsistent')
    versions[INITIAL_CONFIG_ID] = normalize_config_items(initial_items)
    return versions


__all__ = ['INITIAL_CONFIG_ID', 'normalize_config_items', 'canonical_items',
           'items_from_canonical', 'load_version_items', 'ensure_initial_version',
           'register_config', 'stored_versions', 'load_all_versions']
