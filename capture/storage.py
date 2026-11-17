from contextlib import contextmanager
import sqlite3


SCHEMA = '''
CREATE TABLE IF NOT EXISTS subscriptions(
 id TEXT PRIMARY KEY, source TEXT NOT NULL, config TEXT NOT NULL,
 initial INTEGER NOT NULL, frontier INTEGER NOT NULL DEFAULT 0,
 high INTEGER NOT NULL DEFAULT 0, CHECK(frontier >= 0 AND high >= frontier));
CREATE TABLE IF NOT EXISTS config_versions(
 sub TEXT NOT NULL REFERENCES subscriptions(id), config_id TEXT NOT NULL CHECK(length(config_id) > 0),
 items TEXT NOT NULL, PRIMARY KEY(sub,config_id));
CREATE TABLE IF NOT EXISTS notifications(
 sub TEXT NOT NULL REFERENCES subscriptions(id), pos INTEGER NOT NULL CHECK(pos > 0),
 seq INTEGER NOT NULL CHECK(seq BETWEEN 1 AND 4294967295), payload TEXT NOT NULL,
 config_id TEXT NOT NULL DEFAULT 'initial' CHECK(length(config_id) > 0),
 PRIMARY KEY(sub,pos));
CREATE TABLE IF NOT EXISTS candidates(
 sub TEXT NOT NULL REFERENCES subscriptions(id), pos INTEGER NOT NULL CHECK(pos > 0),
 seq INTEGER NOT NULL, config_id TEXT NOT NULL CHECK(length(config_id) > 0),
 state TEXT NOT NULL CHECK(state IN ('waiting','invalid')), payload TEXT NOT NULL,
 PRIMARY KEY(sub,pos));
CREATE TABLE IF NOT EXISTS pending(
 sub TEXT NOT NULL REFERENCES subscriptions(id), pos INTEGER NOT NULL CHECK(pos > 0),
 seq INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(sub,pos));
CREATE TABLE IF NOT EXISTS gaps(
 sub TEXT NOT NULL REFERENCES subscriptions(id), pos INTEGER NOT NULL CHECK(pos > 0),
 seq INTEGER NOT NULL, source TEXT NOT NULL, reason TEXT NOT NULL,
 PRIMARY KEY(sub,pos));
CREATE TABLE IF NOT EXISTS acknowledgments(
 sub TEXT NOT NULL, pos INTEGER NOT NULL, seq INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','done')),
 attempts INTEGER NOT NULL DEFAULT 0, receipt TEXT,
 PRIMARY KEY(sub,pos), FOREIGN KEY(sub,pos) REFERENCES notifications(sub,pos));
'''


class CaptureDatabase:
    def __init__(self, path):
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, timeout=1)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.before_commit = None
        self.initialize()

    def initialize(self):
        self.connection.executescript(SCHEMA)
        self._migrate()

    def _migrate(self):
        columns = {row['name'] for row in
                   self.connection.execute('PRAGMA table_info(notifications)')}
        if 'config_id' not in columns:
            # Databases written before configuration versions keep initial bindings.
            self.connection.execute(
                "ALTER TABLE notifications ADD COLUMN config_id TEXT NOT NULL DEFAULT 'initial'")

    @contextmanager
    def transaction(self):
        if self.connection.in_transaction:
            raise RuntimeError('nested transaction')
        self.connection.execute('BEGIN IMMEDIATE')
        try:
            yield self.connection
            if self.before_commit is not None:
                self.before_commit()
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    @contextmanager
    def snapshot(self):
        if self.connection.in_transaction:
            raise RuntimeError('nested snapshot')
        self.connection.execute('BEGIN')
        try:
            yield self.connection
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
