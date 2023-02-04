import datetime
import logging
import sqlite3

from core.message import Sample


logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.db = sqlite3.connect("aggregator.db")

        self.db.execute(
            """
            PRAGMA journal_mode=WAL;
            """
        )

        self.create_tables()

        self.tag_cache = {}
        # load tags from database
        for row in self.db.execute("SELECT id, name FROM tags;"):
            self.tag_cache[row[1]] = row[0]

    def create_tables(self):
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        """
        )

        self.db.execute(
            """CREATE TABLE IF NOT EXISTS measurements (
                tag_id INTEGER NOT NULL,
                value REAL NOT NULL,
                timestamp INTEGER NOT NULL
            );"""
        )

        # Create index on tag_id and timestamp
        self.db.execute(
            """CREATE INDEX IF NOT EXISTS measurements_tag_id_timestamp_idx
            ON measurements (tag_id, timestamp);"""
        )

        self.db.execute(
            """CREATE TABLE IF NOT EXISTS measurements_1min (
                tag_id INTEGER NOT NULL,
                min REAL NOT NULL,
                max REAL NOT NULL,
                avg REAL NOT NULL,
                count INTEGER NOT NULL,
                timestamp INTEGER NOT NULL
            );"""
        )

        self.db.execute(
            """CREATE INDEX IF NOT EXISTS measurements_1min_tag_id_timestamp_idx
            ON measurements_1min (tag_id, timestamp);"""
        )

        self.db.commit()

    def store_measurement(self, sample: Sample):
        tag_id = self._get_or_create_tag_id(sample.tag)

        self.db.execute(
            "INSERT INTO measurements (tag_id, value, timestamp) VALUES (?, ?, ?)",
            (tag_id, sample.value, sample.timestamp.timestamp()),
        )

        self.db.commit()

    def _get_or_create_tag_id(self, tag: str):
        if tag in self.tag_cache:
            return self.tag_cache[tag]

        cursor = self.db.execute("SELECT id FROM tags WHERE name = ?", (tag,))

        row = cursor.fetchone()

        if row:
            return row[0]

        cursor = self.db.execute("INSERT INTO tags (name) VALUES (?)", (tag,))

        self.tag_cache[tag] = cursor.lastrowid

        return cursor.lastrowid

    def get_aggregates_for_minute(self, timestamp: datetime):
        for tag in self.tag_cache:
            logger.info(f"Aggregating {tag}")
            yield tag, self._get_aggregates_for_minute(tag, timestamp)

    def _get_aggregates_for_minute(self, tag, timestamp):
        tag_id = self.tag_cache[tag]
        start_of_minute = timestamp.replace(second=0, microsecond=0)
        end_of_minute = start_of_minute + datetime.timedelta(minutes=1)

        logger.debug(f"Aggregating {tag} for {start_of_minute} - {end_of_minute}")

        cursor = self.db.execute(
            """
            SELECT
                tag_id,
                AVG(value) AS avg,
                MIN(value) AS min,
                MAX(value) AS max,
                COUNT(*) AS count
            FROM measurements
            WHERE tag_id = ? AND timestamp >= ? AND timestamp < ?;
            """,
            (tag_id, start_of_minute.timestamp(), end_of_minute.timestamp()),
        )

        row = cursor.fetchone()

        return {
            "tag_id": row[0],
            "avg": row[1],
            "min": row[2],
            "max": row[3],
            "count": row[4],
        }

    def remove_old_measurements(self, timestamp: datetime):
        self.db.execute(
            """
            DELETE FROM measurements
            WHERE timestamp < ?;
            """,
            (timestamp.timestamp(),),
        )

        self.db.commit()

    def store_aggregate(self, timestamp, aggregate):
        # if we have no measurements for this minute, we don't need to store anything
        if aggregate["count"] == 0:
            return

        # if we already have an aggregate for this minute, update it
        cursor = self.db.execute(
            """
            SELECT tag_id, timestamp FROM measurements_1min
            WHERE tag_id = ? AND timestamp = ?;
            """,
            (aggregate["tag_id"], timestamp.timestamp()),
        )

        row = cursor.fetchone()

        if row:
            logger.debug(f"Updating aggregate for {timestamp}")
            self.db.execute(
                """
                UPDATE measurements_1min
                SET min = ?, max = ?, avg = ?, count = ?
                WHERE tag_id = ? and timestamp = ?;
                """,
                (
                    aggregate["min"],
                    aggregate["max"],
                    aggregate["avg"],
                    aggregate["count"],
                    row[0],
                    row[1],
                ),
            )
            self.db.commit()
            return

        logger.debug(f"Storing aggregate for {timestamp}")
        self.db.execute(
            """
            INSERT INTO measurements_1min (tag_id, min, max, avg, count, timestamp)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                aggregate["tag_id"],
                aggregate["min"],
                aggregate["max"],
                aggregate["avg"],
                aggregate["count"],
                timestamp.timestamp(),
            ),
        )

        self.db.commit()
