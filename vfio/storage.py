import numpy as np

from .sqlite_setup import sqlite3


class VideoFrameStorage:
    def __init__(self, path):
        self.__con = sqlite3.connect(path)
        self.__cur = self.__con.cursor()
        self.create_table()

    def create_table(self):
        self.__cur.execute(
            'CREATE TABLE IF NOT EXISTS frames'
            '('
            '    number    INTEGER PRIMARY KEY,'
            '    timestamp REAL,'
            '    image     array'
            ')'
        )
        self.__con.commit()

    def loaded_frame_indexes(self):
        a = np.array(self.__cur.execute('SELECT number FROM frames').fetchall())
        return a[:, 0] if a.size else np.array([])

    def entry_count(self):
        return self.__cur.execute('SELECT COUNT(*) FROM frames').fetchone()[0]

    def put(self, number, timestamp, image):
        self.__cur.execute(
            'INSERT INTO frames VALUES (?, ?, ?)',
            [
                number,
                timestamp,
                image,
            ]
        )

    def commit(self):
        self.__con.commit()

    def get(self, number):
        return self.__cur.execute(
            'SELECT number, timestamp, image FROM frames WHERE number = ?',
            [number]
        ).fetchone()

    def close(self):
        self.__cur.close()
        self.__con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
