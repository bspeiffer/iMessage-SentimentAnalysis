
import tornado.options
import glob
import os
import sqlite3
import logging
import datetime
import csv

# http://theiphonewiki.com/wiki/IMessage
MADRID_OFFSET = 978307200
MADRID_FLAGS_SENT = [36869, 45061]

# not sure this is really needed to reformat
def _utf8(s):
    if isinstance(s, str):
        s = s.encode('utf-8')
    assert isinstance(s, bytes)
    return s

def dict_factory(cursor, row):
    d = {}
    for idx,col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class DB(object):
    def __init__(self, *args, **kwargs):
        self._db = sqlite3.connect(*args, **kwargs)
        self._db.row_factory = dict_factory
    
    def query(self, sql, params=None):
        try:
            c = self._db.cursor()
            c.execute(sql, params or [])
            res = c.fetchall()
            self._db.commit()
        except:
            if self._db:
                self._db.rollback()
            raise
        
        c.close()
        return res


def extract_messages(db_file):
    db = DB(db_file)
    skipped = 0
    found = 0
    errored = 0
    for row in db.query('SELECT datetime (message.date / 1000000000 + strftime ("%s", "2001-01-01"), "unixepoch", "localtime") AS message_date, message.text, message.is_from_me, chat.chat_identifier FROM chat JOIN chat_message_join ON chat. "ROWID" = chat_message_join.chat_id JOIN message ON chat_message_join.message_id = message. "ROWID" ORDER BY message_date ASC'):
        try:
            m = parse_row(row)
        except:
            logging.exception('failed on %r', row)
            errored += 1
            if errored > 10:
                raise
        if m:
            found += 1
            yield m
        else:
            skipped += 1
    logging.info('found %d skipped %d', found, skipped,)

def parse_row(row):
    ts = row['message_date']
    
    if not row['text']:
        return

    # this date convertion didnt work -- Brent 12/3
    dt = datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
    logging.debug('[%s] %r %r', dt, row.get('text'), row)

    # if "is_madrid" in row and row['is_madrid']:
    #     sent = row['madrid_flags'] in MADRID_FLAGS_SENT
    # elif 'flags' in row:
    #     sent = row['flags'] in [3, 35]
    # else:
    sent = row['is_from_me'] == 1

    if tornado.options.options.sent_only and not sent:
        return
    if dt.year != tornado.options.options.year:
        return
    
    return dict(
        is_from_me='1' if sent else '0',
        # service=service,
        # subject=_utf8(row['subject'] or ''),
        text=str(row['text'] or ''),
        message_date=ts,
        chat_identifier=row['chat_identifier']
    )
    
    
def run():
    assert not os.path.exists(tornado.options.options.output_file)
    logging.info('writing out to %s', tornado.options.options.output_file)
    f = open(tornado.options.options.output_file, 'w')
    columns = ["message_date", "text", "is_from_me", "chat_identifier"]
    writer = csv.DictWriter(f, columns)
    writer.writeheader()
    pattern = os.path.expanduser(tornado.options.options.input_pattern)
    for db_file in glob.glob(pattern):
        logging.info("reading %r. use --input-patern to select only this file", db_file)
        for row in extract_messages(db_file):
            if tornado.options.options.exclude_message_text:
                row['text'] = ''
            writer.writerow(row)
    f.close()

if __name__ == "__main__":
    tornado.options.define("input_pattern", type=str, default="~/Library/Messages/chat.db")
    tornado.options.define("output_file", type=str, default="txt_messages.csv")
    tornado.options.define("year", type=int, default=2019)
    tornado.options.define("sent_only", type=bool, default=False)
    tornado.options.define("exclude_message_text", type=bool, default=False)
    tornado.options.parse_command_line()
    run()