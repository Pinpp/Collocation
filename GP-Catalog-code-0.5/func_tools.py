# -*- coding: utf-8 -*-

import json
import psycopg2


def load_params(json_file):
    with open(json_file, 'r') as read_file:
        pd_params = json.load(read_file)
    return pd_params

def con_db():
    db_conf = 'db_conf.json'
    pd_params = load_params(db_conf)
    try:
        db = psycopg2.connect(**pd_params['database'])
    except psycopg2.Error as e:
        print(e)
        return False
    else:
        return db

def close_db(db):
    db.close()

def sql_act(db, sql, n=1):
    # db = con_db()
    if db:
        # try:
            cur = db.cursor()
            cur.execute(sql)
            if n == 0:
                db.commit()
                cur.close()
                # db.close()
                return
            else:
                rows = cur.fetchall()
                cur.close()
                # db.close()
                return rows
        # except psycopg2.Error as e:
        #     print(e)
        #     return False
    else:
        print("\n\033[0;31mWARNING:\033[0m Connection to the db is Error.")
        return 0
