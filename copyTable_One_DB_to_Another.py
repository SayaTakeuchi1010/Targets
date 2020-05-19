import os
import sqlite3

def sqlite_db(path):

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    # test if this is really sqlite file
    cur = conn.cursor()
    cur.execute('SELECT 1 from sqlite_master where type = "table"')
    try:
        data = cur.fetchone()
    except sqlite3.DatabaseError:
        msg = '%s can\'t be read as SQLite DB' % path

    return conn



src_db_ = sqlite_db(r'C:\Users\saya6\Desktop\target db\migrating table\SYSTEM_without_TARGETS.db')
dst_db_ = sqlite_db(r'C:\Users\saya6\Desktop\target db\migrating table\Target_measured_data_200515.db')
tableName_ = 'PORTS'
def copy_table_to_another_db(src_db=src_db_,dst_db=dst_db_,tableName=tableName_):

    src_cur = src_db.cursor()
    dst_cur = dst_db.cursor()

    src_cur.execute('SELECT * from sqlite_master')
    src_master = src_cur.fetchall()

    src_tables = filter(lambda r: r['type'] == 'table', src_master)
    src_indices = filter(lambda r: r['type'] == 'index' and r['sql'] != None, src_master)

    # logger.info('Found tables: %d', len(src_tables))
    for table in src_tables:
        # logger.info('Processing table: %s', table['name'])
        #
        # logger.info('Delete old table in destination db, if exists')
        if table['name']==tableName:
            dst_cur.execute("DROP TABLE IF EXISTS " + table['name'])

            # logger.info('Creating table structure')
            # logger.debug('SQL: %s', table['sql'])
            dst_cur.execute(table['sql'])

            # logger.info('Moving data')
            src_cur.execute('SELECT COUNT(1) AS cnt FROM %s' % table['name'])
            total_rows = src_cur.fetchone()['cnt']
            # logger.debug('Rows: %d', total_rows)

            src_cur.execute('SELECT * FROM %s' % table['name'])
            item = 0
            for row in src_cur:
                item += 1
                if item % 50000 == 0:
                    # logger.debug('Processing %d / %d', item, total_rows)
                    dst_db.commit()

                cols = row.keys()
                query = 'INSERT INTO %(tbl)s (%(cols)s) VALUES (%(phold)s)' % {
                    'tbl': table['name'],
                    'cols': ','.join(cols),
                    'phold': ','.join(('?',) * len(cols))
                }
                dst_cur.execute(query, [row[col] for col in cols])

            dst_db.commit()


            table_idx = filter(lambda r: r['tbl_name'] == table['name'], src_indices)
            for idx in table_idx:
                dst_cur.execute(idx['sql'])


    src_db.close()
    dst_db.close()

result=copy_table_to_another_db(src_db_, dst_db_, tableName_)