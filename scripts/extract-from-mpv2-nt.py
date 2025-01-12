#!/usr/bin/env python3

import os
import time
import sqlite3
from sqlite3 import Cursor


def extract_table_to(
        input_cursor: Cursor, table_name: str, keys: list[str],
        output_cursor: Cursor, output_table_name: str,
        batch_size: int = 100):    # 检查表是否存在
    try:
        # 检查输入表是否存在
        input_cursor.execute("""
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))

        if not input_cursor.fetchone():
            print(f"表 '{table_name}' 不存在于输入数据库中。")
            return

        print(f"正在导出表 '{table_name}'...")

        # 获取输入表的列类型信息
        input_cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = input_cursor.fetchall()
        columns_dict = {column[1]: column[2] for column in columns_info}  # {column_name: column_type}

        # 检查输出表是否存在
        output_cursor.execute("""
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (output_table_name,))

        if not output_cursor.fetchone():
            # 如果输出表不存在，则根据输入表的列类型创建表
            columns_def = ', '.join([f"{key} {columns_dict[key]}" for key in keys])
            create_table_query = f"CREATE TABLE {output_table_name} ({columns_def})"
            output_cursor.execute(create_table_query)
            output_cursor.connection.commit()
            print(f"表 '{output_table_name}' 已创建。")

        # 构建 SELECT 语句
        select_columns = ', '.join(keys)
        select_query = f"SELECT {select_columns} FROM {table_name}"

        # 构建 INSERT 语句
        placeholders = ', '.join('?' for _ in keys)
        insert_query = f"INSERT INTO {output_table_name} ({', '.join(keys)}) VALUES ({placeholders})"

        # 开始事务
        output_cursor.execute("BEGIN")

        # 分批次处理数据
        batch = []
        for row in input_cursor.execute(select_query):
            batch.append(row)
            if len(batch) >= batch_size:
                output_cursor.executemany(insert_query, batch)
                batch = []

        # 处理剩余的数据
        if batch:
            output_cursor.executemany(insert_query, batch)

        # 提交事务
        output_cursor.execute("COMMIT")
    except Exception as err:
        print(f'出错了：{err}')


if __name__ == '__main__':
    current_timestamp = int(time.time() * 1000)

    user_db = 'data/user.db'
    if not os.path.exists(user_db):
        print('请将 user.db 放入 ./data 目录中')
        exit(-1)

    input_conn = sqlite3.connect(user_db)
    input_cursor = input_conn.cursor()

    output_conn = sqlite3.connect(f'data/extracted-{current_timestamp}.db')
    output_cursor = output_conn.cursor()

    # 导出 nas-tools
    extract_table_to(
        input_cursor, 'DOWNLOAD_HISTORY',
        ['TITLE', 'YEAR', 'TYPE', 'TMDBID', 'TORRENT', 'SE', 'SAVE_PATH'],
        output_cursor, 'NT_DOWNLOAD_HISTORY'
    )
    extract_table_to(
        input_cursor, 'TRANSFER_HISTORY',
        ['TYPE', 'TMDBID', 'TITLE', 'YEAR', 'SEASON_EPISODE', 'SOURCE_FILENAME', 'DEST_FILENAME'],
        output_cursor, 'NT_TRANSFER_HISTORY'
    )
    # 导出 nas-tools 旧版
    extract_table_to(
        input_cursor, 'USERRSS_TASK_HISTORY',
        ['TITLE'],
        output_cursor, 'NT_TRANSFER_HISTORY'
    )
    # 导出 movie-pilot-v2
    extract_table_to(
        input_cursor, 'downloadhistory',
        ['torrent_name'],
        output_cursor, 'MPV2_DOWNLOAD_HISTORY'
    )
    extract_table_to(
        input_cursor, 'transferhistory',
        ['tmdbid', 'seasons', 'episodes', 'files'],
        output_cursor, 'MPV2_DOWNLOAD_HISTORY'
    )


    input_conn.close()
    output_conn.close()
