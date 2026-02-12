# -*- coding: utf-8 -*-
"""查询 dim_user 表中 role 和 real_identity 的枚举值分布（2024年之后注册的用户）"""
import sys, json
from sshtunnel import SSHTunnelForwarder
from impala.dbapi import connect as impala_connect

sys.stdout.reconfigure(encoding='utf-8')

cfg_path = '.cursor/skills/jump-sql-excel-export/jump_export_config.json'
with open(cfg_path, 'r', encoding='utf-8') as f:
    cfg = json.load(f)
cfg.setdefault('db_user', cfg.get('username'))
cfg.setdefault('db_password', cfg.get('password'))

REGIST_FILTER = "AND substr(cast(regist_time as string), 1, 10) >= '2024-01-01'"

with SSHTunnelForwarder(
    (cfg['ssh_host'], 22),
    ssh_username=cfg['ssh_user'],
    ssh_password=cfg['ssh_password'],
    remote_bind_address=(cfg['db_host'], cfg['db_port']),
    local_bind_address=('127.0.0.1', 0)
) as tunnel:
    conn = impala_connect(host='127.0.0.1', port=tunnel.local_bind_port,
        auth_mechanism='PLAIN', database='tmp',
        user=cfg['db_user'], password=cfg['db_password'])
    cur = conn.cursor(dictify=True)

    # 1. dim_user 的 role 枚举值（2024年后注册）
    print('=== dw.dim_user role 枚举值 (2024年后注册) ===')
    cur.execute(f"SELECT role, count(1) as cnt FROM dw.dim_user WHERE 1=1 {REGIST_FILTER} GROUP BY role ORDER BY cnt DESC")
    for row in cur.fetchall():
        print(f"  {row['role']!s:20s}  {row['cnt']:>12,}")

    # 2. dim_user 的 real_identity 枚举值（2024年后注册）
    print('\n=== dw.dim_user real_identity 枚举值 (2024年后注册) ===')
    cur.execute(f"SELECT real_identity, count(1) as cnt FROM dw.dim_user WHERE 1=1 {REGIST_FILTER} GROUP BY real_identity ORDER BY cnt DESC")
    for row in cur.fetchall():
        print(f"  {row['real_identity']!s:20s}  {row['cnt']:>12,}")

    # 3. role 和 real_identity 交叉分布（2024年后注册）
    print('\n=== role × real_identity 交叉分布 (2024年后注册) ===')
    cur.execute(
        f"SELECT role, real_identity, count(1) as cnt "
        f"FROM dw.dim_user WHERE 1=1 {REGIST_FILTER} "
        f"GROUP BY role, real_identity ORDER BY role, cnt DESC"
    )
    for row in cur.fetchall():
        print(f"  {row['role']!s:12s}  {row['real_identity']!s:20s}  {row['cnt']:>12,}")

    conn.close()
