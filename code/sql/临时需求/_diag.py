# -*- coding: utf-8 -*-
"""诊断活跃表数据"""
import sys, json
from sshtunnel import SSHTunnelForwarder
from impala.dbapi import connect as impala_connect

sys.stdout.reconfigure(encoding='utf-8')

cfg_path = '.cursor/skills/jump-sql-excel-export/jump_export_config.json'
with open(cfg_path, 'r', encoding='utf-8') as f:
    cfg = json.load(f)
cfg.setdefault('db_user', cfg.get('username'))
cfg.setdefault('db_password', cfg.get('password'))

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

    # 诊断1: 活跃表 active_user_attribution 分布
    sql1 = (
        "SELECT active_user_attribution, count(distinct u_user) as cnt "
        "FROM dws.topic_user_active_detail_day "
        "WHERE day = 20260101 "
        "AND product_id = '01' "
        "AND client_os IN ('android', 'ios', 'harmony') "
        "GROUP BY active_user_attribution"
    )
    print('=== 诊断1: active_user_attribution 分布 (2026-01-01) ===')
    cur.execute(sql1)
    for row in cur.fetchall():
        print(row)

    # 诊断2: 小学活跃用户 role 分布
    sql2 = (
        "SELECT role, count(distinct u_user) as cnt "
        "FROM dws.topic_user_active_detail_day "
        "WHERE day = 20260101 "
        "AND product_id = '01' "
        "AND client_os IN ('android', 'ios', 'harmony') "
        "AND active_user_attribution = '小学用户' "
        "GROUP BY role"
    )
    print('\n=== 诊断2: 小学活跃用户 role 分布 ===')
    cur.execute(sql2)
    for row in cur.fetchall():
        print(row)

    # 诊断3: 小学活跃用户中新注册用户量
    sql3 = (
        "SELECT count(distinct a.u_user) as cnt "
        "FROM dws.topic_user_active_detail_day a "
        "JOIN dw.dim_user b ON a.u_user = b.u_user "
        "WHERE a.day = 20260101 "
        "AND a.product_id = '01' "
        "AND a.client_os IN ('android', 'ios', 'harmony') "
        "AND a.active_user_attribution = '小学用户' "
        "AND substr(b.regist_time, 1, 7) BETWEEN '2025-02' AND '2026-01'"
    )
    print('\n=== 诊断3: 小学活跃+近一年注册用户量 ===')
    cur.execute(sql3)
    for row in cur.fetchall():
        print(row)

    conn.close()
