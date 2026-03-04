# -*- coding: utf-8 -*-
"""
批量执行"指定一线线索领取明细转化明细.sql"——
将 118 个 worker_name 拆成 10 批，复用同一条 SSH 隧道依次执行，
最终合并为一个 Excel。
"""
import os
import sys
import math
import time
import json

import pandas as pd
from sshtunnel import SSHTunnelForwarder
from impala.dbapi import connect as impala_connect


# ---------- 路径 ----------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..', '..'))
CONFIG_PATH = os.path.join(PROJECT_ROOT, '.cursor', 'skills',
                           'jump-sql-excel-export', 'jump_export_config.json')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'output')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, '指定一线线索领取明细转化明细_202512_202601.xlsx')

BATCH_COUNT = 10  # 拆分批次数


# ---------- SQL 模板 ----------
SQL_TEMPLATE = """
with t1 as (
  select
    a.user_id
    ,substr(a.created_at,1,19) created_at
    ,substr(a.created_at,1,7) created_ym
    ,user_type_name
    ,clue_stage
    ,clue_grade
    ,b.clue_source_name
    ,b.clue_source_name_level_1
    ,worker_id
    ,worker_name
    ,workplace_id
    ,department_id
    ,regiment_id
    ,team_id
    ,c.phone
  from aws.clue_info a
  left join tmp.wuhan_clue_soure_name b on a.clue_source = b.clue_source
  left join
  (
    select
    u_user,if(phone is null,phone,if(phone rlike "^\\\\d+$",phone,cast(unbase64(phone) as string))) AS phone
    from dw.dim_user
    where length(phone)>0
  ) c
    on a.user_id = c.u_user
  where
    substr(a.created_at,1,10) between '2025-12-01' and '2026-01-31'
    and user_sk > 0
    and worker_id <> 0
    and a.workplace_id in (4,400,702)
)

, t2 as (
select
  substr(pay_time,1,19) pay_time
  ,substr(pay_time,1,7) pay_ym
  ,worker_id
  ,order_id
  ,business_good_kind_name_level_1
  ,business_good_kind_name_level_2
  ,user_id
  ,amount
  ,worker_name
from aws.crm_order_info a
where
  substr(pay_time,1,10)  between '2025-12-01' and '2026-01-31'
  and worker_id <> 0
  and in_salary = 1
  and is_test = false
  and status = '支付成功'
)

select
substr(t1.created_at,1,19) created_at
,t1.clue_source_name
,t1.worker_name
,d0.workplace_name
,d1.department_name
,d2.regiment_name
,d4.team_name
,t1.user_id
,phone
,substr(pay_time,1,19) pay_time
,order_id
,amount
,t2.worker_name as worker_name_2
from t1
left join t2 on t1.user_id = t2.user_id  and t1.created_at < t2.pay_time
left join dw.dim_crm_organization d0 on t1.workplace_id = d0.id
left join dw.dim_crm_organization d1 on t1.department_id = d1.id
left join dw.dim_crm_organization d2 on t1.regiment_id = d2.id
left join dw.dim_crm_organization d4 on t1.team_id = d4.id
where t1.worker_name in ({names_placeholder})
""".strip()

# ---------- 118 个 worker_name ----------
WORKER_NAMES = [
    '黄高翔01','刘泉泉01','万盛02','涂志财01','李娜05','陈升升01','黄文君01','郭茜01',
    '唐小芸01','陶灯松01','张琳琳01','赵耀01','徐嘉俊01','李博02','刘婧02','李城沉01',
    '徐乐01','罗蒙02','姜爽01','胡乐01','王绍杭01','任治翔01','刘小霞01','徐棒01',
    '宋发毅01','曹骊戈01','魏爽01','陈永康01','周源浩01','吴迪03','雷帆01','张丽芸',
    '鲁馨怡01','邓红林01','邹一帆01','李文祥02','安航05','刘力媛01','黄思敏01','金晶01',
    '朱志豪','覃金涛01','林云02','熊锦龙02','成正军01','李根01','何奇峻01','李强04',
    '王超','杨佳琦01','郭勇01','熊雯','孙周01','石鑫01','王国升01','徐小雨',
    '王建锋01','陈铸01','徐迎01','何流星','许鹏01','史志伟01','安航06','曾毅01',
    '徐文杰02','管纬地01','陈勇01','姜杨杨01','王孝涛01','张海','翟胜浩02','童海微01',
    '钱邈舜01','康梦帆03','李汝01','彭为01','陈诗语01','郭欣月01','杨志高01','刘磊02',
    '冯雷','姜鹏','张斌斌01','张琛','程灿灿01','朱凯','谢振01','徐慧',
    '吕波02','周易','徐亚洲01','张达01','刘军03','郭静霏01','李杰01','张志博02',
    '刘梦婷','高伟01','马原','卢胜兴01','张涛01','肖力02','赵飞01','刘微02',
    '张学龙01','张伟龙02','刘程01','詹文龙','刘坤01','汪力01','刘鹏02','万昌主',
    '卢光明01','陈兆功01','刘艳01','陈健05','刘雅倩01','田芊01',
]


def load_config():
    """加载跳板机配置"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        cfg = json.load(f)
    cfg.setdefault('db_user', cfg.get('username'))
    cfg.setdefault('db_password', cfg.get('password'))
    return cfg


def split_batches(lst, n):
    """将列表均匀拆分为 n 个子列表"""
    size = math.ceil(len(lst) / n)
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def build_sql(names):
    """根据一批 worker_name 生成完整 SQL"""
    names_str = ','.join(f"'{n}'" for n in names)
    return SQL_TEMPLATE.replace('{names_placeholder}', names_str)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    cfg = load_config()

    batches = split_batches(WORKER_NAMES, BATCH_COUNT)
    print(f'共 {len(WORKER_NAMES)} 个 worker_name，拆分为 {len(batches)} 批')
    for i, b in enumerate(batches):
        print(f'  批次 {i+1}: {len(b)} 人')

    all_dfs = []
    total_rows = 0
    overall_start = time.time()

    print('\n正在建立 SSH 隧道...')
    with SSHTunnelForwarder(
        (cfg['ssh_host'], 22),
        ssh_username=cfg['ssh_user'],
        ssh_password=cfg['ssh_password'],
        remote_bind_address=(cfg['db_host'], cfg['db_port']),
        local_bind_address=('127.0.0.1', 0)
    ) as tunnel:
        local_port = tunnel.local_bind_port
        print(f'隧道已建立: 127.0.0.1:{local_port}')

        print('正在连接数据库...')
        conn = impala_connect(
            host='127.0.0.1',
            port=local_port,
            auth_mechanism='PLAIN',
            database='tmp',
            user=cfg['db_user'],
            password=cfg['db_password']
        )

        for idx, batch in enumerate(batches, 1):
            sql = build_sql(batch)
            print(f'\n[批次 {idx}/{len(batches)}] 执行中 ({len(batch)} 人)...')
            t0 = time.time()
            cur = conn.cursor(dictify=True)
            cur.execute(sql)
            rows = cur.fetchall()
            elapsed = time.time() - t0
            print(f'  -> 返回 {len(rows)} 条记录 (耗时 {elapsed:.1f}s)')
            if rows:
                all_dfs.append(pd.DataFrame(rows))
            total_rows += len(rows)
            cur.close()

        conn.close()

    # 合并并导出
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
    else:
        df = pd.DataFrame()
    df.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')

    total_elapsed = time.time() - overall_start
    print(f'\n{"=" * 50}')
    print(f'全部完成！共 {total_rows} 条记录，已导出: {OUTPUT_FILE}')
    print(f'总耗时: {total_elapsed:.1f}s')
    print(f'{"=" * 50}')


if __name__ == '__main__':
    main()
