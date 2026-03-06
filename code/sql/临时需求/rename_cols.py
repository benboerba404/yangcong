# -*- coding: utf-8 -*-
import pandas as pd

df = pd.read_excel('output/通话ID关联组合品成交明细.xlsx')

col_map = {
    'action_id': '通话ID',
    'user_id': '用户ID',
    'call_start_time_str': '通话时间',
    'call_time_length': '通话时长',
    'grade': '年级',
    'clue_stage': '学段',
    'phone': '手机号',
    'agent_name': '坐席名称',
    'regiment_name': '团',
    'team_name': '组',
    'good_name_24h': '24小时内转化商品名称',
    'amount_24h': '24小时内转化金额',
    'good_name_48h': '48小时内转化商品名称',
    'amount_48h': '48小时内转化金额',
    'good_name_3d': '3天内转化商品名称',
    'amount_3d': '3天内转化金额',
    'good_name_7d': '7天内转化商品名称',
    'amount_7d': '7天内转化金额',
    'good_name_14d': '14天内转化商品名称',
    'amount_14d': '14天内转化金额',
}

df.rename(columns=col_map, inplace=True)
df['手机号'] = df['手机号'].apply(lambda x: str(int(x)) if pd.notna(x) and x != '' else '')
df.to_excel('output/通话ID关联组合品成交明细.xlsx', index=False)
print("Done. Shape:", df.shape)
print("Columns:", list(df.columns))
print("\nPhone sample (first 5):")
print(df['手机号'].head())
print("\nConversion stats:")
for w in ['24小时', '48小时', '3天', '7天', '14天']:
    col = f'{w}内转化商品名称'
    if col in df.columns:
        cnt = df[col].notna().sum()
        print(f"  {w}: {cnt} rows with conversion")
