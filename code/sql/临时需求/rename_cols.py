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
    'good_name': '转化商品名称(0302-0308)',
    'amount': '转化金额(0302-0308)',
    'pay_time': '转化时间(0302-0308)',
}

df.rename(columns=col_map, inplace=True)
df['手机号'] = df['手机号'].apply(lambda x: str(int(x)) if pd.notna(x) and x != '' else '')
df.to_excel('output/通话ID关联组合品成交明细.xlsx', index=False)
print("Done. Shape:", df.shape)
print("Columns:", list(df.columns))
print("\nPhone sample (first 5):")
print(df['手机号'].head())
conv_col = '转化商品名称(0302-0308)'
cnt = df[conv_col].notna().sum()
print(f"\nConversion: {cnt} / {len(df)} rows have conversion")
print("\nSample rows with conversion:")
print(df[df[conv_col].notna()][['通话时间', '转化商品名称(0302-0308)', '转化金额(0302-0308)', '转化时间(0302-0308)']].head(3))
