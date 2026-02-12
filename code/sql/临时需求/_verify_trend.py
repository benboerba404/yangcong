# -*- coding: utf-8 -*-
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

df = pd.read_excel('output/近半年小学新增用户每日活跃家长趋势.xlsx')
df['parent_pct'] = (df['parent_cnt'] / df['new_active_cnt'] * 100).round(2)

print(f'共 {len(df)} 天')
print(f'日期范围: {df.day.min()} ~ {df.day.max()}')
print(f'\n新增活跃用户 - 日均: {df.new_active_cnt.mean():,.0f}, 总计: {df.new_active_cnt.sum():,}')
print(f'家长用户 - 日均: {df.parent_cnt.mean():,.0f}, 总计: {df.parent_cnt.sum():,}')
print(f'家长占比 - 均值: {df.parent_pct.mean():.2f}%')
print('\n前5天:')
print(df.head().to_string(index=False))
print('\n最后5天:')
print(df.tail().to_string(index=False))
