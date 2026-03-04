# -*- coding: utf-8 -*-
"""电销领取ARPU分析与预测"""
import pandas as pd
import numpy as np

df = pd.read_excel(r'output/电销领取截止月底转化率_按月_202401起.xlsx')

# 计算指标
df['转化率'] = df['截止月底转化率']
df['客单价'] = df['截止月底转化金额'] / df['截止月底转化人次']
df['ARPU'] = df['截止月底转化金额'] / df['领取人次']

print('=' * 80)
print('历史数据：转化率 / 客单价 / ARPU')
print('=' * 80)
for _, r in df.iterrows():
    tag = ' (未完月)' if r['月份'] == '2026-02' else ''
    print(f"{r['月份']}{tag:<8}  "
          f"转化率={r['转化率']:.4f}  "
          f"客单价={r['客单价']:>10,.0f}  "
          f"ARPU={r['ARPU']:>8,.2f}")

# ---- 预测 ----
# 排除未完月 2026-02
hist = df[df['月份'] != '2026-02'].copy()
hist['month_num'] = range(len(hist))
hist['month_of_year'] = hist['月份'].apply(lambda x: int(x.split('-')[1]))

# 1. 季节因子：每个月份（1-12）的 ARPU 相对全年均值的倍数
seasonal = hist.groupby('month_of_year')['ARPU'].mean()
overall_mean = hist['ARPU'].mean()
seasonal_factor = seasonal / overall_mean

# 2. 去季节化 → 拟合线性趋势
hist['deseasonalized'] = hist.apply(
    lambda r: r['ARPU'] / seasonal_factor[r['month_of_year']], axis=1
)
z = np.polyfit(hist['month_num'], hist['deseasonalized'], 1)
trend = np.poly1d(z)

print(f"\n趋势斜率: {z[0]:.4f}/月 ({'上升' if z[0] > 0 else '下降'})")
print(f"整体均值 ARPU: {overall_mean:,.2f}")

# 3. 预测 2026 全年
print('\n' + '=' * 80)
print('2026年 ARPU 预测（趋势 + 季节性）')
print('=' * 80)

predictions = []
for m in range(1, 13):
    month_num = 24 + (m - 1)  # 2026-01 = index 24
    ym = f'2026-{m:02d}'
    pred_deseason = trend(month_num)
    pred_arpu = pred_deseason * seasonal_factor[m]

    actual_row = df[df['月份'] == ym]
    if len(actual_row) > 0 and ym != '2026-02':
        act = actual_row.iloc[0]['ARPU']
        note = '实际'
        print(f"{ym}  预测={pred_arpu:>8,.2f}  实际={act:>8,.2f}  季节因子={seasonal_factor[m]:.3f}")
    elif ym == '2026-02':
        act = None
        note = '未完月'
        print(f"{ym}  预测={pred_arpu:>8,.2f}  (当月未结束)       季节因子={seasonal_factor[m]:.3f}")
    else:
        act = None
        note = '预测'
        print(f"{ym}  预测={pred_arpu:>8,.2f}                    季节因子={seasonal_factor[m]:.3f}")

    predictions.append({
        '月份': ym,
        '预测ARPU': round(pred_arpu, 2),
        '实际ARPU': round(act, 2) if act is not None else None,
        '季节因子': round(seasonal_factor[m], 3),
        '备注': note,
    })

# 季节因子可视化
print('\n' + '=' * 80)
print('季节因子（各月相对全年均值的倍数）')
print('=' * 80)
for m in range(1, 13):
    bar = '█' * int(seasonal_factor[m] * 30)
    print(f"{m:>2}月: {seasonal_factor[m]:.3f}  {bar}")

# 也输出完整历史表（含客单价、ARPU）
out = df[['月份', '领取人次', '截止月底转化人次', '转化率', '客单价', 'ARPU', '截止月底转化金额']].copy()
out.columns = ['月份', '领取人次', '转化人次', '转化率', '客单价', 'ARPU', '转化金额']

pred_df = pd.DataFrame(predictions)

with pd.ExcelWriter(r'output/电销领取ARPU分析与预测_2026.xlsx', engine='openpyxl') as writer:
    out.to_excel(writer, sheet_name='历史数据', index=False)
    pred_df.to_excel(writer, sheet_name='2026预测', index=False)

print(f"\n结果已导出: output/电销领取ARPU分析与预测_2026.xlsx")
print("  - Sheet1: 历史数据（含转化率、客单价、ARPU）")
print("  - Sheet2: 2026年预测")
