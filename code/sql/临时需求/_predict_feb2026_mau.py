"""
预测2026年02月MAU
基于春节对齐的历史DAU模式 + MAU/DAU比值
春节日期：2024-02-10, 2025-01-29, 2026-02-17
"""
import pandas as pd
import numpy as np

df = pd.read_excel('output/近三年1月2月每日日活及月活量级.xlsx')
df['date'] = pd.to_datetime(df['日期'])

# ====== 1. 构建春节对齐的DAU序列 ======
cny_map = {2024: pd.Timestamp('2024-02-10'), 2025: pd.Timestamp('2025-01-29'), 2026: pd.Timestamp('2026-02-17')}
df['cny_offset'] = df.apply(lambda r: (r['date'] - cny_map[r['date'].year]).days, axis=1)

# 2024 全量参考（Jan+Feb，覆盖 CNY 前后完整周期）
ref_2024 = df[df['date'].between('2024-01-01', '2024-02-29')].set_index('cny_offset')['DAU']
# 2025 全量参考（Jan+Feb）
ref_2025 = df[df['date'].between('2025-01-01', '2025-02-28')].set_index('cny_offset')['DAU']

# 2026-02 已知数据
feb26 = df[df['年月'] == '2026-02'].copy()
feb26_idx = feb26.set_index('cny_offset')['DAU']

# ====== 2. 计算缩放因子 ======
# 用 2026 Feb 已知天（offset -16 ~ -7）与参考年同offset对比
overlap = range(-16, -6)  # offset -16 to -7

def calc_scale(ref, known, offsets):
    pairs = [(known.get(o), ref.get(o)) for o in offsets if o in known.index and o in ref.index]
    if not pairs:
        return None
    ratios = [k / r for k, r in pairs if r and r > 0]
    return np.mean(ratios) if ratios else None

scale_2024 = calc_scale(ref_2024, feb26_idx, overlap)
scale_2025 = calc_scale(ref_2025, feb26_idx, overlap)

print("=" * 60)
print("【缩放因子】2026 vs 历史同期（春节对齐后）")
print(f"  vs 2024: {scale_2024:.4f}  （2026 DAU 约为 2024 的 {scale_2024:.1%}）")
print(f"  vs 2025: {scale_2025:.4f}  （2026 DAU 约为 2025 的 {scale_2025:.1%}）")

# ====== 3. 逐日预测 Feb 11-28 ======
print("\n" + "=" * 60)
print("【逐日DAU预测】2026-02-11 ~ 2026-02-28")
print(f"{'日期':>12}  {'CNY偏移':>8}  {'2024参考':>10}  {'2025参考':>10}  {'预测DAU':>10}  {'备注'}")
print("-" * 80)

projections = []
for day in range(11, 29):
    offset = day - 17
    p24 = ref_2024.get(offset) * scale_2024 if offset in ref_2024.index else None
    p25 = ref_2025.get(offset) * scale_2025 if offset in ref_2025.index else None
    
    vals = [v for v in [p24, p25] if v is not None]
    avg_p = round(np.mean(vals)) if vals else None
    
    # 备注
    note = ""
    if offset == -1: note = "除夕"
    elif offset == 0: note = "初一（春节）"
    elif 1 <= offset <= 6: note = f"初{offset+1}"
    elif offset < -1: note = f"节前{abs(offset)}天"
    elif offset == 7: note = "初八（节后上班）"
    elif offset > 7: note = "节后恢复"
    
    projections.append({'day': day, 'date': f'2026-02-{day:02d}', 'offset': offset, 
                        'p24': p24, 'p25': p25, 'projected': avg_p, 'note': note})
    
    p24_s = f"{p24:,.0f}" if p24 else "N/A"
    p25_s = f"{p25:,.0f}" if p25 else "N/A"
    avg_s = f"{avg_p:,.0f}" if avg_p else "N/A"
    print(f"  2026-02-{day:02d}  {offset:>+8d}  {p24_s:>10}  {p25_s:>10}  {avg_s:>10}  {note}")

proj_df = pd.DataFrame(projections)

# ====== 4. 汇总并预测MAU ======
known_sum = feb26['DAU'].sum()
projected_sum = proj_df['projected'].sum()
total_dau_sum = known_sum + projected_sum

known_avg = feb26['DAU'].mean()
projected_avg = proj_df['projected'].mean()
full_avg = total_dau_sum / 28

# 历史 MAU / sum(DAU) 比值
hist_mau = {'2024-01': 4426608, '2024-02': 3705390, '2025-01': 3843367, 
            '2025-02': 3464134, '2026-01': 3195764}

print("\n" + "=" * 60)
print("【历史 MAU / sum(DAU) 比值】")
for ym, mau in hist_mau.items():
    s = df[df['年月'] == ym]['DAU'].sum()
    r = mau / s
    print(f"  {ym}: MAU={mau:>10,}  sum(DAU)={s:>12,}  比值={r:.4f}")

# 用近期2月数据的比值
feb_ratios = [hist_mau['2024-02'] / df[df['年月'] == '2024-02']['DAU'].sum(),
              hist_mau['2025-02'] / df[df['年月'] == '2025-02']['DAU'].sum()]
avg_feb_ratio = np.mean(feb_ratios)

# 也用2026-01的比值作为当年参考
ratio_2026_01 = hist_mau['2026-01'] / df[df['年月'] == '2026-01']['DAU'].sum()

# 综合比值（2月历史 + 2026年当前）
composite_ratio = np.mean([avg_feb_ratio, ratio_2026_01])

# 方法1：DAU模式预测
mau_method1 = total_dau_sum * avg_feb_ratio
mau_method1b = total_dau_sum * composite_ratio

# 方法2：Jan-Feb MAU比值
r_2024 = hist_mau['2024-02'] / hist_mau['2024-01']  # CNY in Feb
mau_method2 = hist_mau['2026-01'] * r_2024

# 方法3：YoY同比（2025-02的基础上考虑年度衰减）
yoy_decay_jan = hist_mau['2026-01'] / hist_mau['2025-01']  # 2026 vs 2025 Jan的衰减
mau_method3 = hist_mau['2025-02'] * yoy_decay_jan

print("\n" + "=" * 60)
print("【预测汇总】")
print(f"\n  已知数据（Feb 1-10, 10天）:")
print(f"    DAU总和: {known_sum:>12,}")
print(f"    日均DAU: {known_avg:>12,.0f}")
print(f"    累计MAU: {feb26['当月MAU'].iloc[0]:>12,}")

print(f"\n  预测数据（Feb 11-28, 18天）:")
print(f"    DAU总和: {projected_sum:>12,.0f}")
print(f"    日均DAU: {projected_avg:>12,.0f}")

print(f"\n  全月汇总（28天）:")
print(f"    DAU总和: {total_dau_sum:>12,.0f}")
print(f"    日均DAU: {full_avg:>12,.0f}")

print(f"\n  --- MAU 预测 ---")
print(f"  方法1（DAU模式+Feb比值 {avg_feb_ratio:.4f}）: {mau_method1:>12,.0f}")
print(f"  方法1b（DAU模式+综合比值 {composite_ratio:.4f}）: {mau_method1b:>12,.0f}")
print(f"  方法2（Jan-Feb比值 {r_2024:.4f}）:          {mau_method2:>12,.0f}")
print(f"  方法3（YoY同比 {yoy_decay_jan:.4f}）:            {mau_method3:>12,.0f}")

estimates = [mau_method1, mau_method1b, mau_method2, mau_method3]
avg_est = np.mean(estimates)
low_est = min(estimates)
high_est = max(estimates)

print(f"\n  ★ 综合预测: {avg_est:,.0f}（约 {avg_est/10000:.0f} 万）")
print(f"    预测区间: {low_est:,.0f} ~ {high_est:,.0f}（约 {low_est/10000:.0f}万 ~ {high_est/10000:.0f}万）")

# ====== 5. 春节影响分析 ======
print("\n" + "=" * 60)
print("【春节影响分析】")
# 2024: CNY Feb 10, pre-CNY in Jan end + Feb start
# 2025: CNY Jan 29, pre-CNY in Jan, Feb fully post-CNY
# 2026: CNY Feb 17, pre-CNY in Feb start, CNY + recovery in Feb mid-end

# CNY低谷预测
cny_low = proj_df[proj_df['offset'].between(-1, 1)]['projected'].min()
pre_cny_avg = feb26['DAU'].mean()  # Feb 1-10 is pre-CNY
print(f"  节前日均DAU（Feb 1-10）: {pre_cny_avg:,.0f}")
print(f"  预测春节最低DAU: {cny_low:,.0f}（降幅约 {(1 - cny_low/pre_cny_avg):.0%}）")

# 恢复期
recovery_days = proj_df[proj_df['offset'] > 0]
if len(recovery_days) > 0:
    recovery_end = recovery_days.iloc[-1]['projected']
    print(f"  预测月底DAU（Feb 28）: {recovery_end:,.0f}（恢复至节前 {recovery_end/pre_cny_avg:.0%}）")

print(f"\n  2026年春节（02-17）偏晚，导致:")
print(f"    - 春节低谷+恢复期大部分落在2月内")
print(f"    - 拉低2月整体DAU均值和MAU")
print(f"    - 但节前有16天正常活跃，部分对冲低谷影响")
