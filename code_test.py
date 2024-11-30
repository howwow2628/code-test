import requests
import json
import pandas as pd

# 下載 JSON 檔
url = "https://file.notion.so/f/f/d70b900c-92f2-4d32-870b-1fa0d80e953b/2cc1982f-a835-4d84-9002-318758475632/output_clean_date_technical.json?table=block&id=f447ef6f-695d-45bb-9e49-f6a9c2e5ddd0&spaceId=d70b900c-92f2-4d32-870b-1fa0d80e953b&expirationTimestamp=1733040000000&signature=WEJ0j4BIYT6w4AJUgPXLvsMoeZG0QO4e44KWfxRh9wQ&downloadName=output_clean_date_technical.json"

response = requests.get(url)

# 確認請求是否成功
if response.status_code == 200:
    with open('output_clean_date_technical.json', 'wb') as file:
        file.write(response.content)
    print("JSON 檔案下載成功")
else:
    print(f"下載失敗，狀態碼：{response.status_code}")

# JSON 檔案
with open('output_clean_date_technical.json', 'r') as file:
    data = json.load(file)

#%%
"""
period.csv 為以 financialGrowth, ratios, cashFlowStatementGrowth, incomeStatementGrowth, balanceSheetStatementGrowth 整合的csv
"""

list_keys = [key for key, value in data.items() if isinstance(value, list)]
print("data中list的名稱：", list_keys)

financial_Growth = data['financialGrowth']
ratios = data['ratios']
cash_Flow_Statement_Growth = data['cashFlowStatementGrowth']
income_Statement_Growth = data['incomeStatementGrowth']
balance_sheet_growth = data['balanceSheetStatementGrowth']

# 換為 DataFrame
df1 = pd.DataFrame(financial_Growth)
df2 = pd.DataFrame(ratios)
df3 = pd.DataFrame(cash_Flow_Statement_Growth)
df4 = pd.DataFrame(income_Statement_Growth)
df5 = pd.DataFrame(balance_sheet_growth)


# 首先確保每個 DataFrame 中都包含共同的鍵欄位
dfs = [df1, df2, df3, df4, df5]

# 初始化合併結果 DataFrame，從第一個 DataFrame 開始
merged_df = dfs[0]

# 合併剩下的 DataFrame
for df in dfs[1:]:
    period_df = pd.merge(merged_df, df, on=["symbol", "date", "calendarYear", "period"], how="outer")


# 重新排列欄位順序，將 'date'、'symbol'、'calendarYear' 和 'period' 放在最前面
cols = ['date', 'symbol', 'calendarYear', 'period'] + [col for col in period_df.columns if col not in ['date', 'symbol', 'calendarYear', 'period']]
period_df = period_df[cols]

period_df.to_csv('period.csv', index=False)

#%%
"""
week.csv 為以 tech5 整合的csv
month.csv 為以 tech20 整合的csv
quarter.csv 為以 tech60 整合的csv
year.csv 為以 tech252 整合的csv
"""

historicalPriceFull = data['historicalPriceFull']
historical_data = historicalPriceFull['historical']
historical_df = pd.DataFrame(historical_data)

# 添加 'symbol' 欄位到 historical_df 中
historical_df['symbol'] = historicalPriceFull['symbol']

# 將 historical_df 的日期轉換為統一格式
historical_df['date'] = pd.to_datetime(historical_df['date']).dt.date

tech5 = data['tech5']
tech20 = data['tech20']
tech60 = data['tech60']
tech252 = data['tech252']

tech5_df = pd.DataFrame(tech5)
tech20_df = pd.DataFrame(tech20)
tech60_df = pd.DataFrame(tech60)
tech252_df = pd.DataFrame(tech252)

# 移除日期中的時間部分，僅保留年月日
tech5_df['date'] = pd.to_datetime(tech5_df['date']).dt.date
tech20_df['date'] = pd.to_datetime(tech20_df['date']).dt.date
tech60_df['date'] = pd.to_datetime(tech60_df['date']).dt.date
tech252_df['date'] = pd.to_datetime(tech252_df['date']).dt.date

# 根據 'date' 欄位合併兩個 DataFrame
week_df = pd.merge(historical_df, tech5_df, on=[ "date", "open", "high","low","close","volume"], how="outer")
month_df = pd.merge(historical_df, tech20_df, on=[ "date", "open", "high","low","close","volume"], how="outer")
quarter_df = pd.merge(historical_df, tech60_df, on=[ "date", "open", "high","low","close","volume"], how="outer")
year_df = pd.merge(historical_df, tech252_df, on=[ "date", "open", "high","low","close","volume"], how="outer")


# 添加 'calendarYear' 欄位
week_df['calendarYear'] = pd.to_datetime(week_df['date']).dt.year
month_df['calendarYear'] = pd.to_datetime(month_df['date']).dt.year
quarter_df['calendarYear'] = pd.to_datetime(quarter_df['date']).dt.year
year_df['calendarYear'] = pd.to_datetime(year_df['date']).dt.year

# 添加 'period' 欄位，根據月份進行季度劃分
def get_quarter(month):
    if 1 <= month <= 3:
        return "Q1"
    elif 4 <= month <= 6:
        return "Q2"
    elif 7 <= month <= 9:
        return "Q3"
    else:
        return "Q4"

week_df['period'] = pd.to_datetime(week_df['date']).dt.month.apply(get_quarter)
month_df['period'] = pd.to_datetime(month_df['date']).dt.month.apply(get_quarter)
quarter_df['period'] = pd.to_datetime(quarter_df['date']).dt.month.apply(get_quarter)
year_df['period'] = pd.to_datetime(year_df['date']).dt.month.apply(get_quarter)

# 重新排列欄位順序，將 'date', 'symbol', 'calendarYear', 'period' 放在最前面 
cols = ['date', 'symbol', 'calendarYear', 'period'] + [col for col in week_df.columns if col not in ['date', 'symbol', 'calendarYear', 'period']]
week_df = week_df[cols]

cols = ['date', 'symbol', 'calendarYear', 'period'] + [col for col in month_df.columns if col not in ['date', 'symbol', 'calendarYear', 'period']]
month_df = month_df[cols]

cols = ['date', 'symbol', 'calendarYear', 'period'] + [col for col in quarter_df.columns if col not in ['date', 'symbol', 'calendarYear', 'period']]
quarter_df = quarter_df[cols]

cols = ['date', 'symbol', 'calendarYear', 'period'] + [col for col in year_df.columns if col not in ['date', 'symbol', 'calendarYear', 'period']]
year_df = year_df[cols]


week_df.to_csv('week.csv', index=False)
month_df.to_csv('month.csv', index=False)
quarter_df.to_csv('quarter.csv', index=False)
year_df.to_csv('year.csv', index=False)



