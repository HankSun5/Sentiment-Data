import os
import requests
from datetime import datetime
import pandas as pd

api_key = 'db097b6b5b134f5085f1dd09b87a61be7fe1933338c8ce97ff5fe64ee339a447'
export_path = r'SentimentData'  # 设置导出路径

# 确保目录存在的函数
def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Step 1: 获取并保存数据的函数
def Stage1_CryptoETL_Text(api_key=None):
    headers = {}
    if api_key and api_key.strip():  # 检查api_key是否有效
        headers['Apikey'] = api_key  # 使用API密钥设置请求头

    url = 'https://min-api.cryptocompare.com/data/v2/news/?lang=EN'
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 如果响应状态不是200，则抛出异常
        data = response.json()
        if 'Data' not in data or not data['Data']:
            print("API 没有返回有效数据")
            return pd.DataFrame()  # 返回空的 DataFrame 以防止后续报错
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return pd.DataFrame()  # 返回空的 DataFrame 以防止后续报错

    # 将获取的数据转换为DataFrame
    df = pd.DataFrame(data['Data'])

    if df.empty:
        print("API 返回的数据为空")
        return df
    
    # 转换'published_on'为日期格式
    df['date'] = pd.to_datetime(df['published_on'], unit="s")

    # 返回新的数据
    return df

# Step 2: 合并新数据与现有数据，处理重复项，并保存
def update_existing_data(existing_file, new_df):
    ensure_directory_exists(os.path.dirname(existing_file))  # 确保文件保存路径存在
    
    # 检查新数据是否为空
    if new_df.empty:
        print("新获取的数据为空，跳过更新")
        return pd.DataFrame()

    if os.path.exists(existing_file):
        try:
            # 加载现有数据，处理日期列和去除Unnamed列
            existing_df = pd.read_csv(existing_file).drop(columns=['Unnamed: 0'], errors='ignore')
            existing_df['date'] = pd.to_datetime(existing_df['date'])  # 转换日期格式
        except pd.errors.EmptyDataError:
            print(f"Warning: Existing file {existing_file} is empty. Starting fresh.")
            existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame()  # 如果文件不存在，则创建空DataFrame

    # 将新数据与现有数据合并
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    # 确保'id'列数据类型一致
    combined_df['id'] = combined_df['id'].astype(str)

    # 去除重复项（根据'id'去重）
    combined_df.drop_duplicates(subset='id', keep='last', inplace=True)

    # 按日期排序，并重新设置索引
    combined_df = combined_df.sort_values(by='date').reset_index(drop=True)

    # 将更新后的数据重新保存到CSV文件
    combined_df.to_csv(existing_file, index=False)
    print(f"数据已更新并保存到 {existing_file}")
    return combined_df

# 主逻辑
today_date = pd.Timestamp.today().strftime('%d_%m_%Y')  # 获取当天日期
file_name = today_date + '_Sentiment.csv'  # 以当天日期命名文件
existing_file_path = os.path.join(export_path, file_name)  # 完整文件路径

# Step 1: 获取新的新闻数据
new_data = Stage1_CryptoETL_Text(api_key)

# Step 2: 更新现有文件，追加新数据
updated_df = update_existing_data(existing_file_path, new_data)
