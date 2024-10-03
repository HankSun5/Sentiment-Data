api_key = 'db097b6b5b134f5085f1dd09b87a61be7fe1933338c8ce97ff5fe64ee339a447'
export_path = r'SentimentData'  # 设置导出路径

# 确保目录存在的函数
def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Step 1: 获取并保存数据的函数
def Stage1_CryptoETL_Text(export_path, api_key=None):
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

    # 确保保存路径存在
    ensure_directory_exists(export_path)
    
    # 生成文件名并保存数据
    today_date = pd.Timestamp.today().strftime('%d_%m_%Y')  # 获取当天日期
    file_name = today_date + '_Sentiment.csv'  # 以当天日期命名文件
    file_path = os.path.join(export_path, file_name)  # 完整文件路径

    df.to_csv(file_path, index=False)
    print(f"数据已获取并保存到 {file_path}")

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

# Step 3: 合并多个CSV文件中的新闻数据，并保存到当前工作目录
def combine_news_data(base_dir):
    files = os.listdir(base_dir)
    df = pd.DataFrame()

    for f in files:
        file_path = os.path.join(base_dir, f)
        if os.path.isfile(file_path):  # 只处理文件，忽略文件夹
            file_size = os.path.getsize(file_path)
            if file_size > 0:  # 确保文件不为空
                try:
                    print(f"Processing file: {file_path}, size: {file_size} bytes")
                    dfsub = pd.read_csv(file_path)
                    if dfsub.empty:
                        print(f"Warning: {file_path} contains no data, skipping.")
                    else:
                        dfsub['date'] = pd.to_datetime(dfsub['published_on'], unit="s")
                        df = pd.concat([df, dfsub], axis=0)
                except pd.errors.EmptyDataError:
                    print(f"Warning: {file_path} is empty or invalid. Skipping.")
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
            else:
                print(f"Warning: {file_path} is empty. Skipping.")
        else:
            print(f"{file_path} is not a valid file. Skipping.")
    
    if df.empty:
        print("No data to combine.")
        return df  # 返回空DataFrame以防后续报错

    # 创建 'datem' 列并按日期排序
    df['datem'] = pd.to_datetime(df['date']).dt.date
    df = df.sort_values(['datem'])

    # 删除 'Unnamed: 0' 列，忽略不存在的列
    df.drop(['Unnamed: 0'], axis=1, inplace=True, errors='ignore')
    
    # 使用 os.getcwd() 获取当前工作目录路径
    current_directory = os.getcwd()  
    
    # 将最终的汇总数据保存到当前工作目录
    stage_file_path = os.path.join(current_directory, 'Stage_1_Time_Series_Sentiment.csv')
    df.to_csv(stage_file_path, index=False)
    print(f"汇总数据已保存到 {stage_file_path}")
    return df

# 主逻辑
today_date = pd.Timestamp.today().strftime('%d_%m_%Y')  # 获取当天日期
file_name = today_date + '_Sentiment.csv'  # 以当天日期命名文件
existing_file_path = os.path.join(export_path, file_name)  # 完整文件路径

# Step 1: 获取新的新闻数据
new_data = Stage1_CryptoETL_Text(export_path, api_key)

# Step 2: 更新现有文件，追加新数据
updated_df = update_existing_data(existing_file_path, new_data)

# Step 3: 合并所有新闻文件，生成时间序列数据并保存到当前工作目录
final_combined_df = combine_news_data(export_path)
