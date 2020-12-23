import pandas as pd
from typing import Dict, List, Any

key_area = (
    '板橋區', '三重區', '永和區', '新莊區', '土城區', '蘆洲區', '樹林區', '三峽區', '中和區', '汐止區', '新店區', '五股區', '泰山區', '鶯歌區', '林口區', '淡水區',
    '八里區')
non_key_area = ('三芝區', '石門區', '萬里區', '金山區', '瑞芳區', '貢寮區', '深坑區', '烏來區', '雙溪區', '平溪區', '石碇區', '坪林區')
high_area = ('五股區', '鶯歌區', '林口區', '三芝區', '深坑區', '烏來區', '平溪區', '石碇區', '坪林區')
low_area = (
    '板橋區', '三重區', '永和區', '新莊區', '土城區', '蘆洲區', '樹林區', '三峽區', '中和區', '汐止區', '新店區', '泰山區', '淡水區', '八里區', '石門區', '萬里區',
    '金山區', '瑞芳區', '貢寮區', '雙溪區')
waters_safe_area = ('烏來區', '坪林區', '三峽區', '新店區')


def _filter_is_exist_column(df_temp, column, *values):
    return df_temp[df_temp[column].isin(*values)]


def _filter_under_column(df_temp, column, values):
    return df_temp.loc[(df_temp[column] < values)]


# 某列包含某值
def _df_column_is_exist_value(df_temp, column, values):
    return df_temp[df_temp[column].isin(values)]


def raw_data_to_df(raw_data):
    rain_df = pd.read_html(raw_data, encoding='utf-8')[0]
    return rain_df


def open_api_data_df(df):
    obs_time = list()
    CITY = list()
    CITY_SN = list()
    TOWN = list()
    TOWN_SN = list()
    ATTRIBUTE = list()
    ELEV = list()
    RAIN = list()
    MIN_10 = list()
    HOUR_3 = list()
    HOUR_6 = list()
    HOUR_12 = list()
    HOUR_24 = list()

    # 時間欄位
    for idx, row in df.iterrows():
        obs_time.append(row['time']['obsTime'])
    df.time = obs_time
    # 提取城市相關欄位
    for idx, row in df.iterrows():
        temp = row['parameter']
        for i in temp:
            if i['parameterName'] == 'CITY': CITY.append(i['parameterValue'])
            if i['parameterName'] == 'CITY_SN': CITY_SN.append(i['parameterValue'])
            if i['parameterName'] == 'TOWN': TOWN.append(i['parameterValue'])
            if i['parameterName'] == 'TOWN_SN': TOWN_SN.append(i['parameterValue'])
            if i['parameterName'] == 'ATTRIBUTE': ATTRIBUTE.append(i['parameterValue'])
    # 提取氣象相關欄位
    for idx, row in df.iterrows():
        temp = row['weatherElement']
        for i in temp:
            if i['elementName'] == 'ELEV': ELEV.append(i['elementValue'])
            if i['elementName'] == 'RAIN': RAIN.append(i['elementValue'])
            if i['elementName'] == 'MIN_10': MIN_10.append(i['elementValue'])
            if i['elementName'] == 'HOUR_3': HOUR_3.append(i['elementValue'])
            if i['elementName'] == 'HOUR_6': HOUR_6.append(i['elementValue'])
            if i['elementName'] == 'HOUR_12': HOUR_12.append(i['elementValue'])
            if i['elementName'] == 'HOUR_24': HOUR_24.append(i['elementValue'])

    # df_init = pd.DataFrame(range(0, len(CITY)))
    # temp_dict = dict(CITY)
    # df_new = pd.DataFrame.from_dict(temp_dict)
    # df_new = df_init.append(temp_dict)

    # 按欄位製作DF
    temp_list = [CITY,TOWN,df.locationName,ELEV,MIN_10,RAIN,HOUR_3,HOUR_6,HOUR_12,HOUR_24]
    # column改為0 ~ 9
    column_names = range(0, len(CITY))
    # T轉置DF
    df_new = pd.DataFrame(temp_list, columns=column_names).T
    # print(df_new)
    return df_new


def county_rain_df(df, county='all'):
    # 更改欄位名稱
    df.rename(
        columns={0: "縣市", 1: "鄉鎮", 2: "雨量站", 3: "測站高度", 4: "10分鐘", 5: "1小時", 6: "3小時", 7: "6小時", 8: "12小時", 9: "24小時"},
        inplace=True)
    # 將空白列刪除
    df = df.drop(0).reset_index(drop=True)
    # 篩選出需要的縣市
    if county != 'all':
        df = df[df['縣市'].str.contains(county, na=False)]
        df['縣市'] = county
    # 將測站高度欄位裡，非數字的部分去除(Qpsume)
    # if df['測站高度'][-1] == 'm' and type(df['測站高度']) == 'string':
    #     df['測站高度'] = df['測站高度'].map(lambda x: str(x)[:-1])
    # 將-修改為0，x修改為-1
    df.replace('-', '0', inplace=True)
    df.replace('×', '-1', inplace=True)
    # 將數字欄位更改為浮點數
    df[['測站高度', '10分鐘', '1小時', '3小時', '6小時', '12小時', '24小時']] = df[
        ['測站高度', '10分鐘', '1小時', '3小時', '6小時', '12小時', '24小時']].apply(pd.to_numeric, errors='coerce')
    # 將Nan更改為-1
    df.fillna(-1, inplace=True)
    # 排序後重設INDEX
    df.sort_values(by=["10分鐘", "1小時", "3小時", "6小時"], ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.index += 1
    return df


# 產生重點區資料(type:df)
def key_area_df(df):
    # 刪除高度不計的測站
    df_high_dist = _filter_under_column(_filter_is_exist_column(df, '鄉鎮', high_area), '測站高度', 300)
    df_low_dist = _filter_under_column(_filter_is_exist_column(df, '鄉鎮', low_area), '測站高度', 60)
    df = pd.merge(df_high_dist, df_low_dist, how='outer')
    # 過濾出重點區測站
    df = df[df['鄉鎮'].isin(key_area)]
    return df


# 產生非重點區資料(type:df)
def non_key_area_df(df):
    # 刪除高度不計的測站
    df_high_dist = _filter_under_column(_filter_is_exist_column(df, '鄉鎮', high_area), '測站高度', 300)
    df_low_dist = _filter_under_column(_filter_is_exist_column(df, '鄉鎮', low_area), '測站高度', 60)
    df = pd.merge(df_high_dist, df_low_dist, how='outer')
    # 過濾出非重點區測站
    df = df[df['鄉鎮'].isin(non_key_area)]
    return df
