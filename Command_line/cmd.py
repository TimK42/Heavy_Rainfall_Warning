import requests
from lxml import etree
import pandas as pd
import json
import requests
import time
from datetime import datetime
import os
from lxml import etree
from tabulate import tabulate
import ntpcrainwarning
import gc

# 查詢上一次的雨量json

county = '新北市'
# 要LINE PUSH，輸入TOKEN；不要LINE PUSH，輸入no
token = "no"


def check_html_from_json(df):
    if os.path.isfile('json.txt'):
        new_json = df.to_json()
        old_json = ntpcrainwarning.check_json.read_json()
        if new_json == old_json:
            return True
        return False
    else:
        return False


# 儲存這一次的雨量json
def update_html_to_json(df):
    new_json = df.to_json()
    ntpcrainwarning.check_json.write_json(new_json)
    return


def main():
    # 取出OPEN API的資料(DF)
    temp_data = ntpcrainwarning.QpesumesData.open_api_data()
    rain_data = ntpcrainwarning.ETL.open_api_data_df(temp_data)
    # 讀取雨量觀測時間(type:string)
    current_time = ntpcrainwarning.QpesumesData.open_api_time(temp_data)
    # 讀取雨量觀測資料並整理後，篩選出新北市的資料(type:df)
    county_rain_df = ntpcrainwarning.ETL.county_rain_df(rain_data, county)

    # 雨量資料與上次相同跳脫程式
    if check_html_from_json(county_rain_df):
        print('\r更新時間：' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'), end='')
        return
    else:
        # 更新儲存的網頁資料
        update_html_to_json(county_rain_df)

    # 篩選出重點區資料
    key_area_rain_df = ntpcrainwarning.ETL.key_area_df(county_rain_df)
    # 篩選出非重點區資料
    non_key_area_rain_df = ntpcrainwarning.ETL.non_key_area_df(county_rain_df)

    # 顯示雨量表
    os.system("cls");
    print("本程式由新北市政府消防局技佐郭峻廷提供 版本:0.9.1 (109.12.11)\n因應QPESUMS停止運作的趕工測試版本，如有錯誤麻煩回報。")

    print('\r\r\r\r')
    # print(df.head(10).sort_values('10分鐘', ascending=False))
    print(str(current_time) + '，前10名雨量站(依10分鐘雨量及1小時雨量排序):')
    print(tabulate(county_rain_df.head(10), headers='keys', tablefmt='fancy_grid', numalign="right"))
    print("資料來源：氣象局OPEN API雨量觀測資料")
    print('')

    print('開設及進駐警示(參考)：')

    # 顯示是否進駐EOC
    if ntpcrainwarning.ntpc_rainfall_std.into_eoc(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
        print(" ○：EOC輪值人員進駐")
    else:
        print(" ╳ ：EOC輪值人員進駐")

    # 顯示是否強化三級開設
    if ntpcrainwarning.ntpc_rainfall_std.eoc_flood_force_3(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
        print(" ○：EOC強化三級開設")
    else:
        print(" ╳ ：EOC強化三級開設")

    # 顯示是否區級強化三級開設
    if ntpcrainwarning.ntpc_rainfall_std.eoc_flood_force_3(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
        print(" ○：部分行政區強化三級開設")
    else:
        print(" ╳ ：部分行政區強化三級開設")

    # 顯示是否雨情巡查
    if ntpcrainwarning.ntpc_rainfall_std.rain_patrol(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
        print(" ○：雨情巡查")
    else:
        print(" ╳ ：雨情巡查")

        print('')
        # 顯示雨量達標簡訊
    if ntpcrainwarning.ntpc_rainfall_std.rain_over_10(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
        print('10分鐘10mm雨量簡訊稿(參考):')
        # 整備科表示10分鐘10mm簡訊不包含時雨量達標測站
        msg_rain_over_10 = ntpcrainwarning.ntpc_rainfall_std.message_10min(county_rain_df, current_time)
        print(msg_rain_over_10 + '\b\b。')
    else:
        print('10分鐘10mm雨量未達標無簡訊稿(參考)')

    print("\n自動更新氣象局OPEN API雨量觀測資料")
    print('\r更新時間：' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'), end='')


if __name__ == "__main__":
    while True:
        try:
            main()
        finally:
            collect = gc.collect()

        time.sleep(5)
