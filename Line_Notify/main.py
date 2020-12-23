import ntpcrainwarning
from datetime import datetime
import gc

# todo closed check部分加入時間確認
# todo closed 區級強三提供行政區
# todo closed 雨情巡查提供行政區

# todo 處理MPL中文字型問題
# todo 解決CRONTAB最短只能1分鐘執行1次的問題
# todo 掛載到docker


# 測試模式開關
# test = False
county = '新北市'
token = "1xolsS8ixHW6ITD1XjD9kjOF21j8Rx1CKQGfArgtsWj"


# 查詢上一次的雨量json
def check_html_from_json(df):
    new_json = df.to_json()
    old_json = ntpcrainwarning.check_json.read_json()
    if new_json == old_json:
        return True
    return False


# 儲存這一次的雨量json
def update_html_to_json(df):
    new_json = df.to_json()
    ntpcrainwarning.check_json.write_json(new_json)
    return


def main():
    try:
        # 開始時紀錄時間兼推播
        print('開始執行：', datetime.now())
        # ntpcrainwarning.linePush.line_push_text(token, '開始執行')
        # 取出OPEN API的資料(DF)
        temp_data = ntpcrainwarning.QpesumesData.open_api_data()
        rain_data = ntpcrainwarning.ETL.open_api_data_df(temp_data)
        # 讀取雨量觀測時間(type:string)
        current_time = ntpcrainwarning.QpesumesData.open_api_time(temp_data)
        # 讀取雨量觀測資料並整理後，篩選出新北市的資料(type:df)
        county_rain_df = ntpcrainwarning.ETL.county_rain_df(rain_data, county)

        # 篩選出重點區資料
        key_area_rain_df = ntpcrainwarning.ETL.key_area_df(county_rain_df)
        # 篩選出非重點區資料
        non_key_area_rain_df = ntpcrainwarning.ETL.non_key_area_df(county_rain_df)

        # 雨量資料與上次相同跳脫程式
        if check_html_from_json(county_rain_df) is True:
            print("雨量資料與上次相同")
            return
        else:
            # 更新儲存的網頁資料
            update_html_to_json(county_rain_df)

        # 進駐EOC推播LINE
        ntpcrainwarning.ntpc_rainfall_std.into_eoc(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time)
        # LINE推播10分鐘雨量簡訊稿
        ntpcrainwarning.ntpc_rainfall_std.rain_over_10(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time)
        # EOC強化三級推播LINE
        ntpcrainwarning.ntpc_rainfall_std.eoc_flood_force_3(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time)
        # 區公所強化三級推播LINE
        ntpcrainwarning.ntpc_rainfall_std.dist_force_3(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time)
        # 雨情巡查推播LINE
        ntpcrainwarning.ntpc_rainfall_std.rain_patrol(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time)
        # 推播雨量表格
        ntpcrainwarning.ntpc_rainfall_std.rain_table(
            token, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time)


        # 結束時紀錄時間兼推播
        print('結束執行：', datetime.now())
        # ntpcrainwarning.linePush.line_push_text(token, '結束執行')

    except Exception as error:
        print(error)
        # ntpcrainwarning.linePush.line_push_text(token, 'Error')


if __name__ == "__main__":
    try:
        main()
    finally:
        collect = gc.collect()
        print(collect)
    
