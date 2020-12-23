import ntpcrainwarning
import pandas as pd


# 某列高於某值
def _df_column_over_value(df_temp, column, value):
    return df_temp[df_temp[column] >= value]


# 某列包含某值
def _df_column_is_exist_value(df_temp, column, values):
    return df_temp[df_temp[column].isin(values)]


# 某列包含某集合
def _df_column_is_exits_values(df_temp, column, *values):
    return df_temp[df_temp[column].isin(*values)]


# 超過10分鐘10min雨量的DF
def _df_10min_df(df_temp, mm):
    return _df_column_over_value(df_temp, "10分鐘", mm)


# 超過1小時雨量的DF
def _df_1hr_df(df_temp, mm):
    return _df_column_over_value(df_temp, "1小時", mm)


# 區級強化三級開設清單
def _dist_force_3_list(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
    key_df = _df_1hr_df(key_area_rain_df, 40)
    non_key_df = _df_1hr_df(non_key_area_rain_df, 60)
    dist_force_3_df = pd.concat([key_df, non_key_df])[['鄉鎮']].drop_duplicates()
    dist_list = dist_force_3_df.values.tolist()
    return dist_list


# 10分鐘雨量是否達mm
def df_10min_bool(df_temp, mm):
    return _df_column_over_value(df_temp, "10分鐘", mm)['10分鐘'].any()


# (重點區)1小時雨量是否達mm
def df_1hr_key_bool(df_temp, mm):
    return _df_column_over_value(df_temp, "1小時", mm)['1小時'].any()


# (非重點區)1小時雨量是否達mm
def df_1hr_non_key_bool(df_temp, mm):
    return _df_column_over_value(df_temp, "1小時", mm)['1小時'].any()


# (重點區)1小時雨量達mm的數量
def df_1hr_key_count(df_temp, mm, count):
    return _df_column_over_value(df_temp, "1小時", mm)['1小時'].count() >= count


# (非重點區)1小時雨量達mm的數量
def df_1hr_non_key_count(df_temp, mm, count):
    return _df_column_over_value(df_temp, "1小時", mm)['1小時'].count() >= count


def message_10min(temp_df, current_time):
    message = ''
    temp_df = _df_10min_df(temp_df, 10)
    # temp_df = _df_10min_df(temp_df,0)
    for dist, site, mm in zip(temp_df['鄉鎮'], temp_df['雨量站'], temp_df['10分鐘']):
        message = message + dist + '(' + site + ')' + str(mm) + 'mm/10分鐘、'
    message = '十分鐘雨量簡訊稿：\n新北市EOC: ' + current_time[6:] + message[:-1] + '。'
    return message


# 是否進駐EOC
def into_eoc(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
    if ntpcrainwarning.ntpc_rainfall_std.df_10min_bool(
            county_rain_df, 10) or ntpcrainwarning.ntpc_rainfall_std.df_1hr_key_bool(
            key_area_rain_df, 30) or ntpcrainwarning.ntpc_rainfall_std.df_1hr_non_key_bool(
            non_key_area_rain_df, 50):
        msg = '\n提醒：進駐EOC\n\n' + current_time + '：\n雨量達進駐EOC標準'
        if t != 'no':
            ntpcrainwarning.linePush.line_push_text(t, msg)
        return True


# 雨量是否超過10mm/10min
def rain_over_10(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
    if ntpcrainwarning.ntpc_rainfall_std.df_10min_bool(county_rain_df, 10):
        msg = '\n10mm簡訊稿\n\n' + ntpcrainwarning.ntpc_rainfall_std.message_10min(county_rain_df, current_time)
        # LINE推播10分鐘雨量簡訊稿
        if t != 'no':
            ntpcrainwarning.linePush.line_push_text(t, msg)
        return True


# 是否EOC強化三級開設
def eoc_flood_force_3(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
    msg = '\n提醒：EOC強三\n\n' + current_time + '：\n達EOC強化三級開設標準'
    if ntpcrainwarning.ntpc_rainfall_std.df_10min_bool(county_rain_df, 10) and (
            ntpcrainwarning.ntpc_rainfall_std.df_1hr_key_count(
                key_area_rain_df, 40, 3) or ntpcrainwarning.ntpc_rainfall_std.df_1hr_key_count(
                key_area_rain_df, 50, 2) or ntpcrainwarning.ntpc_rainfall_std.df_1hr_key_count(
                key_area_rain_df, 60, 1)):
        # LINE推播
        if t != 'no':
            ntpcrainwarning.linePush.line_push_text(t, msg)
        return True
    if ntpcrainwarning.ntpc_rainfall_std.df_10min_bool(county_rain_df, 10) and (
            ntpcrainwarning.ntpc_rainfall_std.df_1hr_non_key_count(
                non_key_area_rain_df, 80, 2) or ntpcrainwarning.ntpc_rainfall_std.df_1hr_non_key_count(
                non_key_area_rain_df, 90, 1)):
        # LINE推播
        if t != 'no':
            ntpcrainwarning.linePush.line_push_text(t, msg)
        return True


# 是否區級強化三級開設
def dist_force_3(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
    if ntpcrainwarning.ntpc_rainfall_std.df_1hr_key_bool(
            key_area_rain_df, 40) or ntpcrainwarning.ntpc_rainfall_std.df_1hr_non_key_bool(
            non_key_area_rain_df, 60):
        dist_list = _dist_force_3_list(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time)
        msg = '\n提醒：'
        for i in dist_list:
            msg = msg + i[0]
        msg = msg + '區公所強三\n\n' + current_time + '：\n部分行政區雨量達強化三級開設標準'
        # LINE推播
        if t != 'no':
            ntpcrainwarning.linePush.line_push_text(t, msg)
        return dist_list


# 是否雨情巡查
def rain_patrol(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
    if ntpcrainwarning.ntpc_rainfall_std.df_1hr_key_bool(
            key_area_rain_df, 40) or ntpcrainwarning.ntpc_rainfall_std.df_1hr_non_key_bool(
            non_key_area_rain_df, 60):
        dist_list = _dist_force_3_list(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time)
        msg = '\n提醒：'
        for i in dist_list:
            msg = msg + i[0]
        msg = msg + '雨情巡查\n\n' + current_time + '：\n部分行政區雨量達雨情巡查標準'
        # LINE推播
        if t != 'no':
            ntpcrainwarning.linePush.line_push_text(t, msg)
        return dist_list


# 是否需要推播雨量表
def rain_table(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
    if into_eoc(t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time) or eoc_flood_force_3(
            t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time) or dist_force_3(
            t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time) or rain_patrol(
            t, county_rain_df, key_area_rain_df, non_key_area_rain_df, current_time):
        # LINE推播雨量圖片
        msg = 'Qpesums雨量觀測資料'
        file = ntpcrainwarning.rain_pic.df_plt(county_rain_df.head(20), current_time)
        ntpcrainwarning.linePush.line_push_pic(t, msg, file)
        return True
