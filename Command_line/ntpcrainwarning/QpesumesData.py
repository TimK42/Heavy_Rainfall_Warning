# -*- coding: utf-8 -*-

import requests
from lxml import etree
import pandas as pd
import json

def open_api_data():
    response = "https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0002-001?Authorization=CWB-9021377C-CC1A-4F19-B6C1-1C1CF5732596"
    html = requests.get(response)
    htmldict = json.loads(html.text)
    htmldict = htmldict["records"]["location"]
    htmljson = json.dumps(htmldict)
    df = pd.read_json(htmljson)
    return df

def qplus_datetime():
    cookies = {
        'csrftoken': '49AqOpS3E8zEDFwDs8iD4rBHzBcQsxjevOe4rIapmydcqWcWdgmZJIc78YGFXZhY',
        'sessionid': 'xozbfvymonk3te9omzh0fw301kwi1uhh',
    }

    headers = {
        'Connection': 'keep-alive',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://qpeplus.cwb.gov.tw',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://qpeplus.cwb.gov.tw/pub/rainmonitor/',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    data = {
        'tag_name': '^%^E9^%^9B^%^A8^%^E9^%^87^%^8F^%^E8^%^A7^%^80^%^E6^%^B8^%^AC'
    }

    response = requests.post('https://qpeplus.cwb.gov.tw/pub/rainmonitor/get_tag_sectiondisplay_data_time/',
                             headers=headers, cookies=cookies, data=data)
    return response

def raw_data():
    # url = 'http://117.56.4.156/taiwan-html/ChartDirector/gaugemax.php?column=8&sort=1&filter=0&hour_s=hour_1&sh_in' \
    #       '=&county_in=0 '
    #
    # my_headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko'}
    # my_cookies = {'__utma': '173357155.195835215.1593327131.1593327131.1593327131.1',
    #               '__utmb': '173357155.152.10.1593327131',
    #               '__utmc': '173357155',
    #               '__utmt': '1',
    #               '__utmz': '173357155.1593327131.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
    #               'onalert': 'taiwan!mosaic!none!1!RCKT!taiwan!mosaic!125!100!nidsB!0!1!0!0!1!0!0!0!0!0!0!0!0!0!0!0!0'
    #                          '!0!0!0!0!0!1!0!0!0!0!0!0!0!0!0!0!0!0!0!0!0!1!0!0!!!!!!!!!'}
    # r = requests.get(url, headers=my_headers, cookies=my_cookies, timeout=3)

    response = "https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0002-001?Authorization=CWB-9021377C-CC1A-4F19-B6C1-1C1CF5732596"
    df = pd.read_json(response)
    return df


def table_time(raw):
    html = etree.HTML(raw)
    qpesums_current_time = html.xpath('//html/body/div[11]/form/select/option[1]/text()')[0]
    return qpesums_current_time

def open_api_time(df):
    obs_time = list()
    # 時間欄位
    for idx, row in df.iterrows():
        obs_time.append(row.time)
        open_obs_time = max(obs_time)
    return open_obs_time