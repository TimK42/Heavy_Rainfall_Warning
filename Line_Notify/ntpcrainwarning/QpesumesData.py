# -*- coding: utf-8 -*-

import requests
from lxml import etree


def qp_plus_data():
    url = 'https://qpeplus.cwb.gov.tw/pub/rainmonitor/get_tag_sectiondisplay=%E9%9B%A8%E9%87%8F%E8%A7%80%E6%B8%AC' \
          '&data_time=2020-08-28+12%3A00%3A00 '

    cookies = {
        'csrftoken': 'zptEwhN8OUG21rbIrAQgBikgnBfYDvdnG35UCgNmDWAq51YOyUmzGtP8Pc9eKSKs',
        '_ga': 'GA1.3.647096127.1597025305',
        'sessionid': 'o27pemr03dlfxzbcnfvdw4jgr1y8bkye', }

    headers = {
        'Connection': 'keep-alive',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/84.0.4147.135 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://qpeplus.cwb.gov.tw',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://qpeplus.cwb.gov.tw/pub/rainmonitor/',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    data = {
        'tag_name': '雨量觀測',
        'data_time': '2020-08-28 12:50:00'
    }

    response = requests.post(url, headers=headers,
                             cookies=cookies, data=data, timeout=3, verify=False)

    return response.text


def raw_data():
    url = 'http://117.56.4.156/taiwan-html/ChartDirector/gaugemax.php?column=8&sort=1&filter=0&hour_s=hour_1&sh_in' \
          '=&county_in=0 '

    my_headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko'}
    my_cookies = {'__utma': '173357155.195835215.1593327131.1593327131.1593327131.1',
                  '__utmb': '173357155.152.10.1593327131',
                  '__utmc': '173357155',
                  '__utmt': '1',
                  '__utmz': '173357155.1593327131.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
                  'onalert': 'taiwan!mosaic!none!1!RCKT!taiwan!mosaic!125!100!nidsB!0!1!0!0!1!0!0!0!0!0!0!0!0!0!0!0!0'
                             '!0!0!0!0!0!1!0!0!0!0!0!0!0!0!0!0!0!0!0!0!0!1!0!0!!!!!!!!!'}
    r = requests.get(url, headers=my_headers, cookies=my_cookies, timeout=3)
    return r.text


def table_time(raw):
    html = etree.HTML(raw)
    qpesums_current_time = html.xpath('//html/body/div[11]/form/select/option[1]/text()')[0]
    return qpesums_current_time