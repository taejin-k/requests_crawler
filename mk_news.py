# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import traceback
from datetime import datetime
import re
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger()
logger.setLevel("INFO")
formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)1.1s %(lineno)3s:%(funcName)-16.16s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger = logging.getLogger(__file__)

MAX_PAGE = 10


def filter_01(row):
    rex_no = re.compile('p_page=[0-9]+&')
    url = row.select('td.title > a')[0].attrs['href']
    rex_match = rex_no.search(url)
    start, end = rex_match.span()
    url = url[0:start]+url[end:]
    content_url = 'http:' + url

    chul_cheo = 'MK증권'

    return content_url, chul_cheo


def filter_02(content_html):
    title = content_html.select('#Titless')[0].text.replace("'", "").replace('\\', '')

    content = content_html.select("#Conts")[0]
    table_tag_list = content.find_all('table')
    for table_tag in table_tag_list:
        table_tag.extract()
    p_tag_list = content.find_all('p')
    for p_tag in p_tag_list:
        p_tag.extract()
    span_tag_list = content.find_all('span')
    for span_tag in span_tag_list:
        span_tag.extract()
    div_tag_list = content.find_all('div')
    for div_tag in div_tag_list:
        div_tag.extract()
    style_tag_list = content.find_all('style')
    for style_tag in style_tag_list:
        style_tag.extract()
    content = content.text.replace("'", "").strip()[0:10000].replace('\\', '')

    rex_datetime = re.compile('([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}:[0-9]{2})')
    reg_datetime = rex_datetime.search(content_html.text)[0] + ':00'

    return title, content, reg_datetime


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "http://vip.mk.co.kr/newSt/news/news_list.php?p_page={}&sCode=124&termDatef=&search=&topGubun=".format(page)
            response = requests.get(list_url, headers=headers)
            response.raise_for_status()
            response.encoding = None

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')
                rows = html.select("table.table_6 > tr")

                for row in rows:
                    if row.select('td.title') == []:
                        continue

                    content_url, chul_cheo = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)
                    response_content.raise_for_status()
                    response_content.encoding = None

                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')

                        if content_html.select('#Titless') == []:
                            continue

                        title, content, reg_datetime = filter_02(content_html)

                        if content is None or content == '':
                            continue

                        logger.info("{:<2}/{:>2} [NEW] {} {}".format(page, MAX_PAGE, reg_datetime, title))

                    else:
                        msg = "[본문 크롤링 오류] {} {}".format(response_content.status_code, content_url)
                        logger.warning(msg)
            else:
                msg = "[목록 크롤링 오류] {} {}".format(response.status_code, list_url)
                logger.warning(msg)
                break

    except Exception as e:
        msg = "list_url={}{}content_url={}{}{}".format(list_url, os.linesep, content_url, os.linesep, traceback.format_exc())
        logger.error(msg)

    finally:
        elapsed_time = (datetime.now() - start).total_seconds()
        logger.info("소요시간  {:5.2f}".format(elapsed_time))


if __name__ == '__main__':
    crawling()
