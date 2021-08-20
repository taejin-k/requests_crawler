# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import traceback
from datetime import datetime
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

    content_url = 'http://stock.thinkpool.com/' + row.select('div > a')[0].attrs['href']
    content_url = content_url.split('&pageNo')[0]

    return content_url


def filter_02(content_html):

    title = content_html.select('#content > div.nsflash_cnt > div.tit > strong')[0].text

    content = content_html.select("#content > div.nsflash_cnt > div.article")[0]
    strong_tag_list = content.find_all('strong')
    for strong_tag in strong_tag_list:
        strong_tag.extract()

    if '&lt;' in str(content) or '&gt;' in str(content):
        return None, None, None, None

    content = content.text.strip()[0:10000]

    temp = content_html.select('#content > div.nsflash_cnt > div.tit > span')[0].text.split(' | ')

    chul_cheo = temp[0]

    reg_datetime = temp[1]

    return title, content, chul_cheo, reg_datetime


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "http://stock.thinkpool.com/news/newsFlash/list/newsFlash.do"
            response = requests.get(list_url, headers=headers)

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')
                rows = html.select("#content > div.mgr_news > ul > li")

                for row in rows:

                    content_url = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)

                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')

                        if content_html.select('#content > div.nsflash_cnt > div.tit > strong') == []:
                            continue

                        title, content, chul_cheo, reg_datetime = filter_02(content_html)

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

