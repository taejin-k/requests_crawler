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
    title = row.select('a')[0].text.strip()

    chul_cheo = '전기신문'

    url = 'http://www.electimes.com' + row.select('a')[0].get('href')

    date_time = row.select('span')[0].text + ':00'
    date_time = date_time.replace('.', '-')

    return title, chul_cheo, url, date_time


def filter_02(content_html):
    content = content_html.select("div[itemprop=articleBody]")[0].text.strip()
    content = content[0:10000]

    return content


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "http://www.electimes.com/".format(page)
            response = requests.get(list_url, headers=headers)
            response.raise_for_status()
            response.encoding = None

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')

                rows = html.select("#live_news_all > dl > dd > ul > li")

                for row in rows:
                    title, chul_cheo, content_url, date_time = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)
                    response_content.raise_for_status()
                    response_content.encoding = None

                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')

                        content = filter_02(content_html)

                        logger.info("{:<2}/{:>2} [NEW] {} {}".format(page, MAX_PAGE, date_time, title))

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
