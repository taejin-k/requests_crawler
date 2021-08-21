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
    if row.select('ul > li.tit > a')[0].attrs['href'].split('.php')[0] == 'pay_read':
        content_url = 'http://www.joseilbo.com/news/' + row.select('ul > li.tit > a')[0].attrs['href']
    else:
        content_url = 'http://www.joseilbo.com' + row.select('ul > li.tit > a')[0].attrs['href'].split('../..')[1]

    reg_datetime = row.select('ul > li.tit > span')[0].text.replace('.', '-') + ':00'
    reg_datetime = reg_datetime.replace('  ', ' ')

    return content_url, reg_datetime


def filter_02(content_html):
    jag_seong_ja = content_html.select('div.view_title_box > ul > li > a')[0].text

    content = content_html.select('#jose_news_view')[0]
    content = content.text.strip()

    title = content_html.select('div.view_title_box > h1')[0].text

    chul_cheo = '조세일보'

    return content, title, jag_seong_ja, chul_cheo


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "http://www.joseilbo.com/news/news_base.php?menu2=1".format(page)
            response = requests.get(list_url, headers=headers)

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')
                rows = html.select("#tab_menu_01_style4 > div")

                for row in rows:

                    content_url, reg_datetime = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)

                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')
                        content, title, jag_seong_ja, chul_cheo = filter_02(content_html)

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

