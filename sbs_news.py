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
    title = row.select('a.news > p > strong')[0].text

    content_url = 'https://news.sbs.co.kr/' + row.select('a.news')[0].attrs['href']

    reg_datetime = row.select('a.news > p > span.date')[0].text.replace('.', '-') + ':00'

    if row.select('a.news > p > em') == []:
        jag_seong_ja = ''
    else:
        jag_seong_ja = row.select('a.news > p > em')[0].text

    return title, content_url, reg_datetime, jag_seong_ja


def filter_02(content_html):
    content = content_html.select("#container > div.w_inner > div.w_article > div.w_article_cont > div > div.article_cont_area > div.main_text > div")[0]
    div_tag_list = content.find_all('div')
    for div_tag in div_tag_list:
        div_tag.extract()
    content = content.text.strip()

    chul_cheo = 'SBS'

    return content, chul_cheo


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "https://news.sbs.co.kr/news/newsflash.do?plink=SNB&cooper=SBSNEWS&pageIdx={}".format(page)
            response = requests.get(list_url, headers=headers)

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')
                rows = html.select("#container > div > div.w_news_list.type_issue > ul > li")

                for row in rows:

                    title, content_url, reg_datetime, jag_seong_ja = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)
                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')

                        if content_html.select("#container > div.w_inner > div.w_article > div.w_article_cont > div > div.article_cont_area > div.main_text > div") == []:
                             continue

                        content, chul_cheo = filter_02(content_html)

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

