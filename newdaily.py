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
    content_url = row.select('a')[0].attrs['href']

    return content_url


def filter_02(content_html):
    reg_datetime = content_html.select('#ndArtBody > div.nd-news-body-info.clearfix > span')[0].text.split(' | ')[0].replace('입력 ', '') + ':00'

    title = content_html.select('body > div.nd-container.layout-center.border-none.clearfix.container-first.nd-news > section.nd-news-header > h1')[0].text

    jag_seong_ja = content_html.select('#ndArtBody > div.nd-news-body-info.clearfix > div.nd-news-writer.nd-tooltip')[0].text

    content = content_html.select('#ndArtBody')[0]
    script_tag_list = content.find_all('script')
    for script_tag in script_tag_list:
        script_tag.extract()
    noscript_tag_list = content.find_all('noscript')
    for noscript_tag in noscript_tag_list:
        noscript_tag.extract()
    h4_tag_list = content.find_all('h4')
    for h4_tag in h4_tag_list:
        h4_tag.extract()
    iframe_tag_list = content.find_all('iframe')
    for iframe_tag in iframe_tag_list:
        iframe_tag.extract()
    span_tag_list = content.find_all('span')
    for span_tag in span_tag_list:
        span_tag.extract()
    div_01_tag_list = content.find_all('div', class_='nd-news-body-info')
    for div_01_tag in div_01_tag_list:
        div_01_tag.extract()
    div_02_tag_list = content.find_all('div', class_='ex')
    for div_02_tag in div_02_tag_list:
        div_02_tag.extract()
    div_03_tag_list = content.find_all('div', class_='nd-by-line')
    for div_03_tag in div_03_tag_list:
        div_03_tag.extract()
    div_04_tag_list = content.find_all('div', class_='imgframe')
    for div_04_tag in div_04_tag_list:
        div_04_tag.extract()
    div_05_tag_list = content.find_all('div', class_='nd-rel')
    for div_05_tag in div_05_tag_list:
        div_05_tag.extract()
    content = content.text.strip()

    chul_cheo = '뉴데일리'

    return content, title, reg_datetime, jag_seong_ja, chul_cheo


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "http://www.newdaily.co.kr/news/section_list_all.html?catid=all&pn={}".format(page)
            response = requests.get(list_url, headers=headers)

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')
                rows = html.select("body > div > section.nd-center > article")

                for row in rows:

                    content_url = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)

                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')

                        if content_html.select('#ndArtBody > div.nd-news-body-info.clearfix > span') == []:
                            continue

                        content, title, reg_datetime, jag_seong_ja, chul_cheo = filter_02(content_html)

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

