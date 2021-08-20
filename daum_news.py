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
    title = row.select('div.cont_thumb > strong > a')[0].text

    if title is None:
        return None, None, None

    title = title.replace("'", "")
    EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    title = EMOJI.sub(r'', title)

    content_url = row.select('div.cont_thumb > strong > a')[0].attrs['href']

    chul_cheo = row.select('div.cont_thumb > strong > span')[0].contents[0]
    chul_cheo = chul_cheo.replace('[', '').replace(']', '')

    return title, content_url, chul_cheo


def filter_02(content_html):
    if content_html.select("#harmonyContainer > section") == []:
        return None, None, None, None

    content = content_html.select("#harmonyContainer > section")[0]
    figure_tag_list = content.find_all('figure')
    for figure_tag in figure_tag_list:
        figure_tag.extract()
    figcaption_tag_list = content.find_all('figcaption')
    for figcaption_tag in figcaption_tag_list:
        figcaption_tag.extract()
    content = content.text.replace("'", "").strip()[0:10000]
    EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    content = EMOJI.sub(r'', content)

    jag_seong_ja = ''
    date_time = ''
    tag_list = content_html.select('#cSub > div > span > span.txt_info')
    rex_datetime = re.compile('([0-9]{4}). ([0-9]{2}). ([0-9]{2})[.]* ([0-9]{2}:[0-9]{2})')
    if len(tag_list) == 1:
        jag_seong_ja = ''
        date_time = rex_datetime.search(tag_list[0].text)
    elif len(tag_list) == 2:
        jag_seong_ja = tag_list[0].text[0:50]
        date_time = rex_datetime.search(tag_list[1].text)
    elif len(tag_list) == 3:
        jag_seong_ja = tag_list[0].text[0:50]
        date_time = rex_datetime.search(tag_list[2].text)
    date_time = "{}-{}-{} {}".format(date_time.group(1), date_time.group(2), date_time.group(3), date_time.group(4))
    date_time = date_time + ':00'

    daes_geul_su = content_html.select("#alexCounter > span")
    if len(daes_geul_su) > 0:
        daes_geul_su = int(content_html.select("#alexCounter > span")[0].text)
    else:
        daes_geul_su = 0

    return date_time, content, jag_seong_ja,  daes_geul_su


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "https://news.daum.net/breakingnews?page={}".format(page)
            response = requests.get(list_url, headers=headers)

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')
                rows = html.select("#mArticle > div.box_etc > ul > li")

                for row in rows:
                    title, content_url, chul_cheo = filter_01(row)

                    if title is None:
                        continue

                    response_content = requests.get(content_url, headers=headers)

                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')

                        date_time, content, jag_seong_ja, daes_geul_su = filter_02(content_html)

                        if content is None or content == '':
                            continue

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
