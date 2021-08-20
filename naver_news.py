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
    title = row.find('a').text.replace("'", "")
    EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    title = EMOJI.sub(r'', title)

    chul_cheo = row.find('span').text

    url = row.find('a').get('href')

    return title, chul_cheo, url


def filter_02(content_html):

    if content_html.select("#articleBodyContents") == []:
        return None, None

    content = content_html.select("#articleBodyContents")[0]
    a_tag_list = content.find_all('a')
    for a_tag in a_tag_list:
        a_tag.extract()
    script_tag_list = content.find_all('script')
    for script_tag in script_tag_list:
        script_tag.extract()
    p_tag_list = content.find_all('p')
    for p_tag in p_tag_list:
        p_tag.extract()
    content = content.text.replace("'", "").strip()[0:10000]
    EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    content = EMOJI.sub(r'', content)

    date_time = content_html.select('#main_content > div.article_header > div.article_info > div > span.t11')[0].text
    ymd = date_time.split('. ')[0].replace('.', '-')
    h_01 = date_time.split('. ')[1][0:2]
    h_02 = date_time.split('. ')[1].split(' ')[1].split(':')[0]
    if h_01 == '오전':
        if h_02 == '12':
            h_02 = int(h_02) - 12
        else:
            h_02 = h_02
    elif h_01 == '오후':
        if h_02 == '12':
            h_02 = h_02
        else:
            h_02 = int(h_02) + 12
    if int(h_02) < 10:
        h_02 = '0' + str(h_02)
    m = date_time.split('. ')[1].split(' ')[1].split(':')[1]
    hm = str(h_02) + ':' + m
    created_time = ymd + ' ' + hm
    date_time = created_time
    date_time = date_time + ':00'

    return content, date_time


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "https://news.naver.com/main/list.nhn?mode=LSD&mid=sec&listType=title&sid1=001&page={}".format(page)
            response = requests.get(list_url, headers=headers)
            response.raise_for_status()
            response.encoding = None

            if response.status_code == 200:
                html = BeautifulSoup(response.content.decode('euc-kr','replace'), 'html.parser')
                rows = html.select("#main_content > div.list_body.newsflash_body > ul > li")

                for row in rows:
                    title, chul_cheo, content_url = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)
                    response_content.raise_for_status()
                    response_content.encoding = None

                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.content.decode('euc-kr','replace'), 'html.parser')

                        if content_html is None or content_html == '' and len(content_html) == 0:
                            continue

                        content, date_time = filter_02(content_html)

                        if content is None or content == '' and len(content) == 0:
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
