# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import traceback
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re

logger = logging.getLogger()
logger.setLevel("INFO")
formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)1.1s %(lineno)3s:%(funcName)-16.16s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger = logging.getLogger(__file__)

MAX_PAGE = 10


def filter_01(row):
    title = row.select('div > a > span.tb > strong')[0].text.strip().replace('"', '')

    date_time = '2020-' + row.select('div > span > em')[0].text.strip() + ':00'

    url = 'http:' + row.select('div > a')[0].get('href')

    return title, url, date_time


def filter_02(content_html):
    content = ''
    chul_cheo = ''

    if content_html.select("#realArtcContents") != []:
        content = content_html.select("#realArtcContents")[0]
        script_tag_list = content.find_all('script')
        for script_tag in script_tag_list:
            script_tag.extract()
        a_tag_list = content.find_all('a')
        for a_tag in a_tag_list:
            a_tag.extract()
        content = content.text.strip()[0:10000].replace('\n', '')
        EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
        content = EMOJI.sub(r'', content)
    elif content_html.select(".article") != []:
        content = content_html.select(".article")[0]
        script_tag_list = content.find_all('script')
        for script_tag in script_tag_list:
            script_tag.extract()
        a_tag_list = content.find_all('a')
        for a_tag in a_tag_list:
            a_tag.extract()
        content = content.text.strip()[0:10000].replace('\n', '')
        EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
        content = EMOJI.sub(r'', content)
    elif content_html.select("#articleContetns") != []:
        content = content_html.select("#articleContetns")[0]
        script_tag_list = content.find_all('script')
        for script_tag in script_tag_list:
            script_tag.extract()
        a_tag_list = content.find_all('a')
        for a_tag in a_tag_list:
            a_tag.extract()
        content = content.text.strip()[0:10000].replace('\n', '')
        EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
        content = EMOJI.sub(r'', content)

    if content_html.select('#articleView > p > span.link > a.medium') != []:
        chul_cheo = content_html.select('#articleView > p > span.link > a.medium')[0].text.strip()
    elif content_html.select('#cntArea > dl > dt > a > img') != []:
        chul_cheo = content_html.select('#cntArea > dl > dt > a > img')[0].get('alt')
    elif content_html.select('#cntArea > dl > dt > img') != []:
        chul_cheo = content_html.select('#cntArea > dl > dt > img')[0].get('alt')

    return content, chul_cheo


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "https://news.nate.com/recent?mid=n0100&type=c&page={}".format(page)
            response = requests.get(list_url, headers=headers)

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')
                rows = html.select("#newsContents > div.postListType.noListTitle > div.postSubjectContent > div")

                for index, row in enumerate(rows):

                    if row.select('div > span > em') == []:
                        logger.info('확인요망')
                        continue

                    title, content_url, date_time = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)

                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')

                        content, chul_cheo = filter_02(content_html)

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
