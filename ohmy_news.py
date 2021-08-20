# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import traceback
from datetime import datetime
import requests
import re
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

    title = row.select('div > div > dl > dt > a')[0].text

    content_url = 'http://www.ohmynews.com' + row.select('div > div > dl > dt > a')[0].attrs['href'].replace('http://star.ohmynews.com', '')

    jag_seong_ja = row.select('div > div > p > a')[0].text

    reg_datetime = row.select('div > div > p.source')[0].contents[2]
    reg_datetime = '20' + reg_datetime.replace('.', '-') + ':00'

    return title, content_url, jag_seong_ja, reg_datetime


def filter_02(content_html):

    if content_html.select('div.at_contents') != []:

        content = content_html.select("div.at_contents")[0]

        strong_tag_list = content.find_all('strong')
        for strong_tag in strong_tag_list:
            strong_tag.extract()
        table_tag_list = content.find_all('table')
        for table_tag in table_tag_list:
            table_tag.extract()
        div_tag_list = content.find_all('div')
        for div_tag in div_tag_list:
            div_tag.extract()
        content = content.text.strip()[0:9000]
        EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
        content = EMOJI.sub(r'', content)

        chul_cheo = '오마이뉴스'

        return content, chul_cheo

    elif content_html.select('div.text') != []:

        content = content_html.select('div.text')[0]

        strong_tag_list = content.find_all('strong')
        for strong_tag in strong_tag_list:
            strong_tag.extract()
        table_tag_list = content.find_all('table')
        for table_tag in table_tag_list:
            table_tag.extract()
        div_tag_list = content.find_all('div')
        for div_tag in div_tag_list:
            div_tag.extract()
        content = content.text.strip()[0:9000]
        EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
        content = EMOJI.sub(r'', content)

        chul_cheo = '오마이뉴스'

        return content, chul_cheo

    elif content_html.select('.at_contents') != []:

        content = content_html.select('.at_contents')[0]

        strong_tag_list = content.find_all('strong')
        for strong_tag in strong_tag_list:
            strong_tag.extract()
        table_tag_list = content.find_all('table')
        for table_tag in table_tag_list:
            table_tag.extract()
        div_tag_list = content.find_all('div')
        for div_tag in div_tag_list:
            div_tag.extract()
        content = content.text.strip()[0:9000]
        EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
        content = EMOJI.sub(r'', content)

        chul_cheo = '오마이뉴스'

        return content, chul_cheo

    elif content_html.select('.article_body') != []:

        content = content_html.select('.article_body')[0]

        strong_tag_list = content.find_all('strong')
        for strong_tag in strong_tag_list:
            strong_tag.extract()
        table_tag_list = content.find_all('table')
        for table_tag in table_tag_list:
            table_tag.extract()
        div_tag_list = content.find_all('div')
        for div_tag in div_tag_list:
            div_tag.extract()
        content = content.text.strip()[0:9000]
        EMOJI = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
        content = EMOJI.sub(r'', content)

        chul_cheo = '오마이뉴스'

        return content, chul_cheo


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "http://www.ohmynews.com/NWS_Web/Articlepage/Total_Article.aspx?PAGE_CD=N0120&pageno={}".format(page)
            response = requests.get(list_url, headers=headers)

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')
                rows = html.select("ul.list_type1 > li")

                for row in rows:

                    title, content_url, jag_seong_ja, reg_datetime = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)

                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')
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

