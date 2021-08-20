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
    content_url = 'https://gall.dcinside.com' + row.select('td.gall_tit.ub-word > a:nth-of-type(1)')[0].attrs['href'].split('&page')[0]

    chu_cheon = row.select('td.gall_recommend')[0].text

    return content_url, chu_cheon


def filter_02(content_html):
    reg_datetime = content_html.select('span.gall_date')[0].text[0:-3] + ':00'
    reg_datetime = reg_datetime.replace('.', '-')

    content = content_html.select(".writing_view_box")[0]
    script_tag_list = content.find_all('script')
    for script_tag in script_tag_list:
        script_tag.extract()
    content = content.text.strip()

    title = content_html.select('.title_subject')[0].text

    daes_geul_su = content_html.select('.gall_comment')[0].text.replace('댓글', '')

    jo_hoe_su = content_html.select('.gall_count')[0].text.replace('조회 ', '')

    jag_seong_ja = content_html.select('.ip')[0].text.replace('(', '').replace(')', '')

    return reg_datetime, content, title, daes_geul_su, jo_hoe_su, jag_seong_ja


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None

    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}

        for page in range(1, MAX_PAGE + 1):

            list_url = "https://gall.dcinside.com/board/lists/?id=stock_new2&page={}".format(page)

            response = requests.get(list_url, headers=headers)
            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')

                rows = html.select("#container > section.left_content > article:nth-of-type(2) > div.gall_listwrap.list > table > tbody > tr")

                for row in rows[0:25]:
                    if row.select('td.gall_tit.ub-word > a:nth-of-type(1) > b') != []:
                        continue

                    content_url, chu_cheon = filter_01(row)

                    response_content = requests.get(content_url, headers=headers)
                    if response_content.status_code == 200:
                        content_html = BeautifulSoup(response_content.text, 'html.parser')

                        reg_datetime, content, title, daes_geul_su, jo_hoe_su, jag_seong_ja = filter_02(content_html)

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
        msg = "{}{}{}".format(list_url, os.linesep, content_url, os.linesep, traceback.format_exc())
        logger.error(msg)

    finally:
        elapsed_time = (datetime.now() - start).total_seconds()
        logger.info("소요시간  {:5.2f}".format(elapsed_time))


if __name__ == '__main__':
    crawling()
