# -*- coding: utf-8 -*-
"""디지털 데일리"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import traceback
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import re
import json
import pickle
import requests
from bs4 import BeautifulSoup
import pymysql
import time
from mylib.common import isPid, createPid, deletePid, getLogLevel, check_date_time, insert_start, update_end, insert_msg

logger = logging.getLogger()
logger.setLevel(getLogLevel("CATCHONDATA_LOG_LEVEL"))
formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)1.1s %(lineno)3s:%(funcName)-16.16s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger = logging.getLogger(__file__)

SU_JIB_WON_ID = 30
MAX_PAGE = int(os.getenv('CATCHONDATA_1_MAX_PAGE', '1'))
ROW_PER_PAGE = int(os.getenv('CATCHONDATA_1_ROW_PER_PAGE', '15'))
MAX_COMPARE_ROW = MAX_PAGE * 3 * ROW_PER_PAGE

current_dir = os.path.dirname(os.path.abspath(__file__))
current_filename = os.path.splitext(os.path.basename(__file__))[0]


def filter_01(row):
    content_url = 'http://www.ddaily.co.kr' + row.select('dt > a')[0].attrs['href']

    title = row.select('dt > a')[0].text

    temp = row.select('dd > span')[0].text.split(' ')

    jag_seong_ja = temp[0]

    reg_datetime = temp[1].replace('.', '-') + ' ' + temp[2]

    return content_url, title, jag_seong_ja, reg_datetime


def filter_02(content_html):
    content = content_html.select('#news_body_area')[0].text

    chul_cheo = '디지털 데일리'

    return content, chul_cheo


def crawling():
    start = datetime.now()
    list_url = None
    content_url = None
    insert_cnt = 0
    duplication_cnt = 0
    start_end_id = 0

    try:
        start_end_id = insert_start(SU_JIB_WON_ID, os.getpid())
        new_list = []
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'
        }

        last_dict = select_last_data()

        for page in range(1, MAX_PAGE + 1):

            list_url = "http://www.ddaily.co.kr/news/article_list_all/"
            response = requests.get(list_url, headers=headers, verify=False)

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'html.parser')
                rows = html.select("#container > div > div > div > div.section_h12sub > div > div > dl")

                for row in rows[0:10]:

                    content_url, title, jag_seong_ja, reg_datetime = filter_01(row)

                    if last_dict.get(content_url):
                        duplication_cnt += 1
                        logger.info("{:<2}/{:>2} [DUP] {} {}".format(page, MAX_PAGE, reg_datetime, title))
                    else:
                        response_content = requests.get(content_url, headers=headers)

                        if response_content.status_code == 200:
                            content_html = BeautifulSoup(response_content.text, 'html.parser')
                            content, chul_cheo = filter_02(content_html)

                            if content is None or content == '' or len(content) is 0:
                                continue

                            logger.info("{:<2}/{:>2} [NEW] {} {}".format(page, MAX_PAGE, reg_datetime, title))

                            new_list.insert(0, (SU_JIB_WON_ID, content_url, check_date_time(reg_datetime),
                                                content, title, jag_seong_ja, chul_cheo))
                            last_dict[content_url] = len(new_list)
                            insert_cnt += 1

                            time.sleep(4)
                        else:
                            msg = "[본문 크롤링 오류] {} {}".format(response_content.status_code, content_url)
                            logger.warning(msg)
                            insert_msg(SU_JIB_WON_ID, os.getpid(), 'B', msg=msg)
            else:
                msg = "[목록 크롤링 오류] {} {}".format(response.status_code, list_url)
                logger.warning(msg)
                insert_msg(SU_JIB_WON_ID, os.getpid(), 'B', msg=msg)
                break

        insert_new_data(last_dict, new_list)

    except Exception as e:
        msg = "list_url={}{}content_url={}{}{}".format(list_url, os.linesep, content_url, os.linesep, traceback.format_exc())
        logger.error(msg)
        insert_msg(SU_JIB_WON_ID, os.getpid(), 'F', msg=msg)

    finally:
        update_end(start_end_id, 'E', crawling_cnt=insert_cnt)
        elapsed_time = (datetime.now() - start).total_seconds()
        logger.info("소요시간  {:5.2f}  신규 {:>3} 중복 {:>3}".format(elapsed_time, insert_cnt, duplication_cnt))

    return insert_cnt


def select_last_data():
    db = None
    row_dict = None
    pickle_name = current_dir + os.sep + 'pickle' + os.sep + '{}.pck'.format(current_filename)

    try:
        if os.path.getsize(pickle_name) > 0:
            with open(pickle_name, "rb") as f:
                row_dict = pickle.load(f)

    except FileNotFoundError as e:
        pass

    if row_dict:
        logger.info("pickle dict {:>3} (최대건:{})".format(len(row_dict), MAX_COMPARE_ROW))
        return row_dict
    else:
        try:
            db = pymysql.connect(host=os.environ['CATCHONDATA_HOST'], port=int(os.environ['CATCHONDATA_PORT']),
                                 user=os.environ['CATCHONDATA_USER'], passwd=os.environ['CATCHONDATA_PASSWD'],
                                 db=os.environ['CATCHONDATA_DB'], charset='utf8')

            row_dict = {}

            with db.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(
                    "select url from raw_data where date_time >= date_sub(now(), interval 6 day) and su_jib_won_id = %s order by date_time desc limit %s",
                    (SU_JIB_WON_ID, MAX_COMPARE_ROW))
                row_list = cursor.fetchall()

                ranking = MAX_COMPARE_ROW + 1
                for row in row_list:
                    url = row['url']

                    if row_dict.get(url) is None:
                        row_dict[url] = ranking
                        ranking += 1
            logger.info("table list {:>3} dict {:>3} {}".format(
                len(row_list), len(row_dict), "" if len(row_list) == len(row_dict) else "[중복]")
            )

            with open(pickle_name, "wb") as f:
                pickle.dump(row_dict, f)

        except Exception as e:
            raise e

        finally:
            if db is not None:
                db.close()

        return row_dict


def insert_new_data(last_dict, new_list):
    if len(new_list) == 0:
        return

    db = None
    try:
        db = pymysql.connect(host=os.environ['CATCHONDATA_HOST'], port=int(os.environ['CATCHONDATA_PORT']),
                             user=os.environ['CATCHONDATA_USER'], passwd=os.environ['CATCHONDATA_PASSWD'],
                             db=os.environ['CATCHONDATA_DB'], charset='utf8')

        with db.cursor() as cursor:
            cursor.executemany(
                "insert into raw_data (su_jib_won_id, url, date_time, content, title, jag_seong_ja, chul_cheo)"
                "values(%s, %s, %s, %s, %s, %s, %s)", new_list)
            db.commit()

        sorted_list = sorted(last_dict.items(), key=(lambda x: x[1]))
        new_last_dict = {}

        for index, row in enumerate(sorted_list, start=1):
            if index > MAX_COMPARE_ROW:
                break

            url = row[0]
            new_last_dict[url] = MAX_COMPARE_ROW + index

        pickle_name = current_dir + os.sep + 'pickle' + os.sep + '{}.pck'.format(current_filename)
        with open(pickle_name, "wb") as f:
            pickle.dump(new_last_dict, f)

    except Exception as e:
        raise e

    finally:
        if db is not None:
            db.close()


if __name__ == '__main__':
    is_start = False

    try:
        filename = current_dir + os.sep + "log" + os.sep + current_filename + ".log"
        handler = TimedRotatingFileHandler(filename=filename, when='midnight', backupCount=7, encoding='utf8')
        handler.suffix = '%Y%m%d'
        formatter = logging.Formatter('%(asctime)s %(levelname)-7s %(lineno)3s:%(funcName)-15s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        if isPid(__file__) is False:
            createPid(__file__)
        else:
            is_start = False
            msg = "이전에 시작한 작업이 끝나지 않아 종료합니다."
            logger.warning(msg)
            insert_msg(SU_JIB_WON_ID, os.getpid(), 'F', msg=msg)
            sys.exit(1)

        is_start = True
        crawling()

    except Exception as e:
        logger.error(traceback.format_exc())

    finally:
        if is_start:
            deletePid(__file__)

