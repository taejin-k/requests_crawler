# -*- coding: utf-8 -*-
"""NAVER 금융 종목뉴스"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import traceback
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import re
import argparse
import json
import pickle
import time
import requests
from bs4 import BeautifulSoup
import pymysql
from mylib.common import isPid, createPid, deletePid, getLogLevel, check_date_time, insert_start, update_end, insert_msg

logger = logging.getLogger()
logger.setLevel(getLogLevel("CATCHONDATA_LOG_LEVEL"))
formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)1.1s %(lineno)3s:%(funcName)-16.16s %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger = logging.getLogger(__file__)

SU_JIB_WON_ID = 35
MAX_PAGE = int(os.getenv('CATCHONDATA_12_MAX_PAGE', '10'))
ROW_PER_PAGE = int(os.getenv('CATCHONDATA_12_ROW_PER_PAGE', '20'))
MAX_COMPARE_ROW = MAX_PAGE * 3 * ROW_PER_PAGE

current_dir = os.path.dirname(os.path.abspath(__file__))
current_filename = os.path.splitext(os.path.basename(__file__))[0]


def batch(start_id, end_id):
    start = datetime.now()
    db = None
    crawling_cmp_cnt = 0
    crawling_cnt = 0
    start_end_id = 0

    try:
        start_end_id = insert_start(SU_JIB_WON_ID, os.getpid(), start_cmp_id=start_id, end_cmp_id=end_id)

        db = pymysql.connect(host=os.environ['CATCHONDATA_HOST'], port=int(os.environ['CATCHONDATA_PORT']),
                             user=os.environ['CATCHONDATA_USER'], passwd=os.environ['CATCHONDATA_PASSWD'],
                             db=os.environ['CATCHONDATA_DB'], charset='utf8')

        cmp_list = None
        with db.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("select service, code from company where id between %s and %s order by id", (start_id, end_id))
            cmp_list = cursor.fetchall()

        if db is not None:
            db.close()
            db = None

        index = start_id
        for row in cmp_list:

            cnt = crawling(row['code'], index, end_id)

            if cnt is None:
                continue

            if cnt > 0:
                crawling_cmp_cnt += 1
                crawling_cnt += cnt
            index += 1

    except Exception as e:
        msg = "{}".format(traceback.format_exc())
        logger.error(msg)
        insert_msg(SU_JIB_WON_ID, os.getpid(), 'F', msg=msg)

    finally:
        time = (datetime.now() - start).total_seconds()
        logger.info("-" * 80)
        logger.info("총 소요시간({:.2f}), 업체수({}) 크롤링 업체수({}) 인서트 건수({})".format(time, len(cmp_list), crawling_cmp_cnt, crawling_cnt))

        if db is not None:
            db.close()

        update_end(start_end_id, 'E', crawling_cmp_cnt=crawling_cmp_cnt, crawling_cnt=crawling_cnt)


def filter_01(response):
    html = BeautifulSoup(response.text, 'html.parser')

    if html.select('table.Nnavi > tr > td > a') == []:
        return None, None

    page_no_list = html.select("table.Nnavi > tr > td > a")
    rex_no = re.compile('[0-9]+')

    for page_no in page_no_list:
        rex_match = rex_no.match(page_no.text.strip())

        if rex_match is not None:
            max_page = int(rex_match.group())

    tbody = html.select("body")[0]
    tag_list_01 = tbody.find_all('tr', 'relation_lst')
    for tag_01 in tag_list_01:
        tag_01.extract()
    rows = tbody.select('div.tb_cont > table.type5 > tbody > tr')

    return max_page, rows


def filter_02(row):
    date_time = row.select("td")[2].text
    rex_date_time = re.compile('([0-9]{4}).([0-9]{2}).([0-9]{2})[.]* ([0-9]{2}:[0-9]{2})')
    rex_search = rex_date_time.search(date_time)
    date_time = "{}-{}-{} {}:00".format(rex_search.group(1), rex_search.group(2), rex_search.group(3), rex_search.group(4))

    title = row.select("td")[0].text.strip()
    title = title.replace('\r\n', '')
    title = title.replace('\r', '')
    list_title = title.replace('\n', '')

    content_url = row.select("td.title > a")[0].attrs['href'].strip()
    content_url = 'https://finance.naver.com' + content_url
    content_url = content_url.split('&page')[0]

    chul_cheo = row.select("td")[1].text.strip()

    return date_time, list_title, content_url, chul_cheo


def filter_03(content_html):

    title = content_html.select('.p15')[0].text

    content = content_html.select("#news_read")[0]
    div_tag_list = content.find_all('div')
    for div_tag in div_tag_list:
        div_tag.extract()
    a_tag_list = content.find_all('a')
    for a_tag in a_tag_list:
        a_tag.extract()
    content = content.text.strip().replace('\\r', os.linesep)
    content = content.replace('\r', os.linesep)[0:10000]

    return title, content


def crawling(code, current_id, end_id):
    start = datetime.now()
    list_url = None
    content_url = None
    insert_cnt = 0
    duplication_cnt = 0
    max_page = MAX_PAGE

    try:
        logger.info("{} {:<4}/{:>4}".format(code, current_id, end_id))
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'
        }
        new_list = []
        last_dict = select_last_data(code, current_id)

        for page in range(1, MAX_PAGE + 1):

            if max_page < page:
                break

            list_url = "https://finance.naver.com/item/news_news.nhn?code={}&page={}&sm=title_entity_id.basic&clusterId=".format(code, page)
            response = requests.get(list_url, headers=headers)

            if response.status_code == 200:

                max_page, rows = filter_01(response)

                if rows is None:
                    logger.info('게시글이 존재하지 않습니다')
                    break

                for row in rows:

                    date_time, list_title, content_url, chul_cheo = filter_02(row)

                    if last_dict.get(content_url):
                        duplication_cnt += 1
                        logger.info("{:<2}/{:>2} {} [DUP] {} {}".format(page, max_page, code, date_time, list_title))
                    else:
                        response_content = requests.get(content_url, headers=headers)
                        if response_content.status_code == 200:
                            content_html = BeautifulSoup(response_content.text, 'html.parser')

                            if content_html.select('.p15') == []:
                                continue

                            title, content = filter_03(content_html)

                            if content is None or content == '':
                                continue

                            logger.info("{:<2}/{:>2} {} [NEW] {} {}".format(page, max_page, code, date_time, title))

                            new_list.insert(0, (check_date_time(date_time), SU_JIB_WON_ID, code, title, content,
                                                content_url, chul_cheo))
                            last_dict[content_url] = len(new_list)
                            insert_cnt += 1

                        else:
                            msg = "[본문 크롤링 오류] {} {} {}".format(code, response.status_code, content_url)
                            logger.warning(msg)
                            insert_msg(SU_JIB_WON_ID, os.getpid(), 'B', msg=msg)
            else:
                msg = "[목록 크롤링 오류] {} {} {}".format(code, response.status_code, list_url)
                logger.warning(msg)
                insert_msg(SU_JIB_WON_ID, os.getpid(), 'B', msg=msg)
                break

        insert_new_data(code, current_id, last_dict, new_list)

    except Exception as e:
        msg = "list_url={}{}content_url={}{}{}".format(list_url, os.linesep, content_url, os.linesep, traceback.format_exc())
        logger.error(msg)
        insert_msg(SU_JIB_WON_ID, os.getpid(), 'F', msg=msg)

    finally:
        elapsed_time = (datetime.now() - start).total_seconds()
        logger.info("- 소요시간  {:5.2f}  신규 {:>3} 중복 {:>3}".format(elapsed_time, insert_cnt, duplication_cnt))

    return insert_cnt


def select_last_data(code, current_id):
    db = None
    row_dict = None
    pickle_name = current_dir + os.sep + 'pickle' + os.sep + '{}_{:04}_{}.pck'.format(current_filename, current_id, code)

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
                    "select url from raw_data_jmtl where date_time >= date_sub(now(), interval 6 day) and su_jib_won_id = %s and code = %s order by id desc limit %s",
                    (SU_JIB_WON_ID, code, MAX_COMPARE_ROW))
                row_list = cursor.fetchall()

                if row_list == ():
                    row_dict = {}
                else:
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


def insert_new_data(code, current_id, last_dict, new_list):
    if len(new_list) == 0:
        return

    db = None
    try:
        db = pymysql.connect(host=os.environ['CATCHONDATA_HOST'], port=int(os.environ['CATCHONDATA_PORT']),
                             user=os.environ['CATCHONDATA_USER'], passwd=os.environ['CATCHONDATA_PASSWD'],
                             db=os.environ['CATCHONDATA_DB'], charset='utf8')

        with db.cursor() as cursor:
            cursor.executemany(
                "insert into raw_data_jmtl (date_time, su_jib_won_id, code, title, content, url, chul_cheo)"
                "values(%s, %s, %s, %s, %s, %s, %s)", new_list)
            db.commit()

        sorted_list = sorted(last_dict.items(), key=(lambda x: x[1]))
        new_last_dict = {}

        for index, row in enumerate(sorted_list, start=1):
            if index > MAX_COMPARE_ROW:
                break

            url = row[0]
            new_last_dict[url] = MAX_COMPARE_ROW + index

        pickle_name = current_dir + os.sep + 'pickle' + os.sep + '{}_{:04}_{}.pck'.format(current_filename, current_id, code)
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
        desc = 'NAVER 종목뉴스 크롤링'
        parser = argparse.ArgumentParser(description=desc)
        parser.add_argument("-start_id", help='시작 ID(company.id, 1부터 시작)', type=int)
        parser.add_argument("-end_id", help='종료 ID(company.id)', type=int)

        args = parser.parse_args()
        if args.start_id is None or args.end_id is None:
            parser.print_help()
            sys.exit(1)

        suffix = "_{:04}-{:04}".format(args.start_id, args.end_id)
        filename = current_dir + os.sep + "log" + os.sep + current_filename + suffix + ".log"
        handler = TimedRotatingFileHandler(filename=filename, when='midnight', backupCount=7, encoding='utf8')
        handler.suffix = '%Y%m%d'
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.info("=" * 80)
        logger.info("{}".format(desc))
        logger.info("- company 테이블 id {}부터 {}까지".format(args.start_id, args.end_id))
        logger.info("=" * 80)

        if isPid(__file__, suffix) is False:
            createPid(__file__, suffix)
        else:
            is_start = False
            msg = "이전에 시작한 작업이 끝나지 않아 종료합니다."
            logger.warning(msg)
            insert_msg(SU_JIB_WON_ID, os.getpid(), 'F', msg=msg)
            sys.exit(1)
        is_start = True

        batch(args.start_id, args.end_id)

    except Exception as e:
        logger.error(traceback.format_exc())

    finally:
        if is_start:
            deletePid(__file__, suffix)
