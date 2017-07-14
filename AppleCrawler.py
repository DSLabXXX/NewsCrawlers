# -*- coding: UTF-8 -*-
# The target of this code is to crawl data from Gossiping broad of PTT
# This code is modify from https://github.com/zake7749/PTT-Chat-Generator

import json
import requests
import time
import datetime
import os
import re

from bs4 import BeautifulSoup
from bs4.element import NavigableString

from LinkKafka import send_json_kafka

import logging
import logging.config


class AppleCrawler(object):
    domain = 'http://www.appledaily.com.tw'
    root = domain + '/appledaily/archive/'

    # File path. Will be removed in later version and using config file to instead of it.
    file_root = '/data1/'

    moon_trans = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                  'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                  'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

    def __init__(self):
        self.session = requests.session()
        requests.packages.urllib3.disable_warnings()
        self.log = logging.getLogger('AppleCrawler')
        # self.set_log_conf()

    def set_log_conf(self):
        # 設定log
        self.log.setLevel(logging.DEBUG)

        # Log file 看得到 DEBUG
        file_hdlr = logging.FileHandler('log/' + time.strftime('%Y%m%d%H%M') + '_AppleNews.log')
        file_hdlr.setLevel(logging.DEBUG)

        # Command line 看不到 DEBUG
        console_hdlr = logging.StreamHandler()
        console_hdlr.setLevel(logging.INFO)

        formatter = logging.Formatter('%(levelname)-8s - %(asctime)s - %(name)-12s - %(message)s')
        file_hdlr.setFormatter(formatter)
        console_hdlr.setFormatter(formatter)

        self.log.addHandler(file_hdlr)
        self.log.addHandler(console_hdlr)

    def pages(self, index_range=None):
        target_page = self.root

        if index_range is None:
            yield target_page
        else:
            for index in index_range:
                yield target_page + index

    def articles(self, page):
        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')

        n_child = 0
        for clearmen in soup.select('.clearmen'):
            # '.clearmen:nth-child(3)'
            if n_child != 3:
                n_child += 1
            else:
                for article in clearmen.select('.fillup a'):
                    try:
                        # 因應 href 格式不同
                        if 'home.appledaily' in article['href'] or self.domain in article['href']:
                            yield article['href']
                        else:
                            yield self.domain + article['href']
                    except Exception as e:
                        # (本文已被刪除)
                        self.log.exception(e)

    def parse_article(self, url):
        raw = self.session.get(url, verify=False)
        soup = BeautifulSoup(raw.text, 'lxml')

        try:
            article = dict()

            article['URL'] = url

            # 取得文章作者與文章標題
            article['Author'] = ''
            try:
                # 因應標題格式不同
                title = soup.select('#h1')[0].contents[0]
            except Exception as e:
                try:
                    title = soup.select('div + h1')[0].contents[0]
                except Exception as e:
                    title = ''
            article['Title'] = title

            # 取得文章 Date 如 '20170313' 其實可以用傳的就好
            date = soup.select('#maincontent time')[0].contents[0]
            article['Date'] = time.strftime('%Y%d%m', time.strptime(date, '%Y年%m月%d日'))

            # 取得內文
            content = ''
            # apple 新聞摘要 '#introid'


        except Exception as e:
            self.log.exception(e)
            self.log.error(u'在分析 %s 時出現錯誤' % url)

    def crawl_by_date(self, start=None, end=None, sleep_time=.87):
        def cal_days(begin_date=None, end_date=None):
            if end_date:
                # 找出start -> end 之間的每一天
                date_list = []
                begin_date = datetime.datetime.strptime(begin_date, "%Y%m%d")
                end_date = datetime.datetime.strptime(end_date, "%Y%m%d")
                while begin_date <= end_date:
                    date_str = begin_date.strftime("%Y%m%d")
                    date_list.append(date_str)
                    begin_date += datetime.timedelta(days=1)
                return date_list
            else:
                return [begin_date]
        for day_page in self.pages(cal_days(start, end)):
            for article in self.articles(day_page):
                print(article)
                self.parse_article(article)
                time.sleep(sleep_time)


if __name__ == '__main__':
    apple = AppleCrawler()
    # apple.crawl_by_date('20170713', '20170714')
    apple.crawl_by_date('20170713', '20170713')
