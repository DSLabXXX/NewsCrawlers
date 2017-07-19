# -*- coding: UTF-8 -*-

import json
import requests
import time
import datetime
import os
import re

from bs4 import BeautifulSoup
from bs4.element import Tag

from LinkKafka import send_json_kafka

import logging
import logging.config


class AppleCrawler(object):
    domain = 'http://www.appledaily.com.tw'
    root = domain + '/appledaily/archive/'

    # File path. Will be removed in later version and using config file to instead of it.
    file_root = '/data1/'
    news_name = 'AppleDaily'

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
            yield target_page, index_range
        else:
            for index in index_range:
                yield target_page + index, index

    def articles(self, page):
        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')

        for div in soup.select('div.abdominis'):
            for tag in div:
                try:
                    if type(tag) is Tag:
                        if tag.name == 'section':
                            art = tag.select('article.nclns')[0]
                        elif tag.name == 'article':
                            art = tag
                        catergory = art.find('h2').text
                        for link in art.select('.fillup a'):
                                # 因應 href 格式不同
                                if 'home.appledaily' in link['href'] or self.domain in link['href']:
                                    href = link['href']
                                else:
                                    href = self.domain + link['href']
                                yield catergory, href
                except Exception as e:
                    self.log.exception(e)
                    self.log.error('在解析首頁超連結時出現問題')

    def parse_article(self, catergory, url):
        raw = self.session.get(url, verify=False)
        soup = BeautifulSoup(raw.text, 'lxml')

        try:
            article = dict()

            article['URL'] = url

            article['Category'] = catergory
            # 處理大標題
            big_category = ''
            if soup.select('label a'):
                big_category = soup.select('label a').text.replace(u'\xa0', u'')
            article['BigCategory'] = big_category

            # 取得文章標題
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
            tags = soup.select('.articulum')[0]
            for tag in tags:
                if tag.name == 'p' or tag.name == 'h2':
                    if tag.text != ' ':
                        if tag.name == 'h2':
                            content += '\n'
                        content += tag.text
            article['Content'] = content

            # 取得圖片連結
            img_link = []
            for img in soup.select('.trans figure'):
                img_link.append(img.find('a')['href'])
            article['ImgUrl'] = img_link
            article['LinkUrl'] = []

            # Other keys
            # -----------------------------------------------------------------------
            article['Push'] = ''
            article['Author'] = ''
            article['AuthorIp'] = ''
            # for NLP
            article['KeyWord'] = ''
            article['SplitText'] = ''
            # for NER
            article['Org'] = ''
            article['People'] = ''
            article['Location'] = ''
            # for Analysis
            article['Event'] = ''
            article['HDFSurl'] = ''
            article['Value'] = ''

            article['Source'] = 'AppleDaily'

            return article
        except Exception as e:
            self.log.exception(e)
            self.log.error(u'在分析 %s 時出現錯誤' % url)

    def save_article(self, board, filename, data, meta_new, meta_old, json_today, send):
        # 依照給予的檔名儲存單篇文章
        try:
            # check folder
            file_path = self.file_root + self.news_name + '/' + data['Date'][0:8] + '/'
            if not os.path.isdir(file_path):
                os.makedirs(file_path)

            with open(file_path + filename + '.json', 'w') as op:
                json.dump(data, op, indent=4, ensure_ascii=False)

            # 存檔完沒掛掉就傳到 kafka
            if send:
                send_json_kafka(json.dumps(data))

            # 都沒掛掉就存回 meta date
            meta_old.update({data['URL']: meta_new[data['URL']]})
            with open(json_today, 'w') as wf:
                json.dump(meta_old, wf, indent=4, ensure_ascii=False)
            self.log.info('已完成爬取 %s' %data.get('Title'))

        except Exception as e:
            self.log.exception(e)
            self.log.error(u'在 Check Folder or Save File 時出現錯誤\nfilename:{0}'.format(filename))

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
        for day_page, date in self.pages(cal_days(start, end)):
            for catergory, article in self.articles(day_page):
                print(article)
                art = self.parse_article(catergory, article)
                file_name = '%s_' % art['Date'] + str(art['Title'])
                # self.save_article(board, file_name, art, art_meta_new, art_meta_old, json_today, send)

                time.sleep(sleep_time)


if __name__ == '__main__':
    apple = AppleCrawler()
    # apple.crawl_by_date('20170713', '20170714')
    # apple.crawl_by_date('20170717', '20170718')
    apple.crawl_by_date('20170714')
