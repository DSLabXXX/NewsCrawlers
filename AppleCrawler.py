# -*- coding: UTF-8 -*-

import json
import requests
import time
import os
import re

from bs4 import BeautifulSoup
from bs4.element import Tag

from LinkKafka import send_json_kafka
from Common import cal_days

from Crawler import Crawler


class AppleCrawler(Crawler):
    domain = 'http://www.appledaily.com.tw'
    root = domain + '/appledaily/archive/'

    news_name = 'AppleDaily'

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
                big_category = soup.select('label a')[0].text.replace(u'\xa0', u'')
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

            article['Source'] = self.news_name

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
        for day_page, date in self.pages(cal_days(start, end, format_in="%Y%m%d", format_out="%Y%m%d")):
            for catergory, article in self.articles(day_page):
                print(article)
                art = self.parse_article(catergory, article)
                print(art)
                file_name = '%s_' % art['Date'] + str(art['Title'])
                # self.save_article(board, file_name, art, art_meta_new, art_meta_old, json_today, send)

                time.sleep(sleep_time)


if __name__ == '__main__':
    apple = AppleCrawler()
    # apple.crawl_by_date('20170713', '20170714')
    # apple.crawl_by_date('20170717', '20170718')
    apple.crawl_by_date('20170719')
