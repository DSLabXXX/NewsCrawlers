# -*- coding: UTF-8 -*-

import requests
import time

from bs4 import BeautifulSoup
from bs4.element import Tag

from Common import cal_days, check_folder, check_meta, title_word_replace

from Crawler import Crawler


class AppleCrawler(Crawler):
    domain = 'http://www.appledaily.com.tw'
    root = domain + '/appledaily/archive/'

    news_name = 'AppleDaily'
    carwler_name = 'AppleCrawler'

    # 依據給予的頁面找尋可爬的新聞連結
    def articles(self, page, meta):
        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')

        for div in soup.select('div.abdominis'):
            for tag in div:
                try:
                    if type(tag) is Tag:
                        if tag.name == 'section':
                            if tag.select('article.nclns'):
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
                                if href not in meta:
                                    yield catergory, href
                except Exception as e:
                    self.log.exception(e)
                    self.log.error('在解析首頁超連結 %s 時出現問題' % page)

    # 解析 Apple 新聞文章內容
    def parse_article(self, catergory, url):
        try:
            raw = self.session.get(url, verify=False)
            soup = BeautifulSoup(raw.text, 'lxml')

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
            article['Title'] = title_word_replace(title)

            # 取得文章 Date 如 '20170313' 其實可以用傳的就好
            date = soup.select('#maincontent time')[0].contents[0]
            article['Date'] = time.strftime('%Y%m%d', time.strptime(date, '%Y年%m月%d日'))

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
                try:
                    img_link.append(img.find('a')['href'])
                except Exception as e:
                    self.log.exception(e)
            article['ImgUrl'] = img_link
            article['LinkUrl'] = []

            # Other keys
            # -----------------------------------------------------------------------
            article['Push'] = ''
            article['Author'] = ''
            article['AuthorIp'] = ''
            # for NLP
            article['KeyWord'] = []
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

    def crawl_by_date(self, start=None, end=None, sleep_time=.87, send=False):
        for day_page, date in self.pages(cal_days(start, end, format_in="%Y%m%d", format_out="%Y%m%d")):
            file_path = self.file_root + self.news_name + '/' + date + '/'
            check_folder(file_path)

            meta_path = file_path + date + '.json'
            meta_old = check_meta(meta_path)

            for catergory, article in self.articles(day_page, meta_old):
                art = self.parse_article(catergory, article)
                try:
                    file_name = '%s_' % art['Date'] + str(art['Title'])
                except Exception as e:
                    self.log.exception(e)
                    file_name = 'UnkownFileName_%d' % time.time()
                self.save_article(file_name, art, meta_old, meta_path, send=send)

                time.sleep(sleep_time)


if __name__ == '__main__':
    apple = AppleCrawler()
    # apple.crawl_by_date('20170713', '20170714')
    # apple.crawl_by_date('20170717', '20170718')
    apple.crawl_by_date('20170714', '20170715', send=True)
