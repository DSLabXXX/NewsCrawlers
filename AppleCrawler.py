# -*- coding: UTF-8 -*-

import requests
import time

from bs4 import BeautifulSoup
from bs4.element import Tag

from Common import cal_days, check_folder, check_meta, title_word_replace

from Crawler import Crawler


class AppleCrawler(Crawler):
    domain = 'http://www.appledaily.com.tw'
    domain2 = 'https://tw.appledaily.com'
    root = domain + '/appledaily/archive/'

    news_name = 'AppleDaily'
    carwler_name = 'AppleCrawler'

    # 依據給予的頁面找尋可爬的新聞連結
    def articles(self, page, meta):
        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')

        for div in soup.select('div.abdominis'):
            big_category = ''
            for tag in div:
                try:
                    if type(tag) is Tag:
                        if tag.name == 'section':
                            head = tag.select('h1')
                            if head:
                                big_category = head[0].text.strip()
                            if tag.select('article.nclns'):
                                art = tag.select('article.nclns')[0]
                        elif tag.name == 'article':
                            art = tag
                        catergory = art.find('h2').text
                        for link in art.select('.fillup a'):
                            # 蘋果的url被改過了 不適合當作key使用 以後可能要改json格式
                            href = link['href']
                            if href not in meta:
                                yield catergory, big_category, href
                except Exception as e:
                    self.log.exception(e)
                    self.log.error('在解析首頁超連結 %s 時出現問題' % page)

    # 解析 Apple 新聞文章內容
    def parse_article(self, catergory, big_category, url, date):
        try:
            if url[:12] == '/appledaily/':
                url = self.domain2 + url
            raw = self.session.get(url, verify=False)
            soup = BeautifulSoup(raw.text, 'lxml')

            article = dict()
            article['URL'] = url
            article['Category'] = catergory
            article['BigCategory'] = big_category

            # 取得文章標題
            try:
                # 因應標題格式不同
                h1 = soup.select('h1')
                if len(h1) == 1:
                    title = soup.select('h1')[0].contents[0]
                elif len(h1) > 1:
                    title = soup.select('div + h1')[0].contents[0]
            except Exception as e:
                title = ''

            article['Title'] = title_word_replace(title)

            article['Date'] = date

            # 取得內文
            content = ''
            # .ndArticle_margin p
            try:
                tags = soup.select('.ndArticle_margin')[0]
            except Exception as e:
                # 房產新聞
                tags = soup.select('.articulum')[0]
            for tag in tags:
                if tag.name == 'p' or tag.name == 'h2':
                    if tag.text != ' ':
                        content += tag.text
                        content += '\n'
            article['Content'] = content

            # 取得圖片連結
            # .ndArticle_margin img
            img_link = []
            try:
                for tag in soup.find('div', {'class': 'ndArticle_margin'}):
                    if tag.name == 'figure':
                        img_link.append(tag.find('img')['src'])
            except Exception as e:
                self.log.exception(e)
                # 地產新聞
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

    def crawl_by_date(self, start=None, end=None, sleep_time=.17, send=False):
        for day_page, date in self.pages(cal_days(start, end, format_in="%Y%m%d", format_out="%Y%m%d")):
            file_path = self.file_root + self.news_name + '/' + date + '/'
            check_folder(file_path)

            meta_path = file_path + date + '.json'
            meta_old = check_meta(meta_path)

            for catergory, big_category, article in self.articles(day_page, meta_old):
                art = self.parse_article(catergory, big_category, article, date)
                try:
                    file_name = '%s_' % art['Date'] + str(art['Title'])
                except Exception as e:
                    self.log.exception(e)
                    file_name = 'UnkownFileName_%d' % time.time()
                self.save_article(file_name, art, meta_old, meta_path, send=send)

                time.sleep(sleep_time)


if __name__ == '__main__':
    apple = AppleCrawler()
    # apple.crawl_by_date('20170930', '20171018')
    apple.crawl_by_date('20171101', '20180207')
    # apple.crawl_by_date('20170714', '20170715', send=True)
    # apple.parse_article('要聞', 'http://tw.news.appledaily.com/headline/daily/20180207/37927182/')
    # apple.parse_article('要聞', '要聞', 'http://home.appledaily.com.tw/article/index/20180201/37921117/news/', '20180201')
    # apple.article('要聞', 'http://tw.news.appledaily.com/headline/daily/20180207/37927182/')
