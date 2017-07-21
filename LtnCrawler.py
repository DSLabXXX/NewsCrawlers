# -*- coding: UTF-8 -*-

import requests
import time

from bs4 import BeautifulSoup
from bs4.element import Tag

from Common import cal_days, check_folder, check_meta

from Crawler import Crawler


class LtnCrawler(Crawler):
    # 自由時報 Liberty Time Net
    # http://news.ltn.com.tw/list/newspaper/focus/20170718
    domain = 'http://news.ltn.com.tw'
    root = domain + '/list/newspaper/focus/'

    news_name = 'LTN'

    cate_trans = {'focus': '焦點', 'politics': '政治', 'society': '社會', 'local': '地方',
                  'life': '生活', 'opinion': '評論', 'world': '國際', 'business': '財經',
                  'sports': '體育', 'entertainment': '影視', 'consumer': '消費', 'supplement': '副刊',
                  'culture': '文化週報'}

    def classes(self, page):
        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')

        for div in soup.select('ul.newsSort'):
            for tag in div:
                try:
                    if type(tag) is Tag and tag.name == 'li':
                        yield self.domain + '/' + tag.find('a')['href']
                except Exception as e:
                    self.log.exception(e)
                    self.log.error('在解析首頁超連結時出現問題')

    def articles(self, page, meta):
        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')

        for ul in soup.select('.list'):
            for li in ul.select('li'):
                href = self.domain + li.select('a.ph')[0]['href']
                cate = li.select('.newspapertag')[0].text
                if href not in meta:
                    yield cate, href

    def add_content(self, tag, content):
        if tag.name == 'h4':
            content += '\n' + tag.text + '\n'
        elif tag.name == 'p':
            content += tag.text
        return content

    def custom_sport(self, soup, content, img_link):
        date = soup.select('.c_time')[0].contents[0]
        date = time.strftime('%Y%m%d%H%M%S', time.strptime(date, '%Y/%m/%d %H:%M'))
        for tag in soup.find(itemprop='articleBody'):
            if tag.name:
                if tag.find('img'):
                    img_link.append(tag.find('img')['src'])
                else:
                    content = self.add_content(tag, content)
        return date, content, img_link

    def custom_entertainment(self, soup, content, img_link):
        date = soup.select('.news_content .date')[0].contents[0]
        date = time.strftime('%Y%m%d%H%M%S', time.strptime(date, '%Y/%m/%d %H:%M'))
        for tag in soup.select('#ob')[0]:
            if tag.name:
                if tag.find('img'):
                    img_link.append(tag.find('img')['data-original'])
                else:
                    content = self.add_content(tag, content)
        return date, content, img_link

    def custom_opinion(self, soup, content, img_link):
        date = soup.select('.writer_date')[0].contents[0]
        date = time.strftime('%Y%m%d%H%M%S', time.strptime(date, '%Y-%m-%d %H:%M'))
        for tag in soup.select('div.cont')[0]:
            if tag.name:
                if tag.find('img'):
                    img_link.append(tag.find('img')['src'])
                else:
                    content = self.add_content(tag, content)
        return date, content, img_link

    def parse_article(self, category, url):
        raw = self.session.get(url, verify=False)
        soup = BeautifulSoup(raw.text, 'lxml')

        try:
            article = dict()

            article['URL'] = url

            article['Category'] = category

            # 處理大標題
            big_category = ''
            for cate in self.cate_trans:
                if cate in url:
                    big_category = self.cate_trans.get(cate)
            article['BigCategory'] = big_category

            # 取得文章標題
            if soup.select('h2'):
                title = soup.select('h2')[0].contents[0]
            else:
                title = soup.select('h1')[0].contents[0].replace('				', '')
            article['Title'] = title

            # 取得文章 Date 如 '20170313' or '20170313060000'
            # 取得內文 '應為美老虎隊的編隊飛行表演隊成員六十三歲的應天華...'
            # 取得圖片連結 ['www.asdfaf.jpg', ...]
            content = ''
            img_link = []
            keyword = []
            if article['BigCategory'] == '體育':
                date, content, img_link = self.custom_sport(soup, content, img_link)
            elif article['BigCategory'] == '影視':
                date, content, img_link = self.custom_entertainment(soup, content, img_link)
            elif article['BigCategory'] == '評論':
                date, content, img_link = self.custom_opinion(soup, content, img_link)
            else:
                date = soup.select('.text span')[0].contents[0]
                date = time.strftime('%Y%m%d', time.strptime(date, '%Y-%m-%d'))
                # 取得內文
                for tag in soup.select('div.text')[0]:
                    content = self.add_content(tag, content)
                # 取得關鍵字
                for key in soup.select('.keyword a'):
                    keyword.append(key.text)

            article['Date'] = date
            article['Content'] = content
            article['ImgUrl'] = img_link
            article['LinkUrl'] = []
            article['KeyWord'] = keyword

            # Other keys
            # -----------------------------------------------------------------------
            article['Push'] = ''
            article['Author'] = ''
            article['AuthorIp'] = ''
            # for NLP
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

        for day_page, date in self.pages(cal_days(start, end)):
            file_path = self.file_root + self.news_name + '/' + date + '/'
            check_folder(file_path)

            meta_path = file_path + date + '.json'
            meta_old = check_meta(meta_path)

            for page in self.classes(day_page):
                for catergory, article in self.articles(page, meta_old):
                    art = self.parse_article(catergory, article)
                    print(art)
                    file_name = '%s_' % art['Date'] + str(art['Title'])
                    # self.save_article(file_name, art, meta_old, meta_path, send=send)

                    time.sleep(sleep_time)


if __name__ == '__main__':
    ltn = LtnCrawler()
    ltn.crawl_by_date('20170716', '20170718', send=False)

    # test
    # art = ltn.parse_article('sss', 'http://news.ltn.com.tw/news/business/paper/1119589')
    # art = ltn.parse_article('opinion', 'http://news.ltn.com.tw/news/opinion/paper/1119929')
    # art = ltn.parse_article('sport', 'http://news.ltn.com.tw/news/sports/paper/1119588')
    # art = ltn.parse_article('影視焦點', 'http://news.ltn.com.tw/news/entertainment/paper/1119851')
