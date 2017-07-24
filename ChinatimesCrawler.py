# -*- coding: UTF-8 -*-

import requests
import time

from bs4 import BeautifulSoup
from bs4.element import Tag

from Common import cal_days, check_folder, check_meta, title_word_replace, trans_date_format

from Crawler import Crawler


class ChinatimesCrawler(Crawler):
    domain = 'http://www.chinatimes.com'
    root = domain + '/history-by-date/'

    root_num = '-2601'
    news_name = 'Chinatimes'
    carwler_name = 'ChinatimesCrawler'

    def articles(self, pages, meta):
        for page in pages:
            print('\npage: ', page)
            res = self.session.get(page, verify=False)
            soup = BeautifulSoup(res.text, 'lxml')

            for ul in soup.select('div.listRight'):
                for li in ul.select('li'):
                    href = self.domain + li.find('a')['href']
                    cate = li.select('.kindOf a')[0].text.replace('\r\n                            ', '')
                    if href not in meta:
                        yield cate, href

    def next_page(self, page):
        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')
        if soup.select('div.pagination'):
            for i in soup.select('div.pagination ul')[0]:
                if type(i) == Tag:
                    if i.find('a') and i.find('a').text == '下一頁':
                        return self.domain + i.find('a')['href']
        return None

    def parse_article(self, catergory, url):
        try:
            raw = self.session.get(url, verify=False)
            soup = BeautifulSoup(raw.text, 'lxml')

            article = dict()

            article['URL'] = url

            article['Category'] = catergory
            article['BigCategory'] = ''

            # 取得文章標題
            title = soup.select('.clear-fix h1')[0].contents[0]
            article['Title'] = title_word_replace(title)

            # 取得文章 Date 如 '20170313' 其實可以用傳的就好
            date = soup.select('time')[0].text
            article['Date'] = time.strftime('%Y%m%d%H%M%S', time.strptime(date, '%Y年%m月%d日 %H:%M'))

            # 取得內文
            content = ''
            # .newinfoall+ .clear-fix
            tags = soup.select('article.clear-fix')[0].select('p')
            for tag in tags:
                if tag.text != '(中國時報)':
                    content += tag.text
            article['Content'] = content

            # 取得圖片連結
            img_link = []
            for img in soup.select('.img_view img'):
                img_link.append(img['src'])
            article['ImgUrl'] = img_link
            article['LinkUrl'] = []

            # get keywords
            keywords = []
            keys = soup.select('.a_k a')
            for k in keys:
                keywords.append(k.text)
            article['KeyWord'] = keywords

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
        format_ct = "%Y-%m-%d" + self.root_num
        for day_page, date in self.pages(cal_days(start, end, format_in="%Y%m%d", format_out=format_ct)):
            date = trans_date_format(date, format_ct, "%Y%m%d")
            file_path = self.file_root + self.news_name + '/' + date + '/'
            check_folder(file_path)

            meta_path = file_path + date + '.json'
            meta_old = check_meta(meta_path)

            # 找出所有頁面
            page = day_page
            pages = []
            while page:
                page = self.next_page(page)
                if page:
                    pages.append(page)

            for catergory, article in self.articles(pages, meta_old):
                # print(catergory, article)
                art = self.parse_article(catergory, article)
                # print(art)
                try:
                    file_name = '%s_' % art['Date'] + str(art['Title'])
                except Exception as e:
                    self.log.exception(e)
                    file_name = 'UnkownFileName_%d' % time.time()
                self.save_article(file_name, art, meta_old, meta_path, send=send)

                time.sleep(sleep_time)


# 工商時報
class BusinessTimesCrawlwer(ChinatimesCrawler):
    root_num = '-2602'
    news_name = 'BusinessTimes'
    carwler_name = 'BusinessTimesCrawler'


# 旺報
class DogNewsCrawler(ChinatimesCrawler):
    root_num = '-2603'
    news_name = 'DogNews'
    carwler_name = 'DogNewsCrawler'


# 即時新聞（中時電子報）
class ChinaElectronicsNewsCrawler(ChinatimesCrawler):
    root_num = '-2604'
    news_name = 'ChinaElectronicsNews'
    carwler_name = 'ChinaElectronicsNewsCrawler'


if __name__ == '__main__':
    start = '20170723'
    end = '20170723'
    send = True

    china_times = ChinatimesCrawler()
    business_times = BusinessTimesCrawlwer()
    dog = DogNewsCrawler()
    china_electronics = ChinaElectronicsNewsCrawler()

    china_times.crawl_by_date(start, end, send=send)
    business_times.crawl_by_date(start, end, send=send)
    dog.crawl_by_date(start, end, send=send)
    china_electronics.crawl_by_date(start, end, send=send)

