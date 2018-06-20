# -*- coding: UTF-8 -*-

import requests
import time

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from Common import cal_days, check_folder, check_meta, title_word_replace


# class ZhihuCrawler(Crawler):
class ZhihuCrawler():
    domain = 'https://www.zhihu.com'
    root = domain

    news_name = 'Zhihu'
    carwler_name = 'ZhihuCrawler'

    def __init__(self):
        self.browser = webdriver.PhantomJS()
        # self.browser = webdriver.Chrome()
        self.meta = {}

    def page(self):
        target_page = self.root
        return [target_page]

    def articles(self, pages):
        rlt_title = list()
        rlt_url = list()
        for page in pages:
            self.browser.get(page)

            time.sleep(0.5)

            source = self.browser.page_source
            soup = BeautifulSoup(source, 'lxml')

            for link in soup.select('.ContentItem-title a'):
                # This type of url belongs with 專欄 ex.'//zhuanlan.zhihu.com/p/38205171'
                if '//' in link['href']:
                    continue
                title = link.text
                # ex.'https://www.zhihu.com' + '/question/280945780/answer/417286611'
                href = self.root + link['href']

                # yield one by one time
                # if title not in self.meta:
                #     yield title, href
                # return all
                if title not in self.meta:
                    rlt_title.append(title)
                    rlt_url.append(href)
                    self.meta[title] = href
        return rlt_title, rlt_url

    def parse_article(self, titles, urls):
        for i in range(len(titles)):
            rlt_title = list()
            rlt_url = list()
            print('-\ntarget:{0}'.format(titles[i]))
            print(urls[i])
            try:
                self.browser.get(urls[i])
                time.sleep(0.5)
                source = self.browser.page_source
                soup = BeautifulSoup(source, 'lxml')

                found_tags = soup.select('.SimilarQuestions-item .Button--plain')
                if found_tags:
                    for tag in found_tags:
                        time.sleep(0.78)
                        similar_title = tag.text
                        similar_url = self.root + tag['href']
                        if similar_title not in self.meta:
                            print(similar_title)
                            print(similar_url)
                            rlt_title.append(similar_title)
                            rlt_url.append(similar_url)
                            self.meta[similar_title] = similar_url
            except Exception as e:
                print(e)
            self.parse_article(rlt_title, rlt_url)

    def activate(self):
        # for topic, url in self.articles(self.page()):
        #     self.meta[topic] = url
        #     self.parse_article(topic, url)
        #     print('='*10)
        topic, url = self.articles(self.page())
        self.parse_article(titles=topic, urls=url)
        self.browser.close()

if __name__ == '__main__':
    st = time.time()
    ZC = ZhihuCrawler()
    ZC.activate()
    # ZC.parse_article(url='https://www.zhihu.com/question/277246072/answer/402653357')
    print(time.time() - st)

