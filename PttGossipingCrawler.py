# -*- coding: UTF-8 -*-
# The target of this code is to crawl data from Gossiping broad of PTT
# This code is modify from https://github.com/zake7749/PTT-Chat-Generator

import json
import requests
import time
import os
import re

from bs4 import BeautifulSoup
from bs4.element import NavigableString

import logging
import logging.config


class PttCrawler(object):

    root = 'https://www.ptt.cc/bbs/'
    main = 'https://www.ptt.cc'
    gossip_data = {
        'from': 'bbs/Gossiping/index.html',
        'yes': 'yes'
    }
    board = ''

    # File path. Will be removed in later version and using config file to instead of it.
    file_root = '/data1/'

    moon_trans = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                  'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                  'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

    def __init__(self):
        self.session = requests.session()
        requests.packages.urllib3.disable_warnings()
        self.session.post('https://www.ptt.cc/ask/over18',
                          verify=False,
                          data=self.gossip_data)
        self.log = logging.getLogger('PttGossipingCrawler')
        self.set_log_conf()

    def set_log_conf(self):
        # 設定log
        self.log.setLevel(logging.DEBUG)

        # Log file 看得到 DEBUG
        file_hdlr = logging.FileHandler('log/' + time.strftime('%Y%m%d%H%M') + '_PttGossiping.log')
        file_hdlr.setLevel(logging.DEBUG)

        # Command line 看不到 DEBUG
        console_hdlr = logging.StreamHandler()
        console_hdlr.setLevel(logging.INFO)

        formatter = logging.Formatter('%(levelname)-8s - %(asctime)s - %(name)-12s - %(message)s')
        file_hdlr.setFormatter(formatter)
        console_hdlr.setFormatter(formatter)

        self.log.addHandler(file_hdlr)
        self.log.addHandler(console_hdlr)

    def get_articles(self, page, date, json_file):
        # 從指定頁面找可以爬取的文章
        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')

        # Load got files list
        with open(json_file, 'r') as rf:
            got_articles = json.load(rf)

        check_set = set()
        if len(got_articles) > 0:
            for i in got_articles:
                check_set.add(i['href'])

        articles = []  # 儲存取得的文章資料
        divs = soup.find_all('div', 'r-ent')
        for d in divs:
            # Check 有超連結，表示文章存在、未被刪除。並且發文日期正確
            if d.find('a') and d.find('div', 'date').string.strip() == date:
                href = self.main + d.find('a')['href']
                # Check link is not redundant. 避免重複爬取文章
                if href not in check_set:
                    # 取得推文數
                    push_count = 0
                    if d.find('div', 'nrec').string:
                        try:
                            push_count = int(d.find('div', 'nrec').string)  # 轉換字串為數字
                        except ValueError:  # 若轉換失敗，不做任何事，push_count 保持為 0
                            pass
                    # 取得文章標題
                    title = d.find('a').string
                    # Add to got articles list
                    articles.append(href)
                    got_articles.append({
                        'title': title,
                        'href': href,
                        'push_count': push_count
                    })

        with open(json_file, 'w') as op:
            json.dump(got_articles, op, indent=4, ensure_ascii=False)

        # 取得上一頁的連結
        paging_div = soup.find('div', 'btn-group btn-group-paging')
        prev_url = self.main + paging_div.find_all('a')[1]['href']
        return articles, prev_url

    def articles(self, page):
        # 原作者的文章回傳方式
        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')

        for article in soup.select('.r-ent'):
            try:
                yield self.main + article.select('.title')[0].select('a')[0].get('href')
            except Exception as e:
                # (本文已被刪除)
                self.log.exception(e)

    def pages(self, board=None, index_range=None):
        # 原作者的頁面回傳方式
        target_page = self.root + board + '/index'

        if index_range is None:
            yield target_page + '.html'
        else:
            for index in index_range:
                yield target_page + str(index) + '.html'

    def parse_date(self, date_data):
        # 處理爬取到的ptt日期格式
        try:
            # process time ex.2017 08 8 -> 2017 08 08
            if len(date_data[2]) == 1:
                date_data[2] = '0' + date_data[2]

            date = date_data[-1] + self.moon_trans[date_data[1]] + date_data[2]
            # split 16:24:41
            for i in date_data[3].split(':'):
                date += i
        except Exception as e:
            self.log.exception(e)
            self.log.error(u'在分析 date 時出現錯誤')
        return date

    def parse_url(self, links):
        # 處理爬取到的連結網址或圖片
        try:
            img_urls = []
            link_urls = []
            for link in links:
                if re.match(r'\S+?\.(?:jpg|jpeg|gif|png)', link['href']) or re.match(r'^https?://(i.)?(m.)?imgur.com', link['href']):
                    img_urls.append(link['href'])
                else:
                    link_urls.append(link['href'])
        except Exception as e:
            self.log.exception(e)
            self.log.error(u'在分析 url 時出現錯誤')
        return img_urls, link_urls

    def title_word_replace(self, text):
        # 避免標題出現 '/' 等無法當作檔名的符號
        # 輸入 text 為字串(str)
        text = re.sub(r'([0-9])/([0-9])', r'\1_\2', text)
        text = text.replace('/', ' ')
        return text

    def parse_article(self, url):
        # 爬取ptt文章內容
        raw = self.session.get(url, verify=False)
        soup = BeautifulSoup(raw.text, 'lxml')

        try:
            article = dict()

            article['URL'] = url

            # 取得文章作者與文章標題
            article['Author'] = soup.select('.article-meta-value')[0].contents[0].split(' ')[0]
            article['Title'] = self.title_word_replace(soup.select('.article-meta-value')[2].contents[0])

            # 取得文章 Date 如 '20170313'
            article['Date'] = self.parse_date(soup.select('.article-meta-value')[-1].contents[0].split())

            # 取得內文
            content = ''
            links = list()
            for tag in soup.select('#main-content')[0]:
                if type(tag) is NavigableString and tag != '\n':
                    content += tag
                elif tag.name == 'a':
                    links.append(tag)
            article['Content'] = content

            # 取得 Img & Link url
            article['ImgUrl'], article['LinkUrl'] = self.parse_url(links)

            # Get Author IP & Article URL
            for tag_f2 in soup.select('.f2'):
                sp = tag_f2.text.split(' ')
                if len(sp) > 1 and sp[1] == '發信站:':
                    article['AuthorIp'] = sp[-1].split('\n')[0]

            # 處理回文資訊
            upvote = 0
            downvote = 0
            novote = 0
            response_list = []

            for response_struct in soup.select('.push'):

                # 跳脫「檔案過大！部分文章無法顯示」的 push class
                if 'warning-box' not in response_struct['class']:

                    response_dic = dict()
                    # 處理推文內容 分為連結或一般內容
                    if response_struct.find('a'):
                        response_dic['Content'] = response_struct.select('a')[0].contents[0]
                    else:
                        response_dic['Content'] = response_struct.select('.push-content')[0].contents[0][2:]

                    response_dic['Vote'] = response_struct.select('.push-tag')[0].contents[0][0]
                    response_dic['User'] = response_struct.select('.push-userid')[0].contents[0]
                    response_dic['Date'] = response_struct.select('.push-ipdatetime')[0].contents[0].split('\n')[0]
                    response_list.append(response_dic)

                    if response_dic['Vote'] == u'推':
                        upvote += 1
                    elif response_dic['Vote'] == u'噓':
                        downvote += 1
                    else:
                        novote += 1

            article['Push'] = response_list
            article['UpVote'] = upvote
            article['DownVote'] = downvote
            article['NoVote'] = novote

            # Other keys
            # -----------------------------------------------------------------------
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

            article['Source'] = 'Ptt' + self.board

        except Exception as e:
            self.log.exception(e)
            self.log.error(u'在分析 %s 時出現錯誤' % url)

        return article

    def save_article(self, board, filename, data):
        # 依照給予的檔名儲存單篇文章
        try:
            # check folder
            file_path = self.file_root + 'Ptt/' + board + '/' + data['Date'][0:8] + '/'
            if not os.path.isdir(file_path):
                os.makedirs(file_path)

            with open(file_path + filename + '.json', 'w') as op:
                json.dump(data, op, indent=4, ensure_ascii=False)
        except Exception as e:
            self.log.exception(e)
            self.log.error(u'在 Check Folder or Save File 時出現錯誤\nfilename:{0}'.format(filename))

    def crawl(self, board='Gossiping', start=1, end=2, sleep_time=0.5):
        # 原作者的依照頁面範圍爬取方法
        self.board = board
        crawl_range = range(start, end)

        for page in self.pages(board, crawl_range):
            for article in self.articles(page):
                art = self.parse_article(article)
                self.save_article(board, '%s_' % art['Date'] + str(art['Title']) + '_%s' % art['Author'], art)
                time.sleep(sleep_time)

            self.log.error(u'已經完成 %s 頁面第 %d 頁的爬取' % (board, start))
            start += 1

    def find_first_page(self, board, date):
        # 根據日期尋找要從哪一頁開始往回爬
        url = self.root + board + '/index.html'

        while url:
            set_idx = {'公告', '協尋'}
            res = self.session.get(url, verify=False)
            soup = BeautifulSoup(res.text, 'lxml')

            divs = soup.find_all('div', 'r-ent')
            home_apge = 1
            for d in divs:
                # 發文日期 & date equal
                if home_apge and d.find('a'):
                    # 搜尋時跳過首頁的置底公告
                    m = re.search(r'\[(.*?)\]', d.select('.title')[0].select('a')[0].text)
                    if m:
                        if m.groups(1)[0] in set_idx :
                            home_apge = 0
                            break

                if d.find('div', 'date').string.strip() == date:
                    return url

            # 取得上一頁的連結
            paging_div = soup.find('div', 'btn-group btn-group-paging')
            url = self.main + paging_div.find_all('a')[1]['href']

    def crawl_by_date(self, board='Gossiping', date_path=None, sleep_time=0.5):
        """
        :param board: str, PTT board name like 'Gossiping'
        :param date_path: str, format= %Y%m%d ex.'20170613' 
        :param sleep_time: float, every epoch sleep sleep_time sec
        
        根據輸入日期格式 '%Y%m%d'(如 '20170313') 爬取所有當日文章
        """
        today_articles = list()

        if not date_path:
            # 不指定就爬今天
            date = time.strftime('%m/%d').lstrip('0')
            date_path = time.strftime('%Y%m%d')
        else:
            # 轉換日期格式 20170313 -> 3/13
            date = time.strftime('%m/%d', time.strptime(date_path, '%Y%m%d')).lstrip('0')

        first_page = self.find_first_page(board, date)

        file_path = self.file_root + 'Ptt/' + board + '/' + date_path + '/'

        # check folder
        if not os.path.isdir(file_path):
            os.makedirs(file_path)

        # check crawled list
        json_today = file_path + date_path + '.json'
        if not os.path.isfile(json_today):
            with open(json_today, 'w') as wf:
                json.dump(today_articles, wf)

        articles, pre_link = self.get_articles(first_page, date, json_today)
        while articles:
            today_articles += articles
            articles, pre_link = self.get_articles(pre_link, date, json_today)

        self.log.info('Crawl by date: %s' % date_path)
        self.log.info('first_page : %s' % first_page)
        self.log.info('Data store at : %s' % file_path)
        self.log.info('Num of target articles : {0}'.format(len(today_articles)))

        for art_link in today_articles:
            art = self.parse_article(art_link)
            try:
                file_name = '%s_' % art['Date'] + str(art['Title']) + '_%s' % art['Author']
            except Exception as e:
                self.log.exception(e)
                file_name = 'UnkownFileName_%d' % time.time()
            self.save_article(board, file_name, art)
            time.sleep(sleep_time)
