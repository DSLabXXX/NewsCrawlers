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

    def get_articles(self, page, date, json_file):
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
            # Check 有超連結，表示文章存在，未被刪除 and 發文日期正確
            if d.find('a') and d.find('div', 'date').string.strip() == date:
                href = self.main + d.find('a')['href']
                # Check link is not redundant.
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

        res = self.session.get(page, verify=False)
        soup = BeautifulSoup(res.text, 'lxml')

        for article in soup.select('.r-ent'):
            try:
                yield self.main + article.select('.title')[0].select('a')[0].get('href')
            except Exception as e:
                # (本文已被刪除)
                # logging.exception(e)
                print(e)

    def pages(self, board=None, index_range=None):

        target_page = self.root + board + '/index'

        if index_range is None:
            yield target_page + '.html'
        else:
            for index in index_range:
                yield target_page + str(index) + '.html'

    def parse_date(self, date_data):
        try:
            # process time ex.2017 08 8 -> 2017 08 08
            if len(date_data[2]) == 1:
                date_data[2] = '0' + date_data[2]

            date = date_data[-1] + self.moon_trans[date_data[1]] + date_data[2]
            # split 16:24:41
            for i in date_data[3].split(':'):
                date += i
        except Exception as e:
            print(e)
            print(u'在分析 date 時出現錯誤')
        return date

    def parse_url(self, links):
        try:
            img_urls = []
            link_urls = []
            for link in links:
                if re.match(r'\S+?\.(?:jpg|jpeg|gif|png)', link['href']) or re.match(r'^https?://(i.)?(m.)?imgur.com', link['href']):
                    img_urls.append(link['href'])
                else:
                    link_urls.append(link['href'])
        except Exception as e:
            print(e)
            print(u'在分析 url 時出現錯誤')
        return img_urls, link_urls

    def title_word_replace(self, text):
        text = re.sub(r'([0-9])/([0-9])', r'\1_\2', text)
        text = text.replace('/', ' ')
        return text

    def parse_article(self, url):
        raw = self.session.get(url, verify=False)
        soup = BeautifulSoup(raw.text, 'lxml')

        try:
            article = dict()

            article['URL'] = url

            # 取得文章作者與文章標題
            article['Author'] = soup.select('.article-meta-value')[0].contents[0].split(' ')[0]
            article['Title'] = self.title_word_replace(soup.select('.article-meta-value')[2].contents[0])

            # 取得文章 Date
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
            print(e)
            print(u'在分析 %s 時出現錯誤' % url)

        return article

    def save_article(self, board, filename, data):
        try:
            # check folder
            file_path = self.file_root + 'Ptt/' + board + '/' + data['Date'][0:8] + '/'
            if not os.path.isdir(file_path):
                os.makedirs(file_path)

            with open(file_path + filename + '.json', 'w') as op:
                # op.write(json.dumps(data, indent=4, ensure_ascii=False).encode('utf-8'))
                json.dump(data, op, indent=4, ensure_ascii=False)
        except Exception as e:
            print(e)
            print(u'在 Check Folder or Save File 時出現錯誤')

    def crawl(self, board='Gossiping', start=1, end=2, sleep_time=0.5):
        self.board = board
        crawl_range = range(start, end)
        # for Test
        # art = self.parse_article('https://www.ptt.cc/bbs/Gossiping/M.1487731045.A.EFD.html')
        # art = self.parse_article('https://www.ptt.cc/bbs/Gossiping/M.1497232034.A.13B.html')
        for page in self.pages(board, crawl_range):
            for article in self.articles(page):
                art = self.parse_article(article)
                self.save_article(board, '%s_' % art['Date'] + str(art['Title']) + '_%s' % art['Author'], art)
                time.sleep(sleep_time)

            print(u'已經完成 %s 頁面第 %d 頁的爬取' % (board, start))
            start += 1

    def find_page_date(self, board, date):
        url = self.root + board + '/index.html'

        while url:
            res = self.session.get(url, verify=False)
            soup = BeautifulSoup(res.text, 'lxml')

            divs = soup.find_all('div', 'r-ent')
            for d in divs:
                # 發文日期 & date equal
                if d.find('div', 'date').string.strip() == date:
                    return url

            # 取得上一頁的連結
            paging_div = soup.find('div', 'btn-group btn-group-paging')
            url = self.main + paging_div.find_all('a')[1]['href']

    def auto_crawl(self, board='Gossiping', date_path=None, sleep_time=0.5):
        """
        :param board: str, PTT board name like 'Gossiping'
        :param date_path: str, format= %Y%m%d ex.'20170613' 
        :param sleep_time: float, every epoch sleep sleep_time sec
        """
        today_articles = list()

        if not date_path:
            # 不指定就爬今天
            date = time.strftime('%m/%d').lstrip('0')
            date_path = time.strftime('%Y%m%d')
        else:
            # date = date_path[4:]
            # date = (date[:2] + '/' + date[2:]).lstrip('0')
            date = time.strftime('%m/%d', time.localtime(time.mktime(time.strptime(date_path, '%Y%m%d')))).lstrip('0')

        first_page = self.find_page_date(board, date)
        print('first_page : %s' % first_page)

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
        print(len(today_articles))

        for art_link in today_articles:
            art = self.parse_article(art_link)
            try:
                file_name = '%s_' % art['Date'] + str(art['Title']) + '_%s' % art['Author']
            except Exception as e:
                print(e)
                file_name = 'UnkownFileName_%d' % time.time()
            self.save_article(board, file_name, art)
            time.sleep(sleep_time)


def main():

    crawler = PttCrawler()
    # crawler.crawl(board='Gossiping', start=22500, end=22501)
    # crawler.auto_crawl(board='Gossiping')
    crawler.auto_crawl(board='Gossiping', date_path='20170611')

    # for test
    # art = crawler.parse_article('https://www.ptt.cc/bbs/Gossiping/M.1497312830.A.755.html')
    # art = crawler.parse_article('https://www.ptt.cc/bbs/Gossiping/M.1497338721.A.E5B.html')

    # print(crawler.find_page_date('Gossiping', '6/13'))

if __name__ == '__main__':
    main()
