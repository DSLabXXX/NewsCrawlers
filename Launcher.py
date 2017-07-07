# -*- coding: UTF-8 -*-

from PttGossipingCrawler import PttCrawler


if __name__ == '__main__':
    # 尚有文章失去標頭(header)時的狀況須處理
    # art = crawler.parse_article('https://www.ptt.cc/bbs/Gossiping/M.1496931521.A.C3E.html')

    crawler = PttCrawler()
    # crawler.crawl_by_date(board='Gossiping')
    crawler.crawl_by_date(board='Gossiping', date_path='20170705')

    # 針對 parse_article 做測試
    # art = crawler.parse_article('https://www.ptt.cc/bbs/Gossiping/M.1496420533.A.B5F.html')

    # 針對 根據日期搜尋要爬取的第一頁 做測試
    # print(crawler.find_first_page('Gossiping', '6/08'))

    # 原作者用法
    # crawler.crawl(board='Gossiping', start=22500, end=22501)
