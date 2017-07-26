# -*- coding: UTF-8 -*-

from PttGossipingCrawler import PttCrawler
from AppleCrawler import AppleCrawler
from LtnCrawler import LtnCrawler
from ChinatimesCrawler import ChinatimesCrawler, ChinaElectronicsNewsCrawler
from ChinatimesCrawler import BusinessTimesCrawlwer, DogNewsCrawler

import threading


if __name__ == '__main__':
    # ------ parameters -------
    # 未來改用 config
    start_date = '20160101'
    end_date = '20170720'
    sleep_time = 0.75
    send = True

    # ------ news ------
    ptt = False
    apple = True
    ltn = True
    ct = True

    # ------ ptt ------
    # ptt = True
    # apple = False
    # ltn = False

    # 之後要改thread 同時處理 QQ

    if ptt:
        # PTT 尚有文章失去標頭(header)時的狀況須處理:
        # art = crawler.parse_article('https://www.ptt.cc/bbs/Gossiping/M.1496931521.A.C3E.html')
        # 未加入模組化行列
        ptt_crawler = PttCrawler()
        # ptt_crawler.crawl_by_date(board='Gossiping',send=True)
        ptt_crawler.crawl_by_date(board='Gossiping', date_path='20170616', send=True)

    if apple:
        apple_crawler = AppleCrawler()
        # apple_crawler.crawl_by_date(start_date, end_date, sleep_time, send)
        # apple_crawler.parse_article('蘋果花焦點', 'http://www.appledaily.com.tw/appledaily/article/supplement/20160620/37275837/%E7%86%B1%E7%88%86%E4%BA%86%E9%81%BF%E6%9A%91%E5%8B%9D%E5%9C%B0%E8%B6%85%E6%AE%BA%E6%88%BF%E5%83%B9%E5%90%B8%E5%AE%A2')
        ap = threading.Thread(target=apple_crawler.crawl_by_date, args=(start_date, end_date, send))
        ap.start()

    if ltn:
        # LTN 尚有次頁問題 同種類多出來的頁面暫無處理
        ltn_crawler = LtnCrawler()
        # ltn_crawler.crawl_by_date(start_date, end_date, sleep_time, send)
        lt = threading.Thread(target=ltn_crawler.crawl_by_date, args=(start_date, end_date, send))
        lt.start()

    if ct:
        china_times = ChinatimesCrawler()
        business_times = BusinessTimesCrawlwer()
        dog = DogNewsCrawler()
        china_electronics = ChinaElectronicsNewsCrawler()

        ct = threading.Thread(target=china_times.crawl_by_date, args=(start_date, end_date, send))
        ct.start()

        bt = threading.Thread(target=business_times.crawl_by_date, args=(start_date, end_date, send))
        bt.start()

        dog = threading.Thread(target=dog.crawl_by_date, args=(start_date, end_date, send))
        dog.start()

        ce = threading.Thread(target=china_electronics.crawl_by_date, args=(start_date, end_date, send))
        ce.start()
