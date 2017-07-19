# -*- coding: UTF-8 -*-

import requests

import time

import logging
import logging.config


class Crawler(object):
    news_name = 'Crawler'
    carwler_name = 'Crawler'

    domain = 'http://www.appledaily.com.tw'
    root = domain + '/appledaily/archive/'

    # File path. Will be removed in later version and using config file to instead of it.
    file_root = '/data1/'
    log_root = 'log'

    moon_trans = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                  'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                  'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

    def __init__(self):
        self.session = requests.session()
        requests.packages.urllib3.disable_warnings()
        self.log = logging.getLogger(self.carwler_name)
        self.set_log_conf()

    def set_log_conf(self):
        # 設定log
        self.log.setLevel(logging.DEBUG)

        # Log file 看得到 DEBUG
        file_hdlr = logging.FileHandler(self.log_root + '/' + time.strftime('%Y%m%d%H%M') + '_' + self.news_name + '.log')
        file_hdlr.setLevel(logging.DEBUG)

        # Command line 看不到 DEBUG
        console_hdlr = logging.StreamHandler()
        console_hdlr.setLevel(logging.INFO)

        formatter = logging.Formatter('%(levelname)-8s - %(asctime)s - %(name)-12s - %(message)s')
        file_hdlr.setFormatter(formatter)
        console_hdlr.setFormatter(formatter)

        self.log.addHandler(file_hdlr)
        self.log.addHandler(console_hdlr)

