# -*- coding: UTF-8 -*-

import requests

import time
import json
import os

from LinkKafka import send_json_kafka
from Common import cal_days, check_folder, check_meta

import logging
import logging.config


class Crawler(object):
    news_name = 'Crawler'
    carwler_name = 'Crawler'

    domain = 'http://www.appledaily.com.tw'
    root = domain + '/appledaily/archive/'

    # File path. Will be removed in later version and using config file to instead of it.
    file_root = '/data1/Dslab_News/'
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
        log_path = os.path.join(self.log_root, self.news_name)
        check_folder(log_path)
        log_name = time.strftime('%Y%m%d%H%M') + '_' + self.news_name + '.log'
        file_hdlr = logging.FileHandler(os.path.join(log_path, log_name))
        file_hdlr.setLevel(logging.DEBUG)

        # Command line 看不到 DEBUG
        console_hdlr = logging.StreamHandler()
        console_hdlr.setLevel(logging.INFO)

        formatter = logging.Formatter('%(levelname)-8s - %(asctime)s - %(name)-12s - %(message)s')
        file_hdlr.setFormatter(formatter)
        console_hdlr.setFormatter(formatter)

        self.log.addHandler(file_hdlr)
        self.log.addHandler(console_hdlr)

    def pages(self, index_range=None):
        target_page = self.root

        if index_range is None:
            yield target_page, index_range
        else:
            for index in index_range:
                yield target_page + index, index

    def save_article(self, filename, data, meta_old, meta_path, send):
        # 依照給予的檔名儲存單篇文章
        try:
            # check folder
            file_path = os.path.join(self.file_root, self.news_name, data['Date'][0:8],
                                     data['BigCategory'], data['Category'])
            check_folder(file_path)

            with open(os.path.join(file_path, filename + '.json'), 'w') as op:
                json.dump(data, op, indent=4, ensure_ascii=False)

            # 存檔完沒掛掉就傳到 kafka
            if send:
                send_json_kafka(json.dumps(data))

            # 都沒掛掉就存回 meta date
            meta_old.update({
                data['URL']: {'Title': data['Title'],
                              'Category': data['Category'],
                              'BigCategory': data['BigCategory']}
            })

            with open(meta_path, 'w') as wf:
                json.dump(meta_old, wf, indent=4, ensure_ascii=False)
            self.log.info('已完成爬取 %s > %s > %s' % (data.get('BigCategory'), data.get('Category'), data.get('Title')))

        except Exception as e:
            self.log.exception(e)
            self.log.error(u'在 Check Folder or Save File 時出現錯誤\nfilename:{0}'.format(filename))
