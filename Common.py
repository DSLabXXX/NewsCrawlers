import datetime
import time
import os
import json


def cal_days(begin_date=None, end_date=None, format_in="%Y%m%d", format_out="%Y%m%d"):
    if end_date:
        # 找出start -> end 之間的每一天
        date_list = []
        begin_date = datetime.datetime.strptime(begin_date, format_in)
        end_date = datetime.datetime.strptime(end_date, format_in)
        # 判斷日期先後
        if begin_date > end_date:
            begin_date, end_date = end_date, begin_date

        while begin_date <= end_date:
            date_str = begin_date.strftime(format_out)
            date_list.append(date_str)
            begin_date += datetime.timedelta(days=1)
        return date_list
    else:
        if begin_date:
            return [datetime.datetime.strptime(begin_date, format_in).strftime(format_out)]
        else:
            return [time.strftime(format_out)]


def check_folder(file_path):
    # check folder
    if not os.path.isdir(file_path):
        os.makedirs(file_path)


def check_meta(meta_path):
    art_meta = {}
    if not os.path.isfile(meta_path):
        with open(meta_path, 'w') as wf:
            json.dump(art_meta, wf)
    else:
        with open(meta_path, 'r') as rf:
            art_meta = json.load(rf)
    return art_meta
