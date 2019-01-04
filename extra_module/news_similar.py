import re
import os
import time
import json
import numpy as np
import jieba
import jieba.analyse
from Common import cal_days

path_root = '/data2/Dslab_News/'
path_roots = [os.path.join(path_root, 'AppleDaily'),
              os.path.join(path_root, 'BusinessTimes'),
              os.path.join(path_root, 'LTN'),
              os.path.join(path_root, 'ChinaElectronicsNews'),
              os.path.join(path_root, 'Chinatimes'),
              os.path.join(path_root, 'DogNews')]
jieba.enable_parallel(8)


def load_title_key_pair(path):
    # '/home/c11tch/workspace/PycharmProjects/NewsCrawlers/extra_module/tmp/20180606 title keyword.json'
    with open(path, 'r') as f:
        title_keyword = json.load(f)
    return title_keyword


def cal_correlation(focus):
    # focus = [i for i in title_keyword if '焦點' in i['Category'] and '體育' not in i['Category'] and '財經' not in i['Category'] and '副刊' not in i['Category']]
    # focus = [i for i in title_keyword if '焦點' in i['Category'] or '頭條' in i['Category']]
    # focus = title_keyword
    title_2_tid = dict()
    tid_2_title = dict()
    t_id = 0

    key_2_kid = dict()
    kid_2_key = dict()
    k_id = 0

    tid_2_path = dict()

    for article in focus:
        if article['Title'] not in title_2_tid:
            title_2_tid[article['Title']] = t_id
            tid_2_title[t_id] = article['Title']
            # For find original raw data
            tid_2_path[t_id] = article['Path']

            t_id += 1
        for k in article['Keywords']:
            if k not in key_2_kid:
                key_2_kid[k] = k_id
                kid_2_key[k_id] = k
                k_id += 1

    graph = np.zeros(shape=(len(title_2_tid), len(key_2_kid)))

    for article in focus:
        tid = title_2_tid[article['Title']]
        for k in article['Keywords']:
            kid = key_2_kid[k]
            # graph[tid][kid] += 1
            graph[tid][kid] = 1


    result = np.dot(np.array(graph), np.array(graph).T)
    # correlation = result > 2

    threshold = 4
    correlation_art = list()
    processed_id = set()
    # Print similar articles
    # for i, correlation in enumerate(result):
    #     # Just one similar article means nothing is similar to this article.
    #     if np.sum(correlation > threshold) == 1:
    #         continue
    #     for j, value in enumerate(correlation):
    #         if value > threshold:
    #             print(tid_2_title[j])
    #     print('-' * 30)
    #     # correlation_art.append([])
    #
    # print()

    output_list = list()
    for i, correlation in enumerate(result):
        if np.sum(correlation > threshold) == 1:
            continue

        with open(tid_2_path[i], 'r', encoding='utf-8') as f:
            content_target = json.load(f)['Content']
        divided_article_target = re.split("\.|。|\n|，|（|）|\r|；|？", str(content_target))

        for t_id in range(i, len(correlation)):
            if 10 > correlation[t_id] > threshold:
                common_keyword = [kid_2_key[kid] for kid, bol in enumerate(np.logical_and(np.array(graph[i]),
                                                                                          np.array(graph[t_id]))) if bol]
                print('===\ntitle-1:{0}'.format(tid_2_title[i]))
                print('-\ntitle-2:{0}'.format(tid_2_title[t_id]))
                output_list.append([tid_2_title[i], tid_2_title[t_id]])


                # Find sentence by keywords
                # -------------------------------------------------------------------------------
                # with open(tid_2_path[t_id], 'r', encoding='utf-8') as f:
                #     content = json.load(f)['Content']
                # divided_article = re.split("\.|。|\n|，|（|）|\r|；|？", str(content))
                # for keyword in common_keyword:
                #     print('-\nkeyword:{0}'.format(keyword))
                #     for sent in divided_article_target:
                #         if keyword in sent:
                #             print('match-1:{0}'.format(sent))
                #     for sent in divided_article:
                #         if keyword in sent:
                #             print('match-2:{0}
                # ------------------------- End: Find sentence by keywords ----------------------
        print('=' * 30)
        # input('>>>')

    return output_list


def get_all_file_list(info_data, info):
    list_2_save = list()
    selected = ['政治', '娛樂', '國際', '體育', '財經', '地方', '影視', '社會']

    for fp in info:
        print(fp)
        if os.path.isfile(fp):
            with open(fp, 'r', encoding='utf-8') as f:
                daily_news = json.load(f)

                count = 0
                for _, news_info in daily_news.items():

                    # ############### Start: For testing only selected categories of news ##############
                    check = False
                    for s in selected:
                        if s in news_info['Category'] or s in news_info['BigCategory']:
                            check = True
                    if not check:
                        continue
                    # ############### End: For testing only selected categories of news ##############

                    # 7XX篇,這簡直太靠北了
                    # if news_info['Category'] == '自由共和國' or news_info['Category'] == '人物速寫' or news_info['Category'] == '文化週報':
                    #     continue
                    count += 1
                    if count % 10 == 0:
                        print('processed {0} news'.format(count))
                    title_keyword = dict()
                    title_keyword['Title'] = news_info['Title']
                    title_keyword['Category'] = news_info['Category']
                    real_path = os.path.join(os.path.split(fp)[0], news_info['BigCategory'], news_info['Category'],
                                             '{0}_{1}.json'.format(info_data, news_info['Title']))
                    title_keyword['Path'] = real_path
                    try:
                        with open(real_path, 'r', encoding='utf-8') as jf:
                            news = json.load(jf)
                    except Exception as e:
                        print(real_path)
                        print(e)
                        continue
                    # st = time.time()
                    title_keyword['Keywords'] = jieba.analyse.textrank(news['Title'] + '\n' + news['Content'],
                                                                       topK=10, withWeight=False)
                    # print('each text rank: {0}s'.format(time.time() - st))
                    list_2_save.append(title_keyword)
    return list_2_save


def save_json(obj, prename):
    with open(os.path.join('tmp', '{0}_title_keyword.json').format(prename), 'w') as f:
        json.dump(obj, f, indent=4, ensure_ascii=False)


def find_similar_by_date(target_date):
    # Find keywords (too slow)
    meta_info = [os.path.join(p, target_date, target_date + '.json') for p in path_roots]  # make full path to news
    all_news_path = get_all_file_list(target_date, meta_info)
    # Save to tmp folder
    save_json(all_news_path, prename=target_date + '_4categories')

    # Calculate correlation title by same date
    # cal_correlation(focus=all_news_path)


if __name__ == '__main__':
    # -------------
    # Calculate title keywords pair by same date
    # -------------------------------------------
    # dates = cal_days('20160101', '20181231')
    dates = cal_days('20160101', '20181231')
    for date in dates:
       st = time.time()
       find_similar_by_date(date)
       print('total process time:{0}s'.format(time.time() - st))

    # -------------
    # Calculate correlation title by same date
    # -------------------------------------------
    # pr = '/home/c11tch/workspace/PycharmProjects/NewsCrawlers/extra_module/tmp/'
    # test_p = os.listdir(pr)
    # all_pairs = list()
    # for p in test_p:
    #     all_pairs.extend(cal_correlation(focus=load_title_key_pair(os.path.join(pr, p))))
    # with open('unlabeled_pairs.csv', 'w') as wf:
    #     wf.write('id,query,doc\n')
    #     for i, pair in enumerate(all_pairs):
    #         wf.write('{0},{1},{2}\n'.format(i, pair[0], pair[1]))


