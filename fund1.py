#!/usr/bin/env python
import re
import pickle
import datetime
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor


df = pd.DataFrame()


def main(code_id):
    """
    1.过滤低收益'货币型', '固定收益', '定开债券', 'QDII', '理财型', 'QDII-指数', '分级杠杆'。
    2.基金运行时间大于俩年。
    3.标准差小于4，并且夏普比率大于0.1。
    4.2012年至今，历史年收益都大于-5.0。
    5.季度涨幅和年度涨幅的排名在良好一下的占比。
    :param code_id:
    :return:
    """
    master_page = requests.get("http://fund.eastmoney.com/{code_id}.html".format(code_id=code_id))
    master_page_soup = BeautifulSoup(master_page.content, "lxml")
    master_page_table_root = master_page_soup.find(name="div", attrs={"class": "infoOfFund"}).table
    fund_type = master_page_table_root.find_all(name="td")[0].a.text
    if fund_type in ('货币型', '固定收益', '定开债券', 'QDII', '理财型', 'QDII-指数', '分级杠杆'):
        return

    manager = master_page_table_root.find_all(name="td")[2].a.text
    time_str = re.search(r'\d{4}-\d{2}-\d{2}', master_page_table_root.find_all(name="td")[3].text).group()
    time_list = time_str.split('-')
    create_datetime = datetime.datetime(
        int(time_list[0]),
        int(time_list[1]),
        int(time_list[2]),
    )
    running_days = int((datetime.datetime.now() - create_datetime).days)
    # 700左右很多
    if running_days > 730:
        two_list = feature_data(code_id)
        if not two_list[0] and not two_list[1]:
            feature_manifestation = True
            if not year(code_id):
                year_manifestation = True
                level_tag = master_page_table_root.find_all(name="td")[5].div
                if not level_tag.text:
                    level = level_tag.attrs.get('class')[0]
                else:
                    level = np.nan
                year_and_quarter = ranking(code_id)
                print({"code_id": code_id,
                       "manager": manager,
                       "running_time": time_str,
                       "feature_manifestation": feature_manifestation,
                       "year_manifestation": year_manifestation,
                       "fund_type": fund_type,
                       "level": level,
                       "year_low_ranking": year_and_quarter.get('yearzf'),
                       "quarter_low_ranking": year_and_quarter.get('quarterzf'),
                       })
                global df
                df = df.append({"code_id": str(code_id),
                                "manager": manager,
                                "running_time": time_str,
                                "feature_manifestation": feature_manifestation,
                                "year_manifestation": year_manifestation,
                                "fund_type": fund_type,
                                "level": level,
                                "year_low_ranking": year_and_quarter.get('yearzf'),
                                "quarter_low_ranking": year_and_quarter.get('quarterzf'),
                                }, ignore_index=True)
            else:
                return
        else:
            return
    else:
        return


def feature_data(code_id):
    r = requests.get("http://fund.eastmoney.com/f10/tsdata_{code_id}.html".format(code_id=code_id))
    soup = BeautifulSoup(r.content, "lxml")
    root = soup.find(name="table", attrs={"class": "fxtb"})
    td_list = root.find_all(name="td")
    half_index = int(len(td_list) / 2)
    std = [i for i in td_list[1:half_index] if float(i.text.replace("%", "")) > 4]
    sharp = [i for i in td_list[half_index + 1:] if float(i.text) < 0.1]
    return std, sharp


def year(code_id):
    r = requests.get(
        "http://fund.eastmoney.com/f10/FundArchivesDatas.aspx?type=jdndzf&code={code_id}&rt=0.3754252448332469".format(
            code_id=code_id))
    soup = BeautifulSoup(r.text.split(':"')[1], "lxml")
    root = soup.find(name="table", attrs={"class": "w782 comm jndxq"})
    l = [i.text for i in root.find_all(name="td")[13::8]]
    if len(l) > 6:
        return [i for i in l[:6] if float(i.replace("%", "")) < -5.0]
    else:
        return [i for i in l if float(i.replace("%", "")) < -5.0]


def ranking(code_id):
    temp = {}
    for _type in ('yearzf', 'quarterzf'):
        r = requests.get(
            "http://fund.eastmoney.com/f10/FundArchivesDatas.aspx?type={_type}&code={code_id}&rt=0.05693883405304212".format(
                code_id=code_id, _type=_type))
        soup = BeautifulSoup(r.text.split(':"')[1], "lxml")
        root = soup.find(name="table", attrs={"class": "w782 comm jndxq"})
        all_ranking_list = root.find_all(name="tr")[5].find_all(name="p", attrs={"class": "sifen"})
        low_ranking_list = [i for i in all_ranking_list if i.text not in ["优秀", "良好"]]
        temp[_type] = (len(low_ranking_list) / len(all_ranking_list))
    return temp


pool = ThreadPoolExecutor(5)
all_funds_list = pickle.load(open("all_funds_list.pkl", "rb"))
for code_id in all_funds_list:
    print('开始请求', code_id)
    # 去连接池中获取链接
    pool.submit(main, code_id)

pool.shutdown(wait=True)
print(df)

df.to_csv('Well_behaved_fund.csv', sep=',', encoding='utf-8', index=False)
# 过滤货币型
