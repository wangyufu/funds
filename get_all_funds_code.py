#!/usr/bin/env python
import re
from bs4 import BeautifulSoup
import pickle

"""
查找funds_code.html所有基金代码
"""
rf = open("funds_code.html")
text_html = rf.read()
soup = BeautifulSoup(text_html, "lxml")
root = soup.find(name="div", attrs={"id": "code_content"})


code = []
for i in root.find_all(name="ul"):
    temp = []
    for j in i.find_all(name="li"):
        try:
            temp.append(re.search(r'\d+', j.div.a.text).group())
        except Exception as e:
            print(e)
    code.extend(temp)
print(len(code))
pickle.dump(code, open('all_funds_list.pkl', 'wb'))