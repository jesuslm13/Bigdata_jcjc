# -*- coding:utf-8 -*-

import urllib.request as req
import pandas as pd
import csv
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.tree import export_graphviz
from sklearn.tree import DecisionTreeClassifier
import pydotplus

pol = pd.read_csv('pol_sample.csv')
df = pd.DataFrame(pol)

label=[]
data=[]

for row_index, row in pol.iterrows():
     label.append(row.loc[0])
     row_data=[]
     data.append(row_data)
     
#x=df.pol_name
#y=df.text_num+df.pol_res

data_train, data_test, label_train, label_test = train_test_split(data,label)

tree_clf = DecisionTreeClassifier(max_depth=3)
tree_clf.fit(data_train,label_train)
