import pandas as pd
from pandas import Series, DataFrame
from elasticsearch import Elasticsearch
import elasticsearch
from elasticsearch.helpers import bulk
import certifi
import matplotlib.pyplot as plt

rubbish2 = ['営', 'Unnamed: 1', '業', '区', 'Unnamed: 4', '域', 'Unnamed: 6']
rubbish3 = ['営', '業  区  域']
rubbish4 = ['営', 'Unnamed: 1', '業', '区', 'Unnamed: 4', '域', 'Unnamed: 6']
rubbish5 = ['営  業  区  域', 'Unnamed: 1', 'Unnamed: 2']

personalName = {'人    口\n（ 千  人 ）':'人口', '個人タクシー車  両  数\n（１人 1 車制）':'个人出租车数',
        '法人タクシー車  両  数':'法人出租车数','合    計':'合计',
        '個     人\n（１人 1 車制）\n比率 （％）':'个人出租车比例',
        '個     人\n（１人 1 車制）\n比率（％）':'个人出租车比例'}
parts = {'Table 2':[rubbish2, ['北海道', '札幌交通圏', '小樽市', '函館交通圏', '旭川交通圏', '室蘭市', '苫小牧交通圏',
                    '釧路交通圏', '帯広交通圏', '北見交通圏', '东北', '盛岡交通圏', '青森交通圏', '八戸交通圏',
                    '仙台市', '福島交通圏', '郡山交通圏', '山形交通圏', '秋田交通圏', '北陆信越', '新潟交通圏',
                    '長野交通圏', '松本交通圏', '金沢交通圏', '富山交通圏']],
         'Table 3':[rubbish3, ['关东', '特別区武三交通圏', '北多摩交通圏', '南多摩交通圏', '京浜交通圏', '県央交通圏',
                    '千葉交通圏', '京葉交通圏', '東葛交通圏', '県南中央交通圏', '県南西部交通圏', '県南東部交通圏',
                    '中・西毛交通圏', '宇都宮交通圏', '中部', '名古屋交通圏', '東三河南部交通圏', '静清交通圏',
                    '浜松交通圏', '沼津・三島交通圏', '岐阜交通圏', '伊勢・志摩交通圏', '福井交通圏']],
         'Table 4':[rubbish4, ['近畿', '大阪市域交通圏', '北摂交通圏', '京都市域交通圏', '神戸市域交通圏',
                    '姫路・西播磨交通圏', '大津市域交通圏', '奈良市域交通圏', '和歌山市域交通圏','中国',
                    '広島交通圏', '呉市A', '福山交通圏', '岡山市', '倉敷交通圏', '宇部市', '岩国交通圏',
                    '下関市', '周南市', '四国', '高松交通圏', '徳島交通圏', '松山交通圏', '高知交通圏']],
         'Table 5':[rubbish5, ['九州', '福岡交通圏', '北九州交通圏', '久留米市', '大牟田市', '佐賀市', '長崎交通圏', '佐世保市',
                    '熊本交通圏', '大分市', '別府市', '宮崎交通圏', '鹿児島市', '冲绳', '沖縄本島', '合计']]}

driverName = {'営  業  区  域':'区域', '平成22年':'2010', '23年':'2011', '24年':'2012',
              '25年':'2013', '26年':'2014', '27年':'2015'}

def readPersonalTexiNumsTable(table, area):
    data = pd.read_excel("./unformatedData/個人タクシー許可地域の車両数.xlsx", table)
    data = data.drop(area[0], axis=1)
    data['区域'] = Series(area[1], index=data.index)
    data = data.rename(index=str, columns=personalName)
    return data

def readPersonalTexiNums():
    datas = []
    for table, area in parts.items():
        datas.append(readPersonalTexiNumsTable(table, area))
    return pd.concat(datas, ignore_index=True)

def uploadToElasticsearch():
    es = Elasticsearch(['localhost:9200'],
                       use_ssl=True,
                       verify_certs=True,
                       ca_certs=certifi.where(),
                       http_auth=("elastic", "changeme"),
                       scheme="https",
                       port=9200)
    personalTexiNums = readPersonalTexiNums()
    actions = []
    for row in personalTexiNums.values:
        action = {
            '_op_type': 'index',
            '_index': "personal_texi_nums",
            '_type': "texi",
            '_id': row[5],
            '_source': {"popularity": row[0],
                        "personalTexi":row[1],
                        "companyTexi":row[2],
                        "area":row[5]
            }
        }
        actions.append(action)

    res = elasticsearch.helpers.bulk(es, actions)
    print(res)

def savePersonalTexiNumsToCSV():
    personalTexiNums = readPersonalTexiNums()
    personalTexiNums.to_csv('./data/PersonalTexiNums.csv')

def readPersonalDriverNums():
    data = pd.read_excel("./unformatedData/個人タクシー事業者数の推移.xlsx")
    data = data.drop(["Unnamed: 5"], axis=1)
    data = data.rename(index=str, columns=driverName)
    # data["区域"] = data["区域"].str.strip()
    return data
    # print(data)

def savereadPersonalDriverNumsToCSV():
    personalTexiDriverNums = readPersonalDriverNums()
    personalTexiDriverNums.to_csv('./data/PersonalTexiDriverNums.csv')

if __name__ == '__main__':
    savereadPersonalDriverNumsToCSV()
