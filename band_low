import pandas as pd
from bs4 import BeautifulSoup
import requests as re
from pandas import DataFrame
from pandas import ExcelWriter
import traceback
import time
import random
import math
import json
import datetime
import numpy as np
def get_fnguide_table(code):
    
    try: 
 
        ''' 경로 탐색'''
        url = re.get('http://comp.fnguide.com/SVO2/ASP/SVD_main.asp?pGB=1&gicode=A%s'%(code))
        url = url.content
 
        html = BeautifulSoup(url,'html.parser')
        body = html.find('body')
 
        fn_body = body.find('div',{'class':'fng_body asp_body'})
        ur_table = fn_body.find('div',{'id':'div15'})
        table = ur_table.find('div',{'id':'highlight_D_Y'})
 
        tbody = table.find('tbody')
        tr = tbody.find_all('tr')
        Table = DataFrame()
 
        for i in tr:
 
            ''' 항목 가져오기'''
            category = i.find('span',{'class':'txt_acd'})
 
            if category == None:
                category = i.find('th')   
 
            category = category.text.strip()
 
 
            '''값 가져오기'''
            value_list =[]
 
            j = i.find_all('td',{'class':'r'})
 
            for value in j:
                temp = value.text.replace(',','').strip()
 
                try:
                    temp = float(temp)
                    value_list.append(temp)
                except:
                    value_list.append(0)
 
            Table['%s'%(category)] = value_list
 
            ''' 기간 가져오기 '''    
 
            thead = table.find('thead')
            tr_2 = thead.find('tr',{'class':'td_gapcolor2'}).find_all('th')
            year_list = []
 
            for i in tr_2:
                try:
                    temp_year = i.find('span',{'class':'txt_acd'}).text
                except:
                    temp_year = i.text
                year_list.append(temp_year)
            Table.index = year_list
 
        #Table = Table.T
        Table.reset_index(level=0, inplace=True)
        Table = Table.rename(columns={'index': 'year'}) 
        Table['code'] = code
        
            #Table = Table.append(Table)
        Table = Table.loc[Table.year.isin(['2015/12', '2016/12', '2017/12', '2018/12', '2019/12','2020/12','2021/12','2022/12(E)','2023/12(E)'])]
        return Table
    
    except: 
        print('error detection!')

def get_fnguide_table2(code):
    
    try: 
 
        ''' 경로 탐색'''
        url = re.get('http://comp.fnguide.com/SVO2/ASP/SVD_main.asp?pGB=1&gicode=A%s'%(code))
        url = url.content
 
        html = BeautifulSoup(url,'html.parser')
        body = html.find('body')
 
        fn_body = body.find('div',{'class':'fng_body asp_body'})
        ur_table = fn_body.find('div',{'id':'div15'})
        table = ur_table.find('div',{'id':'highlight_B_Y'})
 
        tbody = table.find('tbody')
        tr = tbody.find_all('tr')
        Table = DataFrame()
 
        for i in tr:
 
            ''' 항목 가져오기'''
            category = i.find('span',{'class':'txt_acd'})
 
            if category == None:
                category = i.find('th')   
 
            category = category.text.strip()
 
 
            '''값 가져오기'''
            value_list =[]
 
            j = i.find_all('td',{'class':'r'})
 
            for value in j:
                temp = value.text.replace(',','').strip()
 
                try:
                    temp = float(temp)
                    value_list.append(temp)
                except:
                    value_list.append(0)
 
            Table['%s'%(category)] = value_list
 
            ''' 기간 가져오기 '''    
 
            thead = table.find('thead')
            tr_2 = thead.find('tr',{'class':'td_gapcolor2'}).find_all('th')
            year_list = []
 
            for i in tr_2:
                try:
                    temp_year = i.find('span',{'class':'txt_acd'}).text
                except:
                    temp_year = i.text
                year_list.append(temp_year)
            Table.index = year_list
 
        #Table = Table.T
        Table.reset_index(level=0, inplace=True)
        Table = Table.rename(columns={'index': 'year'}) 
        Table['code'] = code
        
            #Table = Table.append(Table)
        Table = Table.loc[Table.year.isin(['2015/12', '2016/12', '2017/12', '2018/12', '2019/12','2020/12','2021/12','2022/12(E)','2023/12(E)'])]
        return Table
    
    except: 
        print('error detection!')
        
def get_price(code, n):
    # DATA를 불러오는 부분 입니다.
    url = 'http://finance.daum.net/api/charts/A%s/days?limit=%d&adjusted=true'%(code, n)
    headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Cookie': 'GS_font_Name_no=0; GS_font_size=16; _ga=GA1.3.937989519.1493034297; webid=bb619e03ecbf4672b8d38a3fcedc3f8c; _ga=GA1.2.937989519.1493034297; _gid=GA1.2.215330840.1541556419; KAKAO_STOCK_RECENT=[%22A069500%22]; recentMenus=[{%22destination%22:%22chart%22%2C%22title%22:%22%EC%B0%A8%ED%8A%B8%22}%2C{%22destination%22:%22current%22%2C%22title%22:%22%ED%98%84%EC%9E%AC%EA%B0%80%22}]; TIARA=C-Tax5zAJ3L1CwQFDxYNxe-9yt4xuvAcw3IjfDg6hlCbJ_KXLZZhwEPhrMuSc5Rv1oty5obaYZzBQS5Du9ne5x7XZds-vHVF; webid_sync=1541565778037; _gat_gtag_UA_128578811_1=1; _dfs=VFlXMkVwUGJENlVvc1B3V2NaV1pFdHhpNTVZdnRZTWFZQWZwTzBPYWRxMFNVL3VrODRLY1VlbXI0dHhBZlJzcE03SS9Vblh0U2p2L2V2b3hQbU5mNlE9PS0tcGI2aXQrZ21qY0hFbzJ0S1hkaEhrZz09--6eba3111e6ac36d893bbc58439d2a3e0304c7cf3',
                'Host': 'finance.daum.net',
                'If-None-Match': 'W/"23501689faaaf24452ece4a039a904fd"',
                'Referer': 'http://finance.daum.net/quotes/A069500',
                'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
                }
    headers['Referer'] = 'http://finance.daum.net/quotes/A%s'%code
    r = re.get(url, headers = headers)
    
    # DATA를 보기 좋게 편집하는 부분 입니다.
    data = json.loads(r.text)
    df = pd.DataFrame(data['data'])
    df.index = pd.to_datetime(df['candleTime'])
    return df
    
    
yr_f1="2022/12(P)"
yr0="2019/12"
yr1="2021/12"

 
code_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]
 
code_df.종목코드 = code_df.종목코드.map("{:06d}".format)
code_df = code_df[['종목코드', '회사명', '업종', '주요제품','상장일']]
code_df = code_df.rename(columns={'종목코드': 'code', '회사명':'name', '업종' : 'industry', '주요제품' : 'main_product','상장일':'ipo_date'})


code_list = code_df['code'].values.tolist()
df = pd.DataFrame(columns = [])
 
for i in code_list:
    try:
        df = df.append(get_fnguide_table(i))
        print(i+" 크롤링 성공!")
    except:
        df = df.append(get_fnguide_table(i))
        print(i+" 크롤링 실패@@@@@")

financial_db=df.reset_index(drop=False)
financial_db=financial_db.drop(["지배주주순이익","비지배주주순이익","지배주주지분","비지배주주지분"], axis=1)

financial_db["매출액_2yrago"] = financial_db["매출액"].shift(2)
financial_db["매출액_1yrago"] = financial_db["매출액"].shift(1)
financial_db["매출액_c1"] = financial_db.apply(lambda x: 1 if math.isnan(x["매출액_2yrago"]) else (1 if x["매출액_1yrago"]/(x["매출액_2yrago"]+1.1)>0.9 else 0), axis=1)
financial_db["매출액_c2"] = financial_db.apply(lambda x: 1 if math.isnan(x["매출액_1yrago"]) else (1 if x["매출액"]/(x["매출액_1yrago"]+1)>0.9 else 0), axis=1)

financial_db["영업이익_2yrago"] = financial_db["영업이익"].shift(2)
financial_db["영업이익_1yrago"] = financial_db["영업이익"].shift(1)
financial_db["영업이익_c1"] = financial_db.apply(lambda x: 1 if math.isnan(x["영업이익_2yrago"]) else (1 if x["영업이익_1yrago"]/(x["영업이익_2yrago"]+1.1)>0.9 else 0), axis=1)
financial_db["영업이익_c2"] = financial_db.apply(lambda x: 1 if math.isnan(x["영업이익_1yrago"]) else (1 if x["영업이익"]/(x["영업이익_1yrago"]+1.1)>0.9 else 0), axis=1)

financial_db["PER_1yrlater"] = financial_db["PER"].shift(-1)
financial_db["PER_2yrlater"] = financial_db["PER"].shift(-2)
financial_db["PER_c1"] = financial_db.apply(lambda x: 0 if math.isnan(x["PER_1yrlater"]) else (1 if x["PER"] > x["PER_1yrlater"] else 0), axis=1)
financial_db["PER_c2"] = financial_db.apply(lambda x: 0 if math.isnan(x["PER_2yrlater"]) else (1 if x["PER_1yrlater"] > x["PER_2yrlater"] else 0), axis=1)

financial_db["평균주가"]=financial_db["BPS"]*financial_db["PBR"]
financial_db["PSR"]=financial_db["평균주가"]*financial_db["발행주식수"]*1000/(financial_db["매출액"]*100000000)
financial_db["발행주식수(e)"] = financial_db["발행주식수"].shift(1)
financial_db["SPS"] = (financial_db["매출액"]*100000000)/(financial_db["발행주식수(e)"]*1000).round(1)

df1=financial_db[financial_db["year"] == yr_f1].reset_index(drop=True)

code_list1 = df1['code'].values.tolist()

price_t = pd.DataFrame(columns = [])



for i in code_list1:
    try:
        stock_ds = get_price(i ,20)
        rsi=stock_ds[["symbolCode","date","tradePrice"]]

        rsi['변화량'] = rsi['tradePrice'] - rsi['tradePrice'].shift(1)
        rsi['상승폭'] = np.where(rsi['변화량']>=0, rsi['변화량'], 0)
        rsi['하락폭'] = np.where(rsi['변화량'] <0, rsi['변화량'].abs(), 0)
        rsi['AU'] = rsi['상승폭'].ewm(alpha=1/14, min_periods=14).mean()
        rsi['AD'] = rsi['하락폭'].ewm(alpha=1/14, min_periods=14).mean()
        rsi['RSI'] = rsi['AU'] / (rsi['AU'] + rsi['AD']) * 100
        rsi=rsi.iloc[19:]
        rsi=rsi[["symbolCode","tradePrice","RSI"]]
        price_t = price_t.append(rsi)       
        print("성공"+i)
    except:
        print("실패"+i)
    
price1=price_t[["symbolCode","tradePrice","RSI"]].reset_index(drop=True)
code_df["symbolCode"]="A"+code_df.code.map(str)
code_df1=pd.merge(code_df,price1,on="symbolCode")
code_df1

#미래 적정 주가 구하기
df1=financial_db[financial_db["year"] == yr_f1].reset_index(drop=True)
df1=df1[df1["EPS"]>0].reset_index(drop=True)
df1["symbolCode"]="A"+df1.code.map(str)
code_df["symbolCode"]="A"+code_df.code.map(str)

#최근종가
pricef = code_df1.rename(columns={'tradePrice': '종가_f'}).reset_index(drop=True)

f1=financial_db.loc[(financial_db["year"]==yr_f1)][['code','EPS','BPS','SPS','ROE']].reset_index(drop=True)
f1.columns=["code","EPS_f1",'BPS_f1','SPS_f1','ROE_f1']
f1=f1[f1["EPS_f1"]>0].reset_index(drop=True)

#3년치 평균 per,pbr
yr3_avg=financial_db.loc[(financial_db["year"]>=yr0) & (financial_db["year"]<=yr1)][['PER','PBR','ROE','PSR','부채비율']].groupby(financial_db['code']).mean()
yr3_avg.columns=["PER_3yr","PBR_3yr","ROE_3yr","PSR_3yr",'debt_3yr']
yr3_avg["PER_3yr"]=yr3_avg["PER_3yr"].round(2)
yr3_avg["PBR_3yr"]=yr3_avg["PBR_3yr"].round(2)
yr3_avg["ROE_3yr"]=yr3_avg["ROE_3yr"].round(2)
yr3_avg["PSR_3yr"]=yr3_avg["PSR_3yr"].round(2)
yr3_avg["debt_3yr"]=yr3_avg["debt_3yr"].round(2)
yr3_avg=yr3_avg.reset_index(drop=False)

#3년치 성장률
growth=financial_db[financial_db["year"] == yr1].reset_index(drop=True)
growth['매출g']=growth['매출액_c1']*growth['매출액_c2']
growth['영익g']=growth['영업이익_c1']*growth['영업이익_c2']
growth['futureEPS']=growth['PER_c1']*growth['PER_c2']
growth=growth[['code','매출액','영업이익',"발행주식수",'매출g','영익g','futureEPS','PER_1yrlater','PER_2yrlater']]

#다합치기


d_today = datetime.date.today()
result_f=pd.merge(f1, yr3_avg, on=['code'])
result_f=pd.merge(result_f, pricef, on=['code'])
result_f=pd.merge(result_f, growth, on=['code'])

result_f["PSR"]=result_f["종가_f"]*result_f["발행주식수"]*1000/(result_f["매출액"]*100000000).round(2)

result_f["PER_EPS_f1"] = round(result_f["PER_3yr"]*result_f["EPS_f1"],1)
result_f["PBR_BPS_f1"] = round(result_f["PBR_3yr"]*result_f["BPS_f1"],1)
result_f["EPS_ROE_f1"] = round(result_f["ROE_f1"]*result_f["EPS_f1"],1)
result_f["EPS_RO3_f1"] = round(result_f["ROE_3yr"]*result_f["EPS_f1"],1)
result_f["SPS_PSR_f1"] = round(result_f["PSR_3yr"]*result_f["SPS_f1"],1)

result_f["PER_EPS%"] = round((result_f["PER_EPS_f1"]-result_f["종가_f"])/result_f["종가_f"],3)*100
result_f["PBR_BPS%"] = round((result_f["PBR_BPS_f1"]-result_f["종가_f"])/result_f["종가_f"],3)*100
result_f["EPS_ROE%"] = round((result_f["EPS_ROE_f1"]-result_f["종가_f"])/result_f["종가_f"],3)*100
result_f["EPS_RO3%"] = round((result_f["EPS_RO3_f1"]-result_f["종가_f"])/result_f["종가_f"],3)*100
result_f["SPS_PSR%"] = round((result_f["SPS_PSR_f1"]-result_f["종가_f"])/result_f["종가_f"],3)*100

result_f["min%"] =result_f[["PER_EPS%","PBR_BPS%","EPS_ROE%"]].min(axis=1)
result_f["max%"] =result_f[["PER_EPS%","PBR_BPS%","EPS_ROE%"]].max(axis=1)
result_f["mean%"] =result_f[["PER_EPS%","PBR_BPS%","EPS_ROE%"]].mean(axis=1).round(1)
result_f["지표"]=result_f.apply(lambda x: "고평가" if x["min%"]<-10 else ("저평가" if x["min%"] >=20 else "적정"), axis=1)

result_f2=result_f[["code","name","industry","ipo_date",'매출액','영업이익',"PER_3yr","PBR_3yr","PSR_3yr","ROE_3yr","ROE_f1","debt_3yr","매출g","영익g",'futureEPS','PER_1yrlater','PER_2yrlater',"종가_f","RSI","PER_EPS_f1","PBR_BPS_f1","EPS_RO3_f1","PER_EPS%","PBR_BPS%","EPS_ROE%","지표"]]
result_f2.columns=["code","NAME","INDUSTRY","상장일",'매출액','영업이익',"PER_3yr","PBR_3yr","PSR_3yr","ROE_3yr","ROE_f1","DEBT_3yr","매출3년간증가","영익3년간증가",'futureEPS','PER_1yrlater','PER_2yrlater',"ClosePrice","RSI","PER_Pri(원)","PBR_Pri(원)","ROE_Pri(원)","PER_Pri(%)","PBR_Pri(%)","ROE_Pri(%)","Evaluation"]

result_f2["RSI"] = result_f2["RSI"].apply(np.floor)


result_f2.to_csv('D:/Stock/temp2/fnguide_1'+str(d_today)+'.csv', sep=',', na_rep='NaN', encoding='utf-8-sig')
