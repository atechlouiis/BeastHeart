rom ebest import EBest
import xingapi as xa
import requests
import pandas as pd
from pandas import DataFrame as df
import telegram
import dart_fss as dart
from datetime import datetime

telgm_token = ''

bot = telegram.Bot(token = telgm_token)

try:
    # Open DART API KEY 설정
    api_key=''
    dart.set_api_key(api_key=api_key)
    day = (datetime.today()).strftime("%Y%m%d")
    reports = dart.filings.search(bgn_de=day, pblntf_detail_ty="B001", page_no=100, page_count=100).to_dict()
    rlist = pd.DataFrame(reports["report_list"])
    rlist = rlist[rlist["report_nm"].str.contains("무상증자")].drop(["rcp_no", "corp_code", "corp_code"],axis=1).drop_duplicates()
    rlist = rlist[rlist["report_nm"].str.contains("기재정정") == False].reset_index(drop=True)
    rlist = rlist[rlist["corp_cls"].str.contains("N") == False].reset_index(drop=True)

    if len(rlist) ==0:
        print("[다트] 무상증자 공시 없음")
        bot.sendMessage(chat_id='1039199786', text="[다트] 무상증자 공시 없음")
    else:
        for rl in range(0,len(rlist)):
            bot.sendMessage(chat_id='', text="[다트] "+ rlist["corp_name"][rl] +" 무상증자 공시확인")
            print(str(rlist["corp_name"][rl]))

except:
    print("dart 공시오류")


bot.sendMessage(chat_id='', text="[매수] 업무시작")

#시가총액데이터 불러오기
siga_yest = pd.read_csv('')


app = xa.App(id='', pw='', cert='')

query = xa.Query('t1452')
data = query.call(gubun='0',sdiff=-30,ediff=30).next(keypairs={'idx':'idx'})

df1=data['t1452OutBlock1']
df2=df1[(pd.to_numeric(df1["전일비"])>=400) & (pd.to_numeric(df1["전일대비구분"])==2)] #전일대비 상승한거만 , 거래량 400%
df3=df2[(pd.to_numeric(df2["전일거래량"])>100000)] #전일거래량이 10만건 이상
df3=df3[(pd.to_numeric(df3["현재가"])>3000)]      #현재가가 3000원 이상
df3=df3.reset_index(drop=True)


df4 = df(data={'종목코드': [], 'sum': [], 'n': [], 'debt': [], 'open_p': [], 'high_p': [], 'current_p': []})

for m in range(0, len(df3)):
    comp_daily100 = app.종목정보(df3["종목코드"][m]).일봉(100)
    comp_daily100 = comp_daily100.sort_values(by=['날짜'], axis=0)
    comp_daily100['ma5'] = round(comp_daily100['종가'].rolling(window=5).mean(), 0)  # 5일 이평선
    comp_daily100['ma10'] = round(comp_daily100['종가'].rolling(window=10).mean(), 0)  # 10일 이평선
    comp_daily100['ma20'] = round(comp_daily100['종가'].rolling(window=20).mean(), 0)  # 20일 이평선
    # b['ma60'] = round(b['종가'].rolling(window=60).mean(), 0)  60일 이평선
    # b['ma120'] = round(b['종가'].rolling(window=120).mean(), 0)  120일 이평선
    comp_daily100['vol_inc'] = comp_daily100.apply(lambda x: 1 if (pd.to_numeric(x["거래증가율"]) >= 400) else 0,
                                                   axis=1)  # 400프로 이상 거래율 증가
    comp_daily100['n_vol_inc'] = comp_daily100['vol_inc'].cumsum(axis=0)  # 100일 동안 400프로이상 증가일수
    comp_daily100['20_vol_inc'] = round(comp_daily100['vol_inc'].rolling(window=20).sum(), 0)  # 최근 20일 동안 400프로이상 증가
    comp_daily100 = comp_daily100.dropna()
    comp_daily100 = comp_daily100.sort_values(by=['날짜'], axis=0, ascending=False)
    comp_daily100 = comp_daily100.reset_index(drop=True)
    comp_daily100["close"] = pd.to_numeric(comp_daily100["종가"])

    try:
        a = df3["종목코드"][m]
        URL = "https://finance.naver.com/item/main.nhn?code=" + a

        k = requests.get(URL)
        html = k.text

        financial_stmt = pd.read_html(k.text)[3]

        financial_stmt.set_index(('주요재무정보', '주요재무정보', '주요재무정보'), inplace=True)
        financial_stmt.index.rename('index', inplace=True)
        financial_stmt.columns = financial_stmt.columns.droplevel(0)
        financial_stmt.columns = financial_stmt.columns.droplevel(1)
        financial_stmt.columns = ["year3", "year2", "year1", "year0e", "q5", "q4", "q3", "q2", "q1", "q0(E)"]
        debt_ratio = financial_stmt["year1"][6]  # 최근부채비율
    except:
        debt_ratio = 0  # 최근부채비율
        print('에러발생______________________________DEBT' + df3["종목코드"][m])

        # 종목별 정보 취합
    try:
        comp_daily100["ma20_over"] = comp_daily100.apply(lambda x: 1 if (x['ma20'] < x['close']) else 0, axis=1)
        Total = comp_daily100[0:10]["ma20_over"].sum()
        open_p = comp_daily100["시가"][0]
        high_p = comp_daily100["고가"][0]
        current_p = comp_daily100["종가"][0]


    except:
        Total = 0
        open_p = 0
        high_p = 0
        current_p = 0
        print('에러발생______________________________Total' + df3["종목코드"][m])

    try:
        number = comp_daily100["20_vol_inc"][0]

    except:
        number = 0
        print('에러발생______________________________number' + df3["종목코드"][m])

    new_data = df(
        data={'종목코드': [df3["종목코드"][m]], 'sum': [Total], 'n': [number], 'debt': [debt_ratio], 'open_p': [open_p],
              'high_p': [high_p], 'current_p': [current_p]})
    df4 = df4.append(new_data)
    print(df3["종목명"][m])

df5 = pd.merge(df3, df4, how='left', on='종목코드')
buylist = df5[df5["sum"] >= 5] #최근 10영업일동안 5회 이상 20일 이평 위에
buylist = buylist[buylist["n"] == 2] #두번째 400% 발생건
buylist = buylist[pd.to_numeric(buylist["debt"]) < 200] #최근 부채율 200%이내
buylist = buylist[buylist["현재가"] > buylist["open_p"]] #음봉인거 제외
buylist = buylist[pd.to_numeric(buylist["현재가"]) > pd.to_numeric(buylist["high_p"])*0.92] #긴꼬리 제외
buylist = buylist[pd.to_numeric(buylist["현재가"]) > pd.to_numeric(buylist["open_p"])*1.02] #시가대비 상승한애들
print("룰추가")

buylist = buylist.reset_index(drop=True)
siga=siga_yest[["종목코드", "시가총액"]]
buylist = pd.merge(buylist, siga, on='종목코드', how="inner")
buylist = buylist[pd.to_numeric(buylist["시가총액"])>800].reset_index(drop=True) #800억원 이상인애들만

session = xa.Session()
session.login(id='junkim10', pwd='Gold2140', cert=None)

ebest_demo=EBest("DEMO")
ebest_demo.login()

#계좌정보 가져오기"
ac_info = pd.DataFrame(ebest_demo.get_account_info())
ac_info["money"] = pd.to_numeric(ac_info["증거금률100퍼센트주문가능금액"])
print(buylist)
#구매할 종목
try:
    prob_mon = min(round(6000000/len(buylist),0),5000000) #한종목 당 구매 가능 금액 500만원 less

except:
    prob_mon = 1

bot = telegram.Bot(token = telgm_token)
bot.sendMessage(chat_id='', text="[매수] 이제매수할라고...")

for cp in range(0,len(buylist)):
    try:
        c_price= pd.DataFrame(ebest_demo.get_current_market_price_by_code(buylist["종목코드"][cp]))
        c_price["price_r"]=pd.to_numeric(c_price["price"])
        c_price["high_r"] = pd.to_numeric(c_price["high"])
        c_price["open_r"] = pd.to_numeric(c_price["open"])
        try:
            c_amt = round(prob_mon/c_price["price_r"][0],0)
        except:
            c_amt = 0
        print(c_amt)
        print(round(c_price["price_r"][0]/c_price["open_r"][0],2)) # 1보다 같거나 작으면 안산다
        print(round(c_price["price_r"][0]/c_price["high_r"][0],2)) # 0.9보다 작으면 안산다
        #ebest_demo.order_stock(c["종목코드"][cp], c_amt, price=None, bns_type=2, order_type="03")
        if (c_price["price_r"][0] > c_price["high_r"][0]*0.9) and (c_price["open_r"][0] < c_price["price_r"][0]) and (c_amt>0) :
                ebest_demo.order_stock(buylist["종목코드"][cp], c_amt, price=None, bns_type=2, order_type="03")
                print("주문성공_",buylist["종목명"][cp], str(c_price["price_r"][0]), str(c_amt))
                bot.sendMessage(chat_id='',
                                      text="[매수]주문성공 "+buylist["종목명"][cp]+" "+str(c_price["price_r"][0])+" "+str(c_amt))

        else:
            print("주문실패_"+buylist["종목명"][cp]+"_긴꼬리/음봉/ 잔고부족 rule에 걸려서 매수 안함")
            bot.sendMessage(chat_id='',
                                  text="[매수]주문실패 "+buylist["종목명"][cp]+ " 긴꼬리/음봉/잔고부족 rule 작동")
    except:
         print("Error_",buylist["종목명"][cp], str(c_price["price_r"][0]), str(c_amt))
         bot.sendMessage(chat_id='1039199786',
                          text="Error_" + buylist["종목명"][cp] + "_" + str(c_price["price_r"][0]) + "_" + str(c_amt))

bot.sendMessage(chat_id='', text="[매수] 매수 끝! 퇴근 3시간 전!")

#20200909 21:06
