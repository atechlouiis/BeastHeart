from ebest import EBest
import xingapi as xa
import requests
import pandas as pd
from pandas import DataFrame as df
import telegram
import time

telgm_token = ''

bot = telegram.Bot(token = telgm_token)

bot.sendMessage(chat_id='', text="[긴꼬리] 업무시작")

cur_time = time.ctime()
print("_______________" + cur_time + "_______________")

#시가총액데이터 불러오기
siga_yest = pd.read_csv('')

app = xa.App(id='', pw='', cert='')

query_lt = xa.Query('t1427')
longtail = query_lt.call(gubun=0, signgubun="1", diff=0, jc_num="000016777728").next(keypairs={'idx':'idx'}) #거래정지 정리매매
lt1=longtail['t1427OutBlock1']
longtail2 = query_lt.call(gubun=0, signgubun="2", diff=0, jc_num="000016777728").next(keypairs={'idx':'idx'})
lt2=longtail2['t1427OutBlock1']
lt=pd.concat([lt1,lt2], axis=0).reset_index(drop=True)
print(lt2)
ltset = lt[(lt['종목코드'].isin(siga_yest['종목코드']))].reset_index(drop=True)
ltset["긴꼬리"] = (pd.to_numeric(ltset["현재가"])/pd.to_numeric(ltset["저가"])-1)*100

#조건
ltset = ltset[(pd.to_numeric(ltset["전일거래량"])>100000)].reset_index(drop=True) #전일거래량이 10만건
#ltset = ltset[(pd.to_numeric(ltset["거래증가율"])>-20)].reset_index(drop=True)  #거래량증가율 -20
ltset = ltset[((pd.to_numeric(ltset["현재가"])/pd.to_numeric(ltset["저가"])-1)*100>15)].reset_index(drop=True)  #긴꼬리 15이상
ltset = ltset[pd.to_numeric(ltset["현재가"]) > 2000].reset_index(drop=True)  #긴꼬리 13이상
ltset = ltset[pd.to_numeric(ltset["시가총액"]) > 1000].reset_index(drop=True)  #시가총액 1000억원 이상
print(ltset)

session = xa.Session()
session.login(id='', pwd='', cert=None)

ebest_demo=EBest("DEMO")
ebest_demo.login()

#계좌정보 가져오기"
ac_info = pd.DataFrame(ebest_demo.get_account_info())
ac_info["money"] = pd.to_numeric(ac_info["증거금률100퍼센트주문가능금액"])
print(ac_info)

#구매할 종목
try:
    prob_mon = min(round(5000000/len(ltset),0),1000000) #한종목 당 구매 가능 금액 50만원 less

except:
    prob_mon = 1

bot = telegram.Bot(token = telgm_token)
bot.sendMessage(chat_id='', text="[매수] 이제매수할라고...")

for cp in range(0,len(ltset)):
    try:
        c_price= pd.DataFrame(ebest_demo.get_current_market_price_by_code(ltset["종목코드"][cp]))
        c_price["price_r"]=pd.to_numeric(c_price["price"])

        try:
            c_amt = round(prob_mon/c_price["price_r"][0],0)
        except:
            c_amt = 0

        print(c_amt)

        if (c_amt>0) :
                ebest_demo.order_stock(ltset["종목코드"][cp], c_amt, price=None, bns_type=2, order_type="03")
                print("주문성공_",ltset["한글명"][cp], str(c_price["price_r"][0]), str(c_amt))
                bot.sendMessage(chat_id='',
                                      text="[매수]주문성공 "+ltset["한글명"][cp]+" "+str(c_price["price_r"][0])+" "+str(c_amt))

        else:
            print("주문실패_"+ltset["한글명"][cp]+"_ 매수 오류")
            bot.sendMessage(chat_id='',
                                  text="[매수]주문실패 "+ltset["한글명"][cp]+ " 오류")
    except:
         print("Error_",ltset["한글명"][cp], str(c_price["price_r"][0]), str(c_amt))
         bot.sendMessage(chat_id='',
                          text="Error_" + ltset["한글명"][cp] + "_" + str(c_price["price_r"][0]) + "_" + str(c_amt))

bot.sendMessage(chat_id='', text="[매수] 매수 끝! 퇴근 3시간 전!")

cur_time = time.ctime()
print("_______________" + cur_time + "_______________")
