import dart_fss as dart
from ebest import EBest
from datetime import datetime
import pandas as pd
import telegram
import time
from pandas import DataFrame as df
import xingapi as xa

#텔레그램 로그인
telgm_token = ''
bot = telegram.Bot(token=telgm_token)
bot.sendMessage(chat_id='', text="[공시] 업무시작")

# Open DART API KEY 설정
api_key = ''
dart.set_api_key(api_key=api_key)

#오늘 날짜 찍기
day = (datetime.today()).strftime("%Y%m%d")
FMT = '%H:%M:%S'

app = xa.App(id='', pw='', cert='')

cur_time = time.ctime()
print("_______________"+cur_time+"_______________")

siga_yest = pd.read_csv('')
siga_yest = siga_yest[pd.to_numeric(siga_yest["시가총액"])>1000].reset_index(drop=True) #1000억원 이상인 애들만
siga_yest = siga_yest[pd.to_numeric(siga_yest["거래량"])>100000].reset_index(drop=True) #거래량 10만건 이상인 애들만
print(siga_yest)
# 매수리스트
buylist_off = df(data={'종목이름': [],'종목코드': [], '매수수량': [], '구매시간':[]})

# 공시리스트 업데이트용
old_list = pd.DataFrame()

#이베스트 로그인
ebest_demo = EBest("DEMO")
ebest_demo.login()

while True:
    cur_time = time.ctime()
    print("_______________" + cur_time + "_______________")
    bot = telegram.Bot(token=telgm_token)

    try:
        reports = dart.filings.search(bgn_de=day, pblntf_detail_ty="I001", page_no=100, page_count=100).to_dict()
        rlist = pd.DataFrame(reports["report_list"])
        rlist = rlist[rlist["report_nm"].str.contains("단일판매")].drop(["rcp_no", "corp_code", "corp_code"],axis=1).drop_duplicates()
        rlist = rlist[rlist["report_nm"].str.contains("기재정정") == False].reset_index(drop=True)

        try:
            alarm = rlist[~(rlist['stock_code'].isin(old_list['stock_code']))].reset_index(drop=True)

            if len(alarm) == 0:
                print("[공시] update된 게 없다")
                if (pd.to_numeric(cur_time.split(' ')[-2].split(":")[1]) % 30 == 0) & (pd.to_numeric(cur_time.split(' ')[-2].split(":")[2]) > 58):  # 대기상태면 30분마다 알려줌
                    bot.sendMessage(chat_id='', text="[공시] 추가 업데이트 없음")
            else:
                for n in range(0, len(alarm)):
                    print("[공시] newly updated "+time.ctime())
                    #c_price = pd.DataFrame(ebest_demo.get_current_market_price_by_code(alarm["stock_code"][n]))
                    #c_amt = round(1000000 / pd.to_numeric(c_price["price"][0]), 0)
                    try:
                        c_price=siga_yest[siga_yest["종목코드"]==alarm["stock_code"][n]].reset_index(drop=True)
                        c_amt = round(1000000 / pd.to_numeric(c_price["현재가"][0]), 0) # 어제 종가로 100만원 나누기
                        ebest_demo.order_stock(alarm["stock_code"][n], c_amt, price=None, bns_type=2,order_type="03")
                        print(time.ctime())
                        new_data = df(data={'종목이름': [alarm["corp_name"][n]], '종목코드': [alarm["stock_code"][n]], '매수수량': [c_amt], '구매시간':[time.strftime(FMT, time.localtime(time.time()))]})
                        buylist_off = buylist_off.append(new_data)
                        bot.sendMessage(chat_id='', text="[공시]" + alarm["corp_name"][n] + " 매수 완료")
                    except:
                        bot.sendMessage(chat_id='', text="[공시]" + alarm["corp_name"][n] + " 리스트 미존재")

        except:
            alarm = rlist
            print(alarm)
            for n in range(0, len(alarm)):
                print("[공시] first updated "+time.ctime())
                try:
                    c_price=siga_yest[siga_yest["종목코드"]==alarm["stock_code"][n]].reset_index(drop=True)
                    c_amt = round(1000000 / pd.to_numeric(c_price["현재가"][0]), 0)  # 어제 종가로 100만원 나누기
                    ebest_demo.order_stock(alarm["stock_code"][n], c_amt, price=None, bns_type=2,order_type="03")
                    print(time.ctime())
                    new_data = df(data={'종목이름': [alarm["corp_name"][n]], '종목코드': [alarm["stock_code"][n]], '매수수량': [c_amt], '구매시간':[time.strftime(FMT, time.localtime(time.time()))]})
                    buylist_off = buylist_off.append(new_data).reset_index(drop=True)
                    bot.sendMessage(chat_id='', text="[공시]" + alarm["corp_name"][n] + " 매수완료")
                    print("[공시]" + alarm["corp_name"][n] + "_매수완료")
                except:
                    print("[공시]" + alarm["corp_name"][n] + "_리스트미존재")
                    bot.sendMessage(chat_id='', text="[공시]" + alarm["corp_name"][n] + " 리스트 미존재")

        old_list = rlist

    except:
        #첫 공시 안뜸
        print("아직 공시 NOTHING")
        if (pd.to_numeric(cur_time.split(' ')[-2].split(":")[1]) % 30 == 0) & (pd.to_numeric(cur_time.split(' ')[-2].split(":")[2]) > 58):  # 대기상태면 15분마다 알려줌
            bot.sendMessage(chat_id='', text="[공시] 아직 NOTHING")

    #시간 다됨
    if (pd.to_numeric(cur_time.split(' ')[-2].split(":")[0]) >= 15) & (pd.to_numeric(cur_time.split(' ')[-2].split(":")[1]) >= 20):
        print("영업종료")
        bot.sendMessage(chat_id='', text="[공시] 영업종료")
        break

    #구매한 애들 매도 코드
    if len(buylist_off) > 0 :
        print("매도코드")
        print(buylist_off)

        try:
            # 잔량 있는 종목들만 가져와서 매도
            ac_left = pd.DataFrame(ebest_demo.ac_left())
            ac_left = ac_left[pd.to_numeric(ac_left["jsat"]) == 0]  # 오늘 산애들만 가져와
            ac_left = ac_left[pd.to_numeric(ac_left["janqty"]) > 0]  # 잔량 있는애들만 가져와
            ac_left = ac_left.reset_index(drop=True)
            print("잔고")

            for b in range(0,len(buylist_off)):
               s_time = time.strftime('%H:%M:%S', time.localtime(time.time()))
               sell = (ac_left[ac_left["expcode"]==buylist_off['종목코드'][b]]).reset_index(drop=True)
               if len(sell) > 0:
                   print("sell list")
                   print(sell)
                   try:
                       if pd.to_numeric(sell["sunikrt"][0]) >= 1.5:
                            ebest_demo.order_stock(sell["expcode"][0], sell["janqty"][0], price=None, bns_type=1, order_type="03")
                            bot.sendMessage(chat_id='1039199786', text="[공시]"+sell["hname"][0]+"_"+sell["sunikrt"][0]+"% 익절완료")
                            buylist_off = buylist_off[~buylist_off["종목코드"].str.contains(buylist_off['종목코드'][b])]
                            print("[공시매도]"+sell["hname"][0]+"_"+sell["sunikrt"][0]+"% 익절완료")
                       elif pd.to_numeric(sell["sunikrt"][0]) < - 4:
                            ebest_demo.order_stock(sell["expcode"][0], sell["janqty"][0], price=None, bns_type=1, order_type="03")
                            bot.sendMessage(chat_id='1039199786', text="[공시]"+sell["hname"][0]+"_"+sell["sunikrt"][0]+"% 손절완료")
                            buylist_off = buylist_off[~buylist_off["종목코드"].str.contains(buylist_off['종목코드'][b])]
                            print("[공시매도]"+sell["hname"][0]+"_"+sell["sunikrt"][0]+"% 손절완료")
                       elif pd.to_numeric((datetime.strptime(s_time, FMT) - datetime.strptime(buylist_off["구매시간"][b], FMT)).seconds) > 600: #10분넘으면 팔기
                            ebest_demo.order_stock(sell["expcode"][0], sell["janqty"][0], price=None, bns_type=1, order_type="03")
                            bot.sendMessage(chat_id='1039199786', text="[공시]"+sell["hname"][0] + "_" + sell["sunikrt"][0] + "% 시간다됨")
                            buylist_off = buylist_off[~buylist_off["종목코드"].str.contains(buylist_off['종목코드'][b])]
                            print("[공시매도]"+sell["hname"][0]+"_"+sell["sunikrt"][0]+"% 시간다됨")

                       elif (pd.to_numeric(cur_time.split(' ')[-2].split(":")[0]) >= 15) & (pd.to_numeric(cur_time.split(' ')[-2].split(":")[1]) >= 20):
                           ebest_demo.order_stock(sell["expcode"][0], sell["janqty"][0], price=None, bns_type=1, order_type="03")
                           bot.sendMessage(chat_id='1039199786', text="[공시]" + sell["hname"][0] + "_" + sell["sunikrt"][0] + "% 장마감")
                           print("영업종료")
                           break

                       else:
                           print("대기")
                   except:
                       print("매도 코드 error")
               else: #sell list에 없으면 -> 잔고에 해당 종목 없으면 버리고 에러코드드
                  buylist_off = buylist_off[~buylist_off["종목코드"].str.contains(buylist_off['종목코드'][b])]
                  print(buylist_off)
                  bot.sendMessage(chat_id='', text="[공시] 매도오류")

        except:
            print("잔량없거나 오류")

    buylist_off=buylist_off.reset_index(drop=True)

    time.sleep(30)
