rom ebest import EBest
import pandas as pd
import telegram
import time
from pandas import DataFrame as df
import xingapi as xa

telgm_token = ''
bot = telegram.Bot(token = telgm_token)
FMT = '%H:%M:%S'
print("_______________" + time.ctime() + "_______________")
bot.sendMessage(chat_id='', text="[갭락] 야수의 심장이여 눈을 떠라")

try:
    siga_yest = pd.read_csv('')
    siga_yest = siga_yest[pd.to_numeric(siga_yest["시가총액"])>1000].reset_index(drop=True) #1000억원 이상인 애들만
    siga_yest = siga_yest[pd.to_numeric(siga_yest["거래량"])>100000].reset_index(drop=True) #거래량 10만건 이상인 애들만
    siga_yest = siga_yest.drop(["전일대비구분","전일대비","등락율","거래량","거래비중","시가총액","비중","외인비중"], axis=1)
    siga_yest.rename(columns={'현재가':'yclose'}, inplace = True)
    print(siga_yest)
except:
    print("csv 불러오기 오류")


#시가총액 및 현재가 전일대비
try:
    app = xa.App(id='', pw='', cert='')
    query2 = xa.Query('t1427')
    #yasu_raw = query2.call(gubun='0',sdiff=-15,ediff=-5).next(keypairs={'idx':'idx'})
    #yasu=yasu_raw['t1452OutBlock1']

    yasu_raw1 = query2.call(gubun=0, signgubun="1", diff=0, jc_num="000016777728").next(keypairs={'idx': 'idx'})  # 거래정지 정리매매
    yasu1 = yasu_raw1['t1427OutBlock1']
    yasu_raw2 = query2.call(gubun=0, signgubun="2", diff=0, jc_num="000016777728").next(keypairs={'idx': 'idx'})
    yasu2 = yasu_raw2['t1427OutBlock1']
    yasu = pd.concat([yasu1, yasu2], axis=0).reset_index(drop=True)
    yasu_f=pd.merge(siga_yest, yasu, on='종목코드')
    yasu_f["gap"]=round((pd.to_numeric(yasu_f["시가"])/pd.to_numeric(yasu_f["yclose"])-1)*100,2)
    yasu_f["gap1"]=round((pd.to_numeric(yasu_f["현재가"])/pd.to_numeric(yasu_f["시가"])-1)*100,2)
    yasu_f = yasu_f[pd.to_numeric(yasu_f["gap"])<-5 ].reset_index(drop=True) #거래량 10만건 이상인 애들만
    yasu_f = yasu_f[pd.to_numeric(yasu_f["gap"])>-15].reset_index(drop=True)  # 거래량 10만건 이상인 애들만
    yasu_f = yasu_f[pd.to_numeric(yasu_f["전일거래량"])>100000].reset_index(drop=True) #거래량 10만건 이상인 애들만
    yasu_f = yasu_f[pd.to_numeric(yasu_f["현재가"])>2000].reset_index(drop=True) #한주 당 2000원 이상인 애들만
    yasu_f = yasu_f[pd.to_numeric(yasu_f["gap1"])<2].reset_index(drop=True) #이미 2프로 오른애들은 제외
    print(yasu_f)

    buylist_gap = df(data={'종목이름': [], '종목코드': [], '매수수량': [], '구매시간': []})

    # 로그인
    session = xa.Session()
    session.login(id='', pwd='', cert=None)

    ebest_demo = EBest("DEMO")
    ebest_demo.login()

    print(time.ctime())

    yasu_in = yasu_f[(yasu_f['종목코드'].isin(siga_yest['종목코드']))].reset_index(drop=True)
    yasu_out = yasu_f[~(yasu_f['종목코드'].isin(siga_yest['종목코드']))].reset_index(drop=True)
    print("yasu_in")
    print(yasu_in)
    print("yasu_out")
    print(yasu_out)

    print("가자!")
    print("_______________" + time.ctime() + "_______________")

    try:
        prob_mon = max(round(4000000 / len(yasu_in), 0),1000000)  # 한종목 당 구매 가능 금액 50만원 less
        print("구매가능금액 " + str(prob_mon))

    except:
        prob_mon = 1
        print("구매가능금액 오류")

    # 매수코드
    for n in range(0, len(yasu_in)):
        print("[갭락] 야수의 심장이여 " + time.ctime())
        try:
            c_price2 = pd.DataFrame(ebest_demo.get_current_market_price_by_code(yasu_in["종목코드"][n]))
            print(c_price2)
            c_amt2 = round(prob_mon / pd.to_numeric(c_price2["price"][0]), 0)
            print(yasu_in["종목코드"][n])
            ebest_demo.order_stock(yasu_in["종목코드"][n], c_amt2, price=None, bns_type=2, order_type="03")
            print(time.ctime())
            new_gap = df(data={'종목이름': [yasu_in["종목명"][n]], '종목코드': [yasu_in["종목코드"][n]], '매수수량': [c_amt2],
                               '구매시간': [time.strftime(FMT, time.localtime(time.time()))]})
            buylist_gap = buylist_gap.append(new_gap).reset_index(drop=True)
            bot.sendMessage(chat_id='1039199786', text="[갭락]" + yasu_in["종목명"][n] + " " + str(c_amt2) + "개 매수완료")
            print("[갭락]" + yasu_in["종목명"][n] + "_매수완료")
        except:
            print("[갭락]" + yasu_in["종목명"][n] + "_리스트미존재")
            bot.sendMessage(chat_id='1039199786', text="[갭락]]" + yasu["종목명"][n] + " 리스트 미존재")

    print(buylist_gap)

    # 매도코드
    while True:
        cur_time = time.ctime()
        print("_______________" + cur_time + "_______________")
        bot = telegram.Bot(token=telgm_token)

        # 구매한 애들 매도 코드
        if len(buylist_gap) > 0:
            print("매도코드")
            print(buylist_gap)

            try:
                # 잔량 있는 종목들만 가져와서 매도
                ac_left = pd.DataFrame(ebest_demo.ac_left())
                ac_left = ac_left[pd.to_numeric(ac_left["jsat"]) == 0]  # 오늘 산애들만 가져와
                ac_left = ac_left[pd.to_numeric(ac_left["janqty"]) > 0]  # 잔량 있는애들만 가져와
                ac_left = ac_left.reset_index(drop=True)
                print(ac_left)
                print("잔고")

                for b in range(0, len(buylist_gap)):
                    s_time = time.strftime('%H:%M:%S', time.localtime(time.time()))
                    sell = (ac_left[ac_left["expcode"] == buylist_gap['종목코드'][b]]).reset_index(drop=True)
                    if len(sell) > 0:
                        print("sell list")
                        print(sell)
                        try:
                            if pd.to_numeric(sell["sunikrt"][0]) >= 4:  # 순이익률이 4퍼 이상이면 판매
                                ebest_demo.order_stock(sell["expcode"][0], sell["janqty"][0], price=None, bns_type=1,
                                                       order_type="03")
                                bot.sendMessage(chat_id='1039199786',
                                                text="[갭락] " + sell["hname"][0] + " " + sell["sunikrt"][0] + "% 익절완료")
                                buylist_gap = buylist_gap[~buylist_gap["종목코드"].str.contains(buylist_gap['종목코드'][b])]
                                print("[갭락]" + sell["hname"][0] + "_" + sell["sunikrt"][0] + "% 익절완료")
                            elif pd.to_numeric(sell["sunikrt"][0]) < - 10:  # 순이익률이 -10퍼 이하면 판매
                                ebest_demo.order_stock(sell["expcode"][0], sell["janqty"][0], price=None, bns_type=1,
                                                       order_type="03")
                                bot.sendMessage(chat_id='1039199786',
                                                text="[갭락] " + sell["hname"][0] + " " + sell["sunikrt"][0] + "% 손절완료")
                                buylist_gap = buylist_gap[~buylist_gap["종목코드"].str.contains(buylist_gap['종목코드'][b])]
                                print("[갭락] " + sell["hname"][0] + " " + sell["sunikrt"][0] + "% 손절완료")
                            #elif pd.to_numeric(
                            #        (datetime.strptime(s_time, FMT) - datetime.strptime(buylist_gap["구매시간"][b],
                            #                                                            FMT)).seconds) > 36000:  #6시간으면 팔기
                            #    ebest_demo.order_stock(sell["expcode"][0], sell["janqty"][0], price=None, bns_type=1,
                            #                           order_type="03")
                            #    bot.sendMessage(chat_id='1039199786',
                            #                    text="[갭락] " + sell["hname"][0] + "_" + sell["sunikrt"][0] + "% 시간다됨")
                            #    buylist_gap = buylist_gap[~buylist_gap["종목코드"].str.contains(buylist_gap['종목코드'][b])]
                            #    print("[갭락] " + sell["hname"][0] + " " + sell["sunikrt"][0] + "% 시간다됨")

                            elif (pd.to_numeric(cur_time.split(' ')[-2].split(":")[0]) >= 15) & (
                                    pd.to_numeric(cur_time.split(' ')[-2].split(":")[1]) >= 18):
                                ebest_demo.order_stock(sell["expcode"][0], sell["janqty"][0], price=None, bns_type=1,
                                                       order_type="03")
                                bot.sendMessage(chat_id='1039199786',
                                                text="[갭락] " + sell["hname"][0] + " " + sell["sunikrt"][0] + "% 이제그만")
                                print("영업종료")
                                break

                            else:
                                print("대기")
                        except:
                            print("매도 코드 error")
                    else:  # sell list에 없으면 -> 잔고에 해당 종목 없으면 버리고 에러코드드
                        buylist_gap = buylist_gap[~buylist_gap["종목코드"].str.contains(buylist_gap['종목코드'][b])]
                        print("____________________________여기_________________________________")
                        print(buylist_gap)
                        bot.sendMessage(chat_id='1039199786', text="[갭락] 매도오류")

            except:
                print("잔량없거나 오류")

        else:
            print(len(buylist_gap))
            bot.sendMessage(chat_id='1039199786', text="[갭락] 더 이상 팔게 없다.")
            break

        buylist_gap = buylist_gap.reset_index(drop=True)
        print(buylist_gap)
        time.sleep(15)

    print("탈출")
    bot.sendMessage(chat_id='1039199786', text="[갭락] 성공적인 야수의 심장")
    print("[갭락] 성공적인 야수의 심장")

except:
    bot.sendMessage(chat_id='1039199786', text="[갭락] 전일가 대비 조회 실패")
    print("전일가 대비 조회 실패")

#20200914 0020
