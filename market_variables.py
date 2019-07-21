import os
import pandas as pd ; import numpy as np
import datetime as dt

# load all cryptocurrencies's data
os.chdir("/home/hubert/Downloads/Data Cleaned/final")
BCH = pd.read_csv("BCH_finalVCRIX", sep=',')
BTC = pd.read_csv("BTC_finalVCRIX", sep=',')
EOS = pd.read_csv("EOS_finalVCRIX", sep=',')
ETH = pd.read_csv("ETH_finalVCRIX", sep=',')
XRP = pd.read_csv("XRP_finalVCRIX", sep=',')

#format dates as usual
BCH.date = pd.to_datetime(BCH.date, format='%Y-%m-%d')
BTC.date = pd.to_datetime(BTC.date, format='%Y-%m-%d')
EOS.date = pd.to_datetime(EOS.date, format='%Y-%m-%d')
ETH.date = pd.to_datetime(ETH.date, format='%Y-%m-%d')
XRP.date = pd.to_datetime(XRP.date, format='%Y-%m-%d')



# match times across CCies, so that for each ccy we have the same time indices
# this is needed in order to compute complementar "market" proxies
intersection_dates = list(set.intersection(set(BCH.date),set(BTC.date),set(EOS.date),set(ETH.date),set(XRP.date)))
# we subset our ccies based on this intersection of dates and we reset the index of the vector 
# for easier vector operations 
BCH = BCH.loc[BCH.date.isin(intersection_dates),:].reset_index()
BTC = BTC.loc[BTC.date.isin(intersection_dates),:].reset_index()
EOS = EOS.loc[EOS.date.isin(intersection_dates),:].reset_index()
ETH = ETH.loc[ETH.date.isin(intersection_dates),:].reset_index()
XRP = XRP.loc[XRP.date.isin(intersection_dates),:].reset_index()

# weights for Market-wide variable such as Returns and Liquidity-proxies
# select equal_weighted or marketcap_weighted (yes: 1, no: 0)
equal_weighted = 1
#marketcap_weighted = 1


if equal_weighted:
    BCH["w_idx"] = 0.2
    BTC["w_idx"] = 0.2
    EOS["w_idx"] = 0.2
    ETH["w_idx"] = 0.2
    XRP["w_idx"] = 0.2

else:
    os.chdir("/home/hubert/Documents")
    mc_weights = pd.read_csv("mc_weights", sep=',')
    mc_weights.date = pd.to_datetime(mc_weights.date, format='%Y-%m-%d')
    # put year, month and day info on separate columns

    Y = pd.DatetimeIndex(np.array(mc_weights.date)).year
    M = pd.DatetimeIndex(np.array(mc_weights.date)).month
    D = pd.DatetimeIndex(np.array(mc_weights.date)).day

    mc_weights.insert(len(mc_weights.columns), 'day', D)
    mc_weights.insert(len(mc_weights.columns), 'month', M)
    mc_weights.insert(len(mc_weights.columns), 'year', Y)

    mc_weights=mc_weights[["year","month","day","widx_BCH","widx_BTC","widx_EOS","widx_ETH","widx_XRP"]]

    BCH = pd.merge(left = BCH, right = mc_weights[["year","month","day","widx_BCH"]], how= 'left', on = ["year","month","day"])
    BCH.rename(columns={"widx_BCH":"w_idx"}, inplace=True)

    BTC = pd.merge(left = BTC, right = mc_weights[["year","month","day","widx_BTC"]],how= 'left', on = ["year","month","day"])
    BTC.rename(columns={"widx_BTC":"w_idx"}, inplace=True)

    EOS = pd.merge(left = EOS, right = mc_weights[["year","month","day","widx_EOS"]],how= 'left', on = ["year","month","day"])
    EOS.rename(columns={"widx_EOS":"w_idx"}, inplace=True)

    ETH = pd.merge(left = ETH, right = mc_weights[["year","month","day","widx_ETH"]],how= 'left', on = ["year","month","day"])
    ETH.rename(columns={"widx_ETH":"w_idx"}, inplace=True)

    XRP = pd.merge(left = XRP, right = mc_weights[["year","month","day","widx_XRP"]],how= 'left', on = ["year","month","day"])
    XRP.rename(columns={"widx_XRP":"w_idx"}, inplace=True)


###################################################################################################################
###		COMPUTE -- Market Proxies --
####################################################################################################################

# we build a function that computes the Market Proxies (taking the originating ccy out of the equation each time)
def complementaryMARKETproxy( proxy_name , crypto_name):
    C = {"BCH":BCH,"BTC":BTC,"EOS":EOS,"ETH":ETH,"XRP":XRP}
    L = pd.Series(np.zeros(BCH.shape[0]))
    for key, val in C.items():
        if key!=crypto_name:
            L += val[proxy_name]*val["w_idx"]   
    return L        

# extend original dataframes witth their corresponding MARKET WIDE proxies
proxies = ["EWPQS", "EWDEPTH", "SWPQS", "SWPES", "SWPTS", "TWPQS","return"]

for p in proxies:
    BCH["MKT_"+p] = complementaryMARKETproxy(p,"BCH")
    BTC["MKT_"+p] = complementaryMARKETproxy(p,"BTC")
    EOS["MKT_"+p] = complementaryMARKETproxy(p,"EOS")
    ETH["MKT_"+p] = complementaryMARKETproxy(p,"ETH")
    XRP["MKT_"+p] = complementaryMARKETproxy(p,"XRP")



os.chdir("/home/hubert/Downloads/Data Cleaned/regressions")
BCH.to_csv("baseBCH", index = False)
BTC.to_csv("baseBTC", index = False)
EOS.to_csv("baseEOS", index = False)
ETH.to_csv("baseETH", index = False)
XRP.to_csv("baseXRP", index = False)


###################################################################################################################
###     EXTEND -- 1-Lag, 1-Lead Proxies --
####################################################################################################################


# for p in ["MKT_EWPQS", "MKT_EWDEPTH", "MKT_SWPQS", "MKT_SWPES", "MKT_SWPTS", "MKT_TWPQS", "MKT_return"]:
#     BCH[p+"_LAG1"] = BCH[p].shift(periods = 1, axis = 0)
#     BTC[p+"_LAG1"] = BTC[p].shift(periods = 1, axis = 0)
#     EOS[p+"_LAG1"] = EOS[p].shift(periods = 1, axis = 0)
#     ETH[p+"_LAG1"] = ETH[p].shift(periods = 1, axis = 0)
#     XRP[p+"_LAG1"] = XRP[p].shift(periods = 1, axis = 0)

#     BCH[p+"_LAG2"] = BCH[p].shift(periods = 2, axis = 0)
#     BTC[p+"_LAG2"] = BTC[p].shift(periods = 2, axis = 0)
#     EOS[p+"_LAG2"] = EOS[p].shift(periods = 2, axis = 0)
#     ETH[p+"_LAG2"] = ETH[p].shift(periods = 2, axis = 0)
#     XRP[p+"_LAG2"] = XRP[p].shift(periods = 2, axis = 0)

#     BCH[p+"_LAG3"] = BCH[p].shift(periods = 3, axis = 0)
#     BTC[p+"_LAG3"] = BTC[p].shift(periods = 3, axis = 0)
#     EOS[p+"_LAG3"] = EOS[p].shift(periods = 3, axis = 0)
#     ETH[p+"_LAG3"] = ETH[p].shift(periods = 3, axis = 0)
#     XRP[p+"_LAG3"] = XRP[p].shift(periods = 3, axis = 0)

#     BCH[p+"_LEAD1"] = BCH[p].shift(periods = -1, axis = 0)
#     BTC[p+"_LEAD1"] = BTC[p].shift(periods = -1, axis = 0)
#     EOS[p+"_LEAD1"] = EOS[p].shift(periods = -1, axis = 0)
#     ETH[p+"_LEAD1"] = ETH[p].shift(periods = -1, axis = 0)
#     XRP[p+"_LEAD1"] = XRP[p].shift(periods = -1, axis = 0)

#     BCH[p+"_LEAD2"] = BCH[p].shift(periods = -2, axis = 0)
#     BTC[p+"_LEAD2"] = BTC[p].shift(periods = -2, axis = 0)
#     EOS[p+"_LEAD2"] = EOS[p].shift(periods = -2, axis = 0)
#     ETH[p+"_LEAD2"] = ETH[p].shift(periods = -2, axis = 0)
#     XRP[p+"_LEAD2"] = XRP[p].shift(periods = -2, axis = 0)

#     BCH[p+"_LEAD3"] = BCH[p].shift(periods = -3, axis = 0)
#     BTC[p+"_LEAD3"] = BTC[p].shift(periods = -3, axis = 0)
#     EOS[p+"_LEAD3"] = EOS[p].shift(periods = -3, axis = 0)
#     ETH[p+"_LEAD3"] = ETH[p].shift(periods = -3, axis = 0)
#     XRP[p+"_LEAD3"] = XRP[p].shift(periods = -3, axis = 0)

# # remove first and last observation for each CCY
# # BCH = BCH[1:-1]
# # BTC = BTC[1:-1]
# # EOS = EOS[1:-1]
# # ETH = ETH[1:-1]
# # XRP = XRP[1:-1]

# BCH = BCH[3:-3]
# BTC = BTC[3:-3]
# EOS = EOS[3:-3]
# ETH = ETH[3:-3]
# XRP = XRP[3:-5]

# os.chdir("/home/hubert/Downloads/Data Cleaned/regressions")
# BCH.to_csv("baseBCH", index = False)
# BTC.to_csv("baseBTC", index = False)
# EOS.to_csv("baseEOS", index = False)
# ETH.to_csv("baseETH", index = False)
# XRP.to_csv("baseXRP", index = False)
import pandas as pd
import numpy as np
base = pd.DataFrame([[7,580,-63],[4,645,-24],[6,555,-44]], columns=["x","y","z"])


def complementaryMARKETproxy( proxy_name , crypto_name):
    C = {"BCH":BCH,"BTC":BTC,"EOS":EOS,"ETH":ETH,"XRP":XRP}
    L = pd.Series(np.zeros(BCH.shape[0]))
    for key, val in C.items():
        if key!=crypto_name:
            L += val[proxy_name]*val["w_idx"]   
    return L        

# extend original dataframes witth their corresponding MARKET WIDE proxies
proxies = ["EWPQS", "EWDEPTH", "SWPQS", "SWPES", "SWPTS", "TWPQS","return"]