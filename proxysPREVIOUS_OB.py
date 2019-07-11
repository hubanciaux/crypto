import os
import pandas as pd ; import numpy as np
import datetime as dt

###########################################################################################################
### USED FUNCTIONS
###########################################################################################################

###########################################################################################################
### LOADING DATA & format manipulations
###########################################################################################################
#os.chdir("/home/hubert/Downloads/Data Cleaned/best")
os.chdir("/home/hubert/data/Data Cleaned/best")
BCH_ob_best = pd.read_csv("XRP_ob_best", sep=',')

#os.chdir("/home/hubert/Downloads/Data Cleaned/raw")
os.chdir("/home/hubert/data/Data Cleaned/raw")
BCH_trade = pd.read_csv("XRP_trade.csv", sep=',')


# transfo colonne SELL en colonne DIR {-1 / +1}

BCH_trade.loc[BCH_trade.sell==True,"dir"] = -1
BCH_trade.loc[BCH_trade.sell==False,"dir"] = 1

# elimine les colonnes inutiles dans TRADES
BCH_trade.drop(labels = ["id","exchange","symbol","day","sell","hour"], axis = 1, inplace = True)

# change datetime columns into actual datetime-type
BCH_ob_best.datetime =  pd.to_datetime(BCH_ob_best.datetime, format='%d/%m/%Y %H:%M:%S')
BCH_trade.datetime =  pd.to_datetime(BCH_trade.datetime, format='%d/%m/%Y %H:%M:%S')

###########################################################################################################
### SORT both dataframes  BY DATETIME
###########################################################################################################

BCH_ob_best.sort_values(by=['datetime'], inplace=True, ascending=True)
BCH_ob_best.reset_index(inplace=True)
BCH_ob_best.drop(labels = ["index"], axis = 1, inplace=True)


BCH_trade.sort_values(by=['datetime'], inplace=True, ascending=True)
BCH_trade.reset_index(inplace=True)
BCH_trade.drop(labels = ["index"], axis = 1, inplace=True)

###########################################################################################################
###  Add column in TRADES : corresponding OB datettime to Trade datetime
###########################################################################################################

tt=np.array(BCH_trade.datetime)
ot=np.array(BCH_ob_best.datetime)
nt = np.zeros(tt.shape, dtype='datetime64[ns]')
cursor=0
for i, t in enumerate(tt):
    if i%1000000==0:
        print(i)
    for j, o in enumerate(ot[cursor:]):
        if t < o:
            nt[i]=ot[cursor+j-1]
            cursor+=j
            break
        elif t == o:
            nt[i]=ot[cursor+j]
            cursor+=j
            break

nt=pd.Series(nt)
BCH_trade["corresp_OB_datetime"]=nt

# retirer les qqes derniers trades qui n'ont pas ob.datetime correspondant (25 derniers trades n'ont pas d'equivalent en ob)
BCH_trade = BCH_trade.loc[BCH_trade.corresp_OB_datetime!=np.datetime64('1970-01-01 00:00:00') , :]

###################################################################################################################
###  DURATION of quotes
##################################################################################################################

# 1) we start by computing the delta_times between each consecutive pair of quotes
BCH_ob_best["delta_time"] = (BCH_ob_best.datetime.diff().fillna(0)).apply(pd.Timedelta.total_seconds)

# la premiere obs devrait poser probleme (pcq on fait une diff du current - precedent)
# donc nous  retirerons les rows correspondant à la premiere date de OB dans les deux dataframes
# enregistrer la date qui pose probleme d abord
date_cassepied = BCH_ob_best.iloc[0].datetime

# retirer la premiere observation de TRADES car delta_time non-définie pour le premier element
BCH_ob_best = BCH_ob_best.iloc[1:]
BCH_ob_best.reset_index(drop=True, inplace=True)

# du coup retirer aussi les equivalents dans TRADES, en utilisant la date sauvegardée 'date_cassepied'
BCH_trade = BCH_trade.loc[BCH_trade.corresp_OB_datetime!=np.datetime64(date_cassepied) , :]
BCH_trade.reset_index(drop=True, inplace=True)
# 2) We check whether the quote has changed through time and we compute the DURATION 
# for this we use our previously computed delta times & use numpy for faster execution
A = np.array(BCH_ob_best.PA01)
B = np.array(BCH_ob_best.PB01)
T = np.array(BCH_ob_best.datetime)
D = np.array(BCH_ob_best.delta_time)
TW = np.zeros(T.shape)
MAX_DELTA_TIME = 60 # max 60 seconds between consecutive order book times

for index, (a, b, t, d) in enumerate(zip(A, B, T, D)):
    if index == 0: 
        prev_PB = b
        prev_PA = a
        tw = 0
        block_count = 0
    else:
        if (a != prev_PA or b != prev_PB 
            or index == len(T) - 1 or d > MAX_DELTA_TIME):
            if block_count > 1:
                x = TW[index -1]
                TW[index - block_count:index] = x
            tw = 0
            block_count = 0
            prev_PB = b
            prev_PA = a
    if d <= MAX_DELTA_TIME:
        tw += d
    else: # d > MAX_DELTA_TIME
        tw = MAX_DELTA_TIME
    block_count += 1
    TW[index] = tw

BCH_ob_best.insert(len(BCH_ob_best.columns), 'tw', TW)
BCH_ob_best.drop(labels = ["delta_time"], axis = 1, inplace=True)

#################################################################################################################
###  Calcul des proxys de liquidité EX ANTE
##################################################################################################################
# D'abord calculer le midpoint quote price
BCH_ob_best["midpoint"] = (BCH_ob_best["PA01"] + BCH_ob_best["PB01"]) / 2

# depth
BCH_ob_best["DEPTH"] = (BCH_ob_best["QA01"] + BCH_ob_best["QB01"]) / 2

# PQS" 	Proportional (percent) Quoted Spread "
BCH_ob_best["PQS"] = (BCH_ob_best["PA01"] - BCH_ob_best["PB01"]) / BCH_ob_best["midpoint"]





####################################################################################################################
###  MERGE TRADE & OB
####################################################################################################################

#remove duplicates on datetimes, by taking the median of every feature distrib
BCH_ob_group = BCH_ob_best.groupby(["datetime"], as_index=False).median()

# fusion des tables
BCH_merged = pd.merge(left = BCH_trade, right = BCH_ob_group, how = "left",left_on = "corresp_OB_datetime", right_on = "datetime" )
BCH_merged.drop(labels = ["datetime_y"], axis = 1, inplace = True)
BCH_merged = BCH_merged.rename(columns={"datetime_x":"datetime"})

####################################################################################################################
###  Calcul des proxys de liquidité EX POST
####################################################################################################################

# Proportional (percent) Effective Spread
BCH_merged["PES"] = (2 * BCH_merged.dir * (BCH_merged.price - BCH_merged.midpoint)) / BCH_merged.midpoint 

# Proportional (percent) Trade Spread
BCH_merged["PTS"] = BCH_merged.PES / 2


#########################################################################################################
###  BUNDLE data into 5 min intervals
##########################################################################################################

# en observant la table nous constatons que pour chaque "corresp_OB_datetime"
# nous avons au moins 1 trade !
# voir resultat des deux lignes ci dessous:
 counter = BCH_merged.groupby(["corresp_OB_datetime"]).count()
 counter[counter.price<1]

# donc si nous prenons des intervalles de 5 minutes nous aurons
# au minimumm des intervalles avec 5 trades

def FiveMinClassifier(instant):
    discard = dt.timedelta( minutes = instant.minute % 5, seconds = instant.second)
    instant -= discard
    if discard <= dt.timedelta(minutes=5):
        instant += dt.timedelta(minutes=5)
    return instant

# proceed to bundle by 5min
# example: 00:05:00 interval will hold contain all trades from 00:00:00 up to 00:05:00
BCH_merged["interval"] = BCH_merged.corresp_OB_datetime.apply(FiveMinClassifier)



# os.chdir("/home/hubert/Downloads/Data Cleaned/proxys")
os.chdir("/home/hubert/data/Data Cleaned/proxys")
BCH_medianized_proxys = BCH_merged.groupby(["interval"]).median()
BCH_medianized_proxys.to_csv("XRP_med_prox", index=True)

