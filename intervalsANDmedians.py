import os
import pandas as pd ; import numpy as np
import datetime as dt


###########################################################################################################
### LOADING DATA & format manipulations
###########################################################################################################

os.chdir("/home/hubert/Downloads/Data Cleaned/proxys")
BCH_proxys_raw = pd.read_csv("BCH_proxys_raw", sep=',')

BCH_proxys_raw.datetime =  pd.to_datetime(BCH_proxys_raw.datetime, format='%Y/%m/%d %H:%M:%S')
BCH_proxys_raw.corresp_OB_datetime =  pd.to_datetime(BCH_proxys_raw.corresp_OB_datetime, format='%Y/%m/%d %H:%M:%S')
#########################################################################################################
###  Drop unused columns
##########################################################################################################
#BCH_proxy = BCH_raw.drop(labels = ["volume","PA01","PB01","QA01","QB01","midpoint"], axis = 1)


#########################################################################################################
###  BUNDLE data into 5 min intervals
##########################################################################################################

# en observant la table nous constatons que pour chaque "corresp_OB_datetime" nous avons au moins 1 trade !
 # voir resultat des deux lignes ci dessous:
 counter = BCH_proxys_raw.groupby(["corresp_OB_datetime"]).count()
 counter[counter.price<1]

# donc si nous prenons des intervalles de 5 minutes nous aurons

def FiveMinClassifier(instant):
	discard = dt.timedelta(	minutes = instant.minute % 5, seconds = instant.second)
	instant -= discard
	if discard <= dt.timedelta(minutes=5):
		instant += dt.timedelta(minutes=5)
	return instant

# proceed to bundle by 5min
# example: 00:05:00 interval will hold contain all trades from 00:00:00 up to 00:05:00
BCH_proxys_raw["interval"] = BCH_proxys_raw.corresp_OB_datetime.apply(FiveMinClassifier)


BCH_medianized_proxys = BCH_proxys_raw.groupby(["interval"]).median()
BCH_medianized_proxys.to_csv("BCH_med_prox", index=True)


# os.chdir("/home/hubert/Downloads/Data Cleaned/proxys")
# z= pd.read_csv("BCH_med_prox", sep=',')