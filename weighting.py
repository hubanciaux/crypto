import os
import pandas as pd ; import numpy as np
import datetime as dt


###########################################################################################################
### LOADING DATA & format manipulations
###########################################################################################################

os.chdir("/home/hubert/Downloads/Data Cleaned/proxys/proxys")
BCH_med = pd.read_csv("BCH_med_prox", sep=',')

BCH_med.drop(labels = ["dir","PB01","PA01","QB01","QA01","midpoint"], axis = 1, inplace = True)
BCH_med.interval =  pd.to_datetime(BCH_med.interval, format='%Y/%m/%d %H:%M:%S')


###########################################################################################################
### Classify all intervals (for the year) into the 24*4 = 96 possible 15-min intervals of a day
###########################################################################################################
H = pd.DatetimeIndex(np.array(BCH_med.interval)).hour
T = pd.DatetimeIndex(np.array(BCH_med.interval)).minute
G = np.zeros(T.shape)

for index, (t,g) in enumerate(zip(T,G)):
	if t == 5 or t==10 or t==15:
		g=15
	elif t==20 or t==25 or t==30:
		g=30
	elif t==35 or t==40 or t==45:
		g=45
	elif t==50 or t==55 or t==00:
		g=00
	G[index] = g

BCH_med.insert(len(BCH_med.columns), 'group', G)
BCH_med.insert(len(BCH_med.columns), 'hour', H)


###########################################################################################################
###
### Compute different averages of our proxys within these intervals:  Equally-Weighted
###		Size-Weighted, Time-Weighted
###
############################################################################################################
# EQUALLY WEIGHTED
#############################################################################################################
#Standadize wrt to intra group MEAN and ST_DEV
base_EW = BCH_med[["PQS","DEPTH","group","hour"]]
EW = base_EW.groupby(["hour","group"]).transform(lambda x: (x - x.mean()) / x.std())
EW = EW.rename(columns={"PQS":"EWPQS", "DEPTH":"EWDEPTH"})

#############################################################################################################
###
#############################################################################################################












# ###########################################################################################################
# ### Classify all intervals for all days into 3-hour intervals
# ###########################################################################################################
# H = pd.DatetimeIndex(np.array(BCH_med.interval)).hour
# G = np.zeros(H.shape)

# for index, (h,g) in enumerate(zip(H,G)):
# 	if h==0 or h==1 or h==2:
# 		g=3
# 	elif h==3 or h==4 or h==5:
# 		g=6
# 	elif h==6 or h==7 or h==8:
# 		g=9
# 	elif h==9 or h==10 or h==11:
# 		g=12
# 	elif h==12 or h==13 or h==14:
# 		g=15
# 	elif h==15 or h==16 or h==17:
# 		g=18
# 	elif h==18 or h==19 or h==20:
# 		g=21
# 	elif h==21 or h==22 or h==23:
# 		g=00		
# 	G[index] = g

# BCH_med.insert(len(BCH_med.columns), 'group', G)

# ###########################################################################################################
# ### Standadize wrt to intra group MEAN and ST_DEV
# ###########################################################################################################

# BCH_stdized = BCH_med.groupby("group").transform(lambda x: (x - x.mean()) / x.std())

