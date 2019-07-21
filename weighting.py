# alternative 3-hour intervals at the end of script
import os
import pandas as pd ; import numpy as np
import datetime as dt


###########################################################################################################
### LOADING DATA & format manipulations
###########################################################################################################

os.chdir("/home/hubert/Downloads/Data Cleaned/proxys/proxys")
BCH_med = pd.read_csv("XRP_medmean", sep=',')

BCH_med.interval =  pd.to_datetime(BCH_med.interval, format='%Y/%m/%d %H:%M:%S')


##############################################################################################################
### PART	I		 compute single proxies for all 15-min intervals and for all CCIES
### PART 	II		 compute market wide proxies
##############################################################################################################


##############################################################################################################
### PART I 	
##############################################################################################################

# buil columns announcing the year, month, day, hout, minute of the medianazed
Y = pd.DatetimeIndex(np.array(BCH_med.interval)).year
M = pd.DatetimeIndex(np.array(BCH_med.interval)).month
D = pd.DatetimeIndex(np.array(BCH_med.interval)).day
H = pd.DatetimeIndex(np.array(BCH_med.interval)).hour
T = pd.DatetimeIndex(np.array(BCH_med.interval)).minute
G = np.zeros(T.shape)

# for index, (t,g) in enumerate(zip(T,G)):
# 	if t == 5 or t==10 or t==15:
# 		g=15
# 	elif t==20 or t==25 or t==30:
# 		g=30
# 	elif t==35 or t==40 or t==45:
# 		g=45
# 	elif t==50 or t==55 or t==00:
# 		g=60
# 	G[index] = g

for index, (t,g) in enumerate(zip(T,G)):
	if t == 0 or t==5 or t==10:
		g=15
	elif t==15 or t==20 or t==25:
		g=30
	elif t==30 or t==35 or t==40:
		g=45
	elif t==45 or t==50 or t==55:
		g=60
	G[index] = g

BCH_med.insert(len(BCH_med.columns), 'group', G)
BCH_med.insert(len(BCH_med.columns), 'hour', H)
BCH_med.insert(len(BCH_med.columns), 'day', D)
BCH_med.insert(len(BCH_med.columns), 'month', M)
BCH_med.insert(len(BCH_med.columns), 'year', Y)


################################################################################################
###     COMPUTE RETURN AND SQUARED RETURNS
################################################################################################
# yeas: 1 , no: 0
inter_interval_returns = 1
#intra_interval_returns = 1



# compute Equally Weighted proxies (reintroduce groupby keys as index: hence 5 times same command)
# in the process we also compute the log return over these 15min intervals
def ewAverage(x):
	return sum(x)/len(x)

def ret(x):
	y = x[1:]
	return sum(y)

f = {'PQS':ewAverage, 'DEPTH':ewAverage, 'return':ret}

# compute log returns over our base : 5 min intervals   
if not inter_interval_returns:
	BCH_med["return"] = (np.log(BCH_med.price)).diff()
	EW =  BCH_med.groupby(["year","month","day","hour","group"]).agg(f)
else:
	EW =  BCH_med.groupby(["year","month","day","hour","group"]).mean()
	EW["return"] = (np.log(EW.price)).diff()


EW.reset_index(level=0, inplace=True)
EW.reset_index(level=0, inplace=True)
EW.reset_index(level=0, inplace=True)
EW.reset_index(level=0, inplace=True)
EW.reset_index(level=0, inplace=True)
# subset relevant features and include return as it is also taken as EW average
EW= EW[["year","month","day","hour","group","PQS","DEPTH","return"]]
EW["V"] = np.power(EW["return"],2)
EW = EW.rename(columns={"PQS":"EWPQS", "DEPTH":"EWDEPTH"})


# compute Size Weighted proxies ...
wm_pqs = lambda x: np.average(x, weights=BCH_med.loc[x.index, "DEPTH"])
wm_pes = lambda x: np.average(x, weights=BCH_med.loc[x.index, "amount"])
wm_pts = lambda x: np.average(x, weights=BCH_med.loc[x.index, "amount"])
f = {'PQS': wm_pqs, 'PES': wm_pes, 'PTS': wm_pts}
SW = BCH_med.groupby(["year","month","day","hour","group"]).agg(f)
SW.reset_index(level=0, inplace=True)
SW.reset_index(level=0, inplace=True)
SW.reset_index(level=0, inplace=True)
SW.reset_index(level=0, inplace=True)
SW.reset_index(level=0, inplace=True)
SW = SW[["year","month","day","hour","group", "PQS", "PES", "PTS"]]
SW = SW.rename(columns={"PQS":"SWPQS", "PES":"SWPES", "PTS":"SWPTS"})

# TW 
# Define a lambda function to compute the weighted mean:
wm_s = lambda x: np.average(x, weights=BCH_med.loc[x.index, "tw"])
f = {'PQS': wm_s}
TW = BCH_med.groupby(["year","month","day","hour","group"]).agg(f)
TW.reset_index(level=0, inplace=True)
TW.reset_index(level=0, inplace=True)
TW.reset_index(level=0, inplace=True)
TW.reset_index(level=0, inplace=True)
TW.reset_index(level=0, inplace=True)
TW= TW[["year","month","day","hour","group", "PQS"]]
TW = TW.rename(columns={"PQS":"TWPQS"})


# merge everything
WProx = pd.merge(EW, SW, how="left", on=["year","month","day","hour","group"]).merge(TW, how="left", on=["year","month","day","hour","group"])
#WProx.rename(columns={'group':'minute'}, inplace=True)

#WProx["date"] = pd.to_datetime(WProx[['year', 'month', 'day', 'hour', 'minute']])


# retirer la premiere observation comme toujours qd on fais une diff alors quand on n'a pas le premier element
WProx= WProx.iloc[1:]
WProx.reset_index(drop=True, inplace=True)


WProx.rename(columns={'group':'minute'}, inplace=True)
WProx["date"] = pd.to_datetime(WProx[['year', 'month', 'day', 'hour', 'minute']])
# save it
os.chdir("/home/hubert/Downloads/Data Cleaned/proxys/all_proxies")
WProx.to_csv("XRP_all_proxies", index=False)



### ALTERNATIVE just in case ####


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
# 		g=24		
# 	G[index] = g

# BCH_med.insert(len(BCH_med.columns), 'group', G)

# ###########################################################################################################
# ### Standadize wrt to intra group MEAN and ST_DEV
# ###########################################################################################################

# BCH_stdized = BCH_med.groupby("group").transform(lambda x: (x - x.mean()) / x.std())


