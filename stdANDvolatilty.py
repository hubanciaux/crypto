import os
import pandas as pd ; import numpy as np
import datetime as dt
import matplotlib.pyplot as plt

###########################################################################################################
### LOADING DATA & format manipulations
###########################################################################################################

os.chdir("/home/hubert/Downloads")
vcrix = pd.read_csv("vcrix.csv", sep=',')
vcrix = vcrix[["date","vcrix"]]
vcrix.date =  pd.to_datetime(vcrix.date, format='%Y-%m-%d')

# put year, month and day info on separate columns
Y = pd.DatetimeIndex(np.array(vcrix.date)).year
M = pd.DatetimeIndex(np.array(vcrix.date)).month
D = pd.DatetimeIndex(np.array(vcrix.date)).day

vcrix.insert(len(vcrix.columns), 'day', D)
vcrix.insert(len(vcrix.columns), 'month', M)
vcrix.insert(len(vcrix.columns), 'year', Y)


# visualize the VCRIX
# plt.ion()
# vcrix.vcrix.plot()


# set thresholds
# lower bound :=  33% quantile of vcrix column
lower_bound = vcrix.vcrix.quantile(0.33)
# upper bound := 67% quantile ...
upper_bound = vcrix.vcrix.quantile(0.66)


# create and fill the volatily column 
vcrix.loc[(vcrix.vcrix<lower_bound) ,"volatility" ]="L"
vcrix.loc[(lower_bound<=vcrix.vcrix) &(vcrix.vcrix<=upper_bound) ,"volatility" ]="M"
vcrix.loc[(vcrix.vcrix>upper_bound) ,"volatility" ]="H"

# make the 'volatiity' feature a categorical variable
vcrix.volatility = vcrix.volatility.astype('category')
vcrix.volatility = vcrix.volatility.cat.set_categories(new_categories=["L", "M", "H"], ordered=True)


# Load all_proxies data
os.chdir("/home/hubert/Downloads/Data Cleaned/proxys/all_proxies")
all_proxies = pd.read_csv("XRP_stdized_prox", sep=',')

# merge volatilty dataframe with our datframe of liquidity proxies
vol_df = vcrix[["day","month","year","volatility"]]
proxies_and_vol = pd.merge(left =all_proxies , right =vol_df, how='left', on=["year","month","day"] )
os.chdir("/home/hubert/Downloads/Data Cleaned/final")
proxies_and_vol.to_csv("XRP_finalVCRIX", index = False)




# now we are able to observe the evolution of our proxies conditioned on the volatility of the day
# see page 9 Petitjean, Giot, Beaupain and replicate the procedure