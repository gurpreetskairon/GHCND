#################################################################################################
## Analysis Q4d:                                                                     ##
#################################################################################################

import pandas as pd
import glob
import os
import matplotlib.pylab as plt

 

path = r'C:\Gurpreet\UC\DATA420-Scalable Data Science\Assignments\Assignment1\temperature'
all_files = glob.glob(os.path.join(path, "*.csv"))     # advisable to use os.path.join as this makes concatenation OS independent

li = []

#print(all_files[0])
#df = pd.read_csv(r'C:\Gurpreet\UC\DATA420-Scalable Data Science\Assignments\Assignment1\temperature\daily_all_nz_T_elements.csv')

#Interagtion all the files

for filename in all_files:
    #print(filename)
    df = pd.read_csv(filename)
    li.append(df)


totalData = pd.concat(li, axis=0, ignore_index=True)
totalData['DATE'] =  pd.to_datetime(totalData['DATE'], format='%Y%m%d')
totalData['YEAR'] = pd.DatetimeIndex(totalData['DATE']).year
totalData['DEGREES'] = totalData.VALUE * 0.1

# For each station of New Zealand

for key, grp in totalData.groupby(['ID']):
    grp = grp.pivot(index='DATE', columns='ELEMENT', values='DEGREES')
    grp.plot()
    plt.title(key)
    plt.ylabel('Temperature in Degrees Centigrade')
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1))
    plt.tight_layout()
    plt.show()

   
# Average of TMIN and TMAX for whole data

averageData = totalData.groupby(['YEAR', 'ELEMENT']).mean().reset_index()

plt_avg = averageData.pivot(index='YEAR', columns='ELEMENT', values='VALUE')
plt_avg.plot()
plt.title("New Zealands Average Temperatures Over the Years")
plt.ylabel('Temperature in Degrees Centigrade')
plt.legend(loc='upper right', bbox_to_anchor=(1, 1))
plt.tight_layout()
plt.show()