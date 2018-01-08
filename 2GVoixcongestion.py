
import sqlanydb
import SenderMail
from SenderMail import Sender
from SenderMail import variation
import numpy as np
from email.mime.text import MIMEText
import time
import datetime
import pandas as pd
from getdata import getData2GDayBETWEENCONGESTION
from getdata import insert_data

df=getData2GDayBETWEENCONGESTION()



TCHCongestion=pd.Series()
TCHAttemp=pd.Series()
TCHBlock=pd.Series()

TCHAttemp=pd.to_numeric(df["CLTCH_TASSALL"])
TCHBlock=pd.to_numeric(df["CLSDCCH_CNRELCONG"]) + pd.to_numeric(df["CELTCHF_TFNRELCONG"]) + pd.to_numeric(df["CELTCHH_THNRELCONG"]) + pd.to_numeric(df["CELTCHF_TFNRELCONGSUB"]) +pd.to_numeric(df["CELTCHH_THNRELCONGSUB"])

TCHCongestion=round((TCHBlock / TCHAttemp)*100,2)
#TRAFIC==([pmPdcpVolDlDrb]-[pmPdcpVolDlDrbLastTTI] + [pmUeThpVolUl]) /(8*1000*1000)
data=df.assign(TCHCongestion=pd.to_numeric(TCHCongestion.values))
datas=data.drop(['CLTCH_TASSALL','CLSDCCH_CNRELCONG', 'CELTCHF_TFNRELCONG', 'CELTCHH_THNRELCONG','CELTCHF_TFNRELCONGSUB','CELTCHH_THNRELCONGSUB' ], axis=1)

topdatascong=datas.where(datas["TCHCongestion"]>60).dropna(axis=0, how='any').sort_values(by='TCHCongestion', ascending=False)
datascong=datas.where(datas["TCHCongestion"]>2).dropna(axis=0, how='any').sort_values(by='site_name', ascending=False)
msg=" Bonjour \n voici les Top sites les plus congestionné d'aujourd'hui \n"
msg=msg+str(topdatascong)

msg=msg+"\n"
msg=msg+"\n voici la listes de tous les sites congestionnée \n"

msg=msg+str(datascong)


msg=msg+"\n Cordialement \n"
print(msg)
for index,val in datascong.iterrows() :
    columns="date_id,time,site_name,cell_name,tchcongestion"
    data="'"+str(val["date_id"])+"'"+","+"0"+","+"'"+str(val["site_name"])+"'"+","+"'"+str(val["cell_name"])+"'"+","+str(val["TCHCongestion"])
  
    id=insert_data("alerte_congestion2G",columns,data)
