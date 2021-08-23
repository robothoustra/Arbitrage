# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 15:02:51 2020

@author: arobert
"""

import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import sys
import numpy as np

def Calc_Contrib_Position(M0, M1, MContrib, dt0):
    """ Calcul la contribution à la performance de chaque position entre 
    la date dt0 (Matrice M0) et dt1 (Matrice M1) """
    M1_temp = M1[(M1['QTEINV_NOMINAL']!=0) & (M1['CODE_CTR'].isna())]
    M0_temp = M0[(M0['QTEINV_NOMINAL']!=0) & (M0['CODE_CTR'].isna())]
    MContrib.loc[M0[M0.index.get_level_values('CATV')=='VMOB'].index,dt0] = (M1_temp['LAST']/M0_temp['LAST']-1)*M0_temp['PRCT_ACT_NET']
    if len(MContrib.loc[M0[M0.index.get_level_values('CATV')=='CRNE'].index,dt0]) > 0:
        MContrib.loc[M0[M0.index.get_level_values('CATV')=='CRNE'].index,dt0] = ((M1_temp['VALEUR_BOURSIERE']/M1_temp['QTEINV_NOMINAL'])/(M0_temp['VALEUR_BOURSIERE']/M0_temp['QTEINV_NOMINAL'])-1)*M0_temp['PRCT_ACT_NET']
    
    if len(MContrib.loc[M0[M0.index.get_level_values('CATV')=='CAT'].index,dt0])>0:
        #MContrib.loc[M0[M0.index.get_level_values('CATV')=='CAT'].index,dt0] = (M0_temp['LAST']/M1_temp['LAST']-1)*M0_temp['PRCT_ACT_NET']
        MContrib.loc[M0[M0.index.get_level_values('CATV')=='CAT'].index,dt0] = (M1_temp['VALEUR_BOURSIERE']-M0_temp['VALEUR_BOURSIERE'])/M0_temp['ACTIF_NET']
    #Pour les futures : (Qt1*Last0-Qt0*Last0)*Nominal/Actif_Net
    if len(MContrib.loc[M0[M0.index.get_level_values('CATV')=='FUTU'].index,dt0]) > 0 :
        act_net = M1['VALEUR_BOURSIERE'].sum()
        MContrib.loc[M0[M0.index.get_level_values('CATV')=='FUTU'].index,dt0] = (M0_temp['QTEINV_NOMINAL']*M1_temp['LAST']-M0_temp['QTEINV_NOMINAL']*M0_temp['LAST'])*(M0_temp['VALEUR_BOURSIERE']/(M0_temp['QTEINV_NOMINAL']*M0_temp['LAST']-M0_temp['QTEINV_NOMINAL']*M0_temp['PRU']))/act_net
    #MContrib.loc[M0[M0.index.get_level_values('CATV')=='TRES'].index,dt0] = (M1['VALEUR_BOURSIERE']/M0['VALEUR_BOURSIERE']-1)*M0['PRCT_ACT_NET']
    
def Calc_Portage(M0, M1, dt0, dt1, MPerf, MContrib, MPNL):
    """ Calcul la contribution à la performance du portage des obligations (coupon) ainsi
    que la performance entre la date dt0 (Matrice M0) et dt1 (Matrice M1)"""

    #Récupère l'ensemble des obligations du portefeuille en t1
    listOblig = M1.loc[(M1.index.get_level_values('CATV') == 'VMOB') & (M1['TYPEVAL'] == 'P1')].index
    
    #Calcul la perf et la contribution du coupon
    MPerf.loc[listOblig,dt1] = M1['COUPON_COURU_TOTAL']/(M1['NOMINAL']*M1['QTEINV_NOMINAL'])-M0['COUPON_COURU_TOTAL']/(M0['NOMINAL']*M0['QTEINV_NOMINAL'])
    MContrib.loc[listOblig,dt1] = MPerf.loc[listOblig,dt1]*M1.loc[listOblig,'PRCT_ACT_NET']
    MPNL.loc[listOblig,dt1] = MPerf.loc[listOblig,dt1]*M1['NOMINAL']*M1['QTEINV_NOMINAL']

    #Ajoute à la liste des coupons ceux en t1 ayant la colonne CATV=CPON
    listCoupon = M1.loc[(M1.index.get_level_values('CATV') == 'CPON') & (M1['TYPEVAL'] == 'T1')].index.get_level_values('CODEISIN').values.tolist()

    #Ajoute à la liste des coupons ceux ayant une performance de coupon négative
    #et une date de next coupon différente de t0 ou un nombre de jour restant jusqu'à la prochaine date de coupon <= 5
    listChangeDateCpn = M1.loc[M1.index.get_level_values('CATV')=='VMOB',['D_T_CPN','NBJ_NEXT_CPN']].join(M0[['D_T_CPN','NBJ_NEXT_CPN']],lsuffix='_M1',rsuffix='_M0')
    listChangeDateCpn = listChangeDateCpn[((listChangeDateCpn['D_T_CPN_M1'] != listChangeDateCpn['D_T_CPN_M0']) & (listChangeDateCpn['D_T_CPN_M1'].notna())) | (listChangeDateCpn['NBJ_NEXT_CPN_M1']<=5)]
    
    if dt1==datetime(2020,3,31,0,0):
        print(dt0)
    
    if len(listChangeDateCpn) >0 :
        #join la liste des coupons ayant une date de next cpn proche avec ceux ayant
        #une perf negative et supérieur à 50 fois la dernière
        dfPerfNeg = M1.loc[(MPerf[dt1]<0) & (abs(MPerf[dt1])/abs(MPerf[dt0])>50)]
        if len(dfPerfNeg)>0:
            listCoupon.extend(listChangeDateCpn.join(dfPerfNeg,how='inner').index.get_level_values('CODEISIN').values.tolist())

    #Supprime les doublons
    if len(listCoupon)>1:
        listCoupon = list(set(listCoupon))

    listCpnDejaConnu = M1[(M1.index.get_level_values('CATV') == 'CPON')].join(M0[(M0.index.get_level_values('CATV') == 'CPON')], how='inner', rsuffix='_M0').index.get_level_values('CODEISIN').values.tolist()
    if len(listCpnDejaConnu)>0:
        for x in listCpnDejaConnu:
            if x in listCoupon:
                listCoupon.remove(x)
    
    #S'il existe des ligne coupon (CATV='CPON') alors il faut vérifier si le dernier jour
    #de coupon à déjà été comptabilisé dans le tableau de perf
    if len(listCoupon) >0:
        listOblig = list()
        M1.sort_index(inplace=True)
        M1_temp = M1.copy()
        for ISIN in listCoupon:
            #Pour réintégrer le dernier coupon il faut au moins que la valeur ('VMOB') soit
            #présente en t0, si non c'est qu'il a déjà été réintégrer pour calculer la perf
            #du dernier jour de coupon
            if ((ISIN,'VMOB') in M0.index) == True:         
                #Quand il y a un remboursement final, il se peut qu'il y ait une ligne "ISIN, 'CPON'"
                #mais pas de ligne "ISIN, 'VMOB'" en t1, dans ce cas il faut ajouter au portefeuille temporaire M1_temp
                #la ligne "ISIN, 'VMOB' pour y mettre le COUPON_COURU_TOTAL égale au champs
                # 'VALEUR_BOURSIERE' de la ligne "ISIN, 'CPON'"
                if (((ISIN,'VMOB') in M1_temp.index) == False):
                    listOblig.append((ISIN,'VMOB'))
                    newIndex = pd.MultiIndex.from_tuples([(ISIN, 'VMOB')], names=['CODEISIN', 'CATV'])
                    newRow = pd.DataFrame(np.zeros((1,len(M1_temp.columns))),index=newIndex, columns=M1_temp.columns)
                    #Si sinkable
                    if M1_temp.loc[(ISIN,'CPON'),'NOMINAL'] > M0.loc[(ISIN,'VMOB'),'NOMINAL']:
                        newRow.loc[(ISIN,'VMOB'):,['NOMINAL','QTEINV_NOMINAL']] = M0.loc[(ISIN,'VMOB'),['NOMINAL','QTEINV_NOMINAL']].values
                    else:
                        newRow.loc[(ISIN,'VMOB'):,['NOMINAL','QTEINV_NOMINAL']] = M1_temp.loc[(ISIN,'CPON'),['NOMINAL','QTEINV_NOMINAL']].values
                    M1_temp = M1_temp.append(newRow)
                    M1_temp.loc[(ISIN,'VMOB'),'COUPON_COURU_TOTAL'] = M1_temp.loc[(ISIN,'CPON'),'VALEUR_BOURSIERE']                        
                    M1_temp.loc[(ISIN,'VMOB'),'PRCT_ACT_NET'] = M0.loc[(ISIN,'VMOB'),'PRCT_ACT_NET']                        
                #Si ce n'est pas un remboursement final alors il faut vérifier que la valeur ('VMOB')
                #en t1 ait bien un coupon supérieur ou égale à zero                
                elif M1_temp.loc[(ISIN,'VMOB'),'COUPON_COURU_TOTAL'] >= 0:
                    listOblig.append((ISIN,'VMOB'))
                    if (((ISIN,'CPON') in M1_temp.index) == True):
                        M1_temp.loc[(ISIN,'VMOB'),'COUPON_COURU_TOTAL'] = M1_temp.loc[(ISIN,'CPON'),'VALEUR_BOURSIERE'] + M1_temp.loc[(ISIN,'VMOB'),'COUPON_COURU_TOTAL']
                        if M1_temp.loc[(ISIN,'CPON'),'QTEINV_NOMINAL'] < M0.loc[(ISIN,'VMOB'),'QTEINV_NOMINAL']:
                            M1_temp.loc[(ISIN,'VMOB'),'QTEINV_NOMINAL'] = M1_temp.loc[(ISIN,'CPON'),'QTEINV_NOMINAL']
                    else:
                        M1_temp.loc[(ISIN,'VMOB'),'COUPON_COURU_TOTAL'] = M0.loc[(ISIN,'VMOB'),'COUPON_COURU_TOTAL'] + M1_temp.loc[(ISIN,'VMOB'),'COUPON_COURU_TOTAL']
                    #Si sinkable
                    if M1_temp.loc[(ISIN,'VMOB'),'NOMINAL'] != M0.loc[(ISIN,'VMOB'),'NOMINAL']:                    
                        M1_temp.loc[(ISIN,'VMOB'),'NOMINAL'] = M0.loc[(ISIN,'VMOB'),'NOMINAL']
                        
        if len(listOblig) > 0:
            MPerf.loc[listOblig,dt1] = (M1_temp.loc[listOblig,'COUPON_COURU_TOTAL']/(M1_temp.loc[listOblig,'NOMINAL']*M1_temp.loc[listOblig,'QTEINV_NOMINAL'])
                                         -M0.loc[listOblig,'COUPON_COURU_TOTAL']/(M0.loc[listOblig,'NOMINAL']*M0.loc[listOblig,'QTEINV_NOMINAL']))
            MContrib.loc[listOblig,dt1] = MPerf.loc[listOblig,dt1]*M1_temp.loc[listOblig,'PRCT_ACT_NET']
            MPNL.loc[listOblig,dt1] = MPerf.loc[listOblig,dt1]*M1_temp['NOMINAL']*M1_temp['QTEINV_NOMINAL']

        
def Get_list_date(ddeb,dfin,dlist):
    """ Retourne les dates d'une liste comprises entre deux dates """
    lOutput = list()
    for x in dlist:
        if x >= ddeb and x <= dfin:
            lOutput.append(x)
    return lOutput

def Get_Dict_Dates(dtDeb,dtMaxInv):
    """ Retourne le dictionnaires des intitulés de colonnes et des dates
    correspondantes """

    d1M = dtMaxInv + relativedelta(months=-1)
    d3M = dtMaxInv + relativedelta(months=-3)
    dYTD = date(dtMaxInv.year,1,1) + relativedelta(days=-1)
    
    dictDates = {'1M': d1M,
               '3M': d3M,
               'YTD': dYTD,
               'DEB': dtDeb}

    return dictDates

def Agreg_Mat(MRes,dfOutput, dictDates, dtMaxInv,colPrefix):
    """ Agrège les données des matrices en fonction
    des sorties souhaitées et des dates"""
    
    lDates = [x for x in MRes.columns.values]
    
    for key,value in dictDates.items():        
        listDate = Get_list_date(pd.to_datetime(value),pd.to_datetime(dtMaxInv),lDates)
        dfOutput.loc[colPrefix + key,MRes.index] = MRes.loc[:,listDate].sum(axis=1)

def Calc_Perf_Position(dfPrtfs,dfOutput, dictDates, dtMaxInv):
    """ Calcul la performance pour chaque sortie souhaitées """
    
    dfLastDates = dfPrtfs[['CODEISIN','CATV','DINV']].groupby(['CODEISIN','CATV']).max().reset_index()    
    for key,value in dictDates.items():  
        dfFirstDates = dfPrtfs.loc[pd.to_datetime(dfPrtfs['DINV'])<=pd.to_datetime(value),['CODEISIN','CATV','DINV']].groupby(['CODEISIN','CATV']).max().reset_index()
        dfFirstPrices = (pd.merge(dfPrtfs, dfFirstDates , on=['CODEISIN','CATV','DINV'], how='inner')
                    .loc[:,['CODEISIN','CATV','LAST']]
                    .set_index(['CODEISIN','CATV']))
        dfLastPrices = (pd.merge(dfPrtfs, dfLastDates , on=['CODEISIN','CATV','DINV'], how='inner')
                    .loc[:,['CODEISIN','CATV','LAST']]
                    .set_index(['CODEISIN','CATV']))
        dfPerf = dfLastPrices / dfFirstPrices - 1
        
        dfOutput.loc['PERF_POS_' + key,dfPerf.index] = dfPerf['LAST']
        
def Calc_Contrib_Vente(dfOpe,dfPrtf, vDates, listISIN):

    dfOpe.insert(loc=len(dfOpe.columns), column='ACTIF_NET',value=0)
    dfISIN = pd.DataFrame.from_records(listISIN.values, columns=['CODEISIN','CATV'])
    dfISIN = dfISIN[dfISIN['CATV'] != 'CPON']

    #Ajout de la colonne CATV et joint les valeurs
    dfOpe = dfOpe.merge(dfISIN,on=['CODEISIN'])

    dfDtPrtf = pd.DataFrame(pd.to_datetime(vDates))
    dfDtPrtf.columns = ['DINV']
    dfDtPrtf.sort_values('DINV',ascending=False, inplace=True)
    
    #Ajout des dates de portefeuilles inférieures ou égales à
    #la date d'exécution de l'ordre (DATE_EXEC)
    vDatesOpe = dfOpe['DATE_EXEC'].drop_duplicates().sort_values()
    for x in vDatesOpe:
        dtPrtf = dfDtPrtf[dfDtPrtf['DINV']<=x].head(1).iloc[0].values[0]
        dfOpe.loc[dfOpe['DATE_EXEC']==x,'DINV'] = dtPrtf
        dfOpe.loc[dfOpe['DATE_EXEC']==x,'ACTIF_NET'] = dfPrtf.loc[dfPrtf['DINV'] == dtPrtf,'VALEUR_BOURSIERE'].sum()

    dfOpeV = dfOpe[dfOpe['SENS']=='V']
    dfOpeV['QUANTITE_MNT'].fillna(dfOpeV['QUANTITE'], inplace=True)
    dfOpeV['DINV'] = dfOpeV['DINV'].astype('datetime64[ns]')

    #Ajout des prix de valorisation à chaque opération
    is_VMOB = dfPrtf['CATV'].isin(['VMOB','CRNE'])
    dfOpeV = dfOpeV.merge(dfPrtf[is_VMOB].loc[:,['CODEISIN','DINV','LAST','QTEINV_NOMINAL','NOMINAL', 'PRCT_ACT_NET']],on=['CODEISIN','DINV'])
    
    #Calcul les contributions, le PNL et les Frais
    dfOpeV.loc[:,'CONTRIB'] = (dfOpeV['COURS_EXEC'] / dfOpeV['LAST'] - 1)*(dfOpeV['QUANTITE_MNT']/(dfOpeV['QTEINV_NOMINAL']*dfOpeV['NOMINAL']))*dfOpeV['PRCT_ACT_NET']
    dfOpeV.loc[:,'PNL'] = (dfOpeV['COURS_EXEC'] - dfOpeV['LAST'])*(dfOpeV['QTEINV_NOMINAL']*dfOpeV['NOMINAL'])/100
    dfOpe.loc[dfOpe['SUPPORT'].str.contains('FCP'),'FRAIS_TRANSAC_TTC_EUR'] = dfOpe['COM_CIC_RL_HT']
    dfOpe.loc[:,'CTRB_FRAIS'] = dfOpe['FRAIS_TRANSAC_TTC_EUR']/dfOpe['ACTIF_NET']
    
    #Fait la somme par jour et par ISIN des contributions, PNL et Frais
    dfContrib = dfOpeV[['CODEISIN','CATV','DINV','CONTRIB']].groupby(['CODEISIN','CATV','DINV']).sum()
    dfPNL = dfOpeV[['CODEISIN','CATV','DINV','PNL']].groupby(['CODEISIN','CATV','DINV']).sum()
    dfFrais = dfOpe[['CODEISIN','CATV','DINV','FRAIS_TRANSAC_TTC_EUR']].groupby(['CODEISIN','CATV','DINV']).sum()
    dfCtrbFrais = dfOpe[['CODEISIN','CATV','DINV','CTRB_FRAIS']].groupby(['CODEISIN','CATV','DINV']).sum()
    
    #Met les dates en colonne
    dfContrib = dfContrib.unstack(level='DINV')
    dfPNL = dfPNL.unstack(level='DINV')
    dfFrais = dfFrais.unstack(level='DINV')
    dfCtrbFrais = dfCtrbFrais.unstack(level='DINV')
    
    #Supprime le premier niveau multiindice de colonne pour homogénéisé avec les
    #la matrice des output
    dfContrib.columns = dfContrib.columns.droplevel()
    dfPNL.columns = dfPNL.columns.droplevel()
    dfFrais.columns = dfFrais.columns.droplevel()
    dfCtrbFrais.columns = dfCtrbFrais.columns.droplevel()
    
    return dfContrib, dfPNL, dfFrais, dfCtrbFrais

def Lissage(dfContrib, dfPerf, dfPrtfs):
    dfPrctAN = dfPrtfs[['CODEISIN','CATV','DINV','PRCT_ACT_NET']].groupby(['CODEISIN','CATV','DINV']).first()
    dfPrctAN = dfPrctAN.unstack(2)
    dfContrib = dfContrib.fillna(0)
    for nLigne in range(1,len(dfContrib.index)):
        for nCol in range(2,len(dfContrib.columns)-2):
            #Si t0=0 et que t-1<>0 et (t1 ou t2 <> 0)
            if dfContrib.iloc[[nLigne],[nCol]].values[0][0] == 0 and dfContrib.iloc[[nLigne],[nCol-1]].values[0][0] != 0 and (dfContrib.iloc[[nLigne],[nCol+1]].values[0][0] != 0 or dfContrib.iloc[[nLigne],[nCol+1]].values[0][0] != 0):
                dfContrib.iloc[[nLigne],[nCol]] = dfContrib.iloc[[nLigne],:][dfContrib.iloc[[nLigne],:]!=0].T.dropna().median()
                #print(dfContrib.iloc[[nLigne],[nCol]])
    