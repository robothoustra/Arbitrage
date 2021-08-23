# -*- coding: utf-8 -*-
import argparse
import pandas as pd
from dateutil.rrule import rrulestr, rrule
from dateutil.parser import parse
from datetime import datetime
import utils
import numpy as np
import csv

def load_arguments(**kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument("-hp", "--histo_prtfs", help="Chemin vers le fichier de l'historique des portefeuilles", type=str, default=r'./input/HISTO_PRTFS_20191231_20201231.csv')
    parser.add_argument("-ho", "--histo_ope", help="Chemin vers le fichier de l'historique des opérations", type=str, default=r'./input/HISTO_OPE_20191231_20201231.csv')
    parser.add_argument("-dd", "--date_debut", help="Date de début du calcul des performances", type=str, default='31/12/2019')
    parser.add_argument("-df", "--date_fin", help="Date de fin du calcul des performances", type=str, default='31/12/2020')
                
    return parser
    
def Calc_Perf(**kwargs):
    
    parser = load_arguments(**kwargs)
    args = parser.parse_args()

    #Si les portefeuilles ne sont pas passés en argument alors on les récupères dans le fichier csv 
    dfPrtfs = kwargs.get('Portefeuilles',pd.DataFrame())
    if dfPrtfs.empty:
        dfPrtfs = pd.read_csv(args.histo_prtfs,header=[0], sep=';', parse_dates=['DINV'])

    #Si les opérations ne sont pas passées en argument alors on les récupères dans le fichier csv 
    dfOpe = kwargs.get('Operations',pd.DataFrame())
    if dfOpe.empty:
        dfOpe = pd.read_csv(args.histo_ope,header=[0], sep=';', parse_dates=['DATE_EXEC'])
        
    dtDeb = datetime.strptime(args.date_debut,'%d/%m/%Y') 
    dtFin = datetime.strptime(args.date_fin,'%d/%m/%Y') 
    
    #récupère l'ensemble des dates des portefeuilles
    dfPrtfs.sort_values(by=['DINV'],ascending=True, inplace=True)
    dtPrtfs = [x.astype('M8[ms]').astype('O') for x in dfPrtfs['DINV'].unique() if x.astype('M8[ms]').astype('O')>=dtDeb and x.astype('M8[ms]').astype('O')<=dtFin]    
    vDates = list(dtPrtfs)
    vDates.sort()

    #récupère l'ensemble des Code ISIN et des CATV des portefeuilles
    listISIN = dfPrtfs.loc[:,['CODEISIN','CATV']].groupby(['CODEISIN','CATV']).first().index
    
    #Initialisation des matrices de résultats (perfs et PNL)
    dfPerfPortage = pd.DataFrame(0,index=listISIN, columns=vDates)
    dfContribPortage = pd.DataFrame(0,index=listISIN, columns=vDates)
    dfPNLPortage = pd.DataFrame(0,index=listISIN, columns=vDates)
    dfContribPosition = pd.DataFrame(0,index=listISIN, columns=vDates)
    dfPNLPosition = pd.DataFrame(0,index=listISIN, columns=vDates)
    dfContribVente = pd.DataFrame(0,index=listISIN, columns=vDates)
    dfPNLVente = pd.DataFrame(0,index=listISIN, columns=vDates)
    dfFrais = pd.DataFrame(0,index=listISIN, columns=vDates)
    dfCtrbFrais = pd.DataFrame(0,index=listISIN, columns=vDates)
    
    #Colonnes du fichier de sortie    
    dfColSortie = ('PERF_POS_1M','PERF_POS_3M','PERF_POS_YTD','PERF_POS_DEB',
                   'CTRB_POS_1M','CTRB_POS_3M','CTRB_POS_YTD','CTRB_POS_DEB',
                   'CTRB_PORT_1M','CTRB_PORT_3M','CTRB_PORT_YTD','CTRB_PORT_DEB',
                   'PERF_PORT_1M','PERF_PORT_3M','PERF_PORT_YTD','PERF_PORT_DEB',
                   'PNL_PORT_1M','PNL_PORT_3M','PNL_PORT_YTD','PNL_PORT_DEB',
                   'CTRB_VENTE_1M','CTRB_VENTE_3M','CTRB_VENTE_YTD','CTRB_VENTE_DEB',
                   'PNL_VENTE_1M','PNL_VENTE_3M','PNL_VENTE_YTD','PNL_VENTE_DEB',
                   'FRAIS_1M','FRAIS_3M','FRAIS_YTD','FRAIS_DEB',
                   'CTRB_FRAIS_1M','CTRB_FRAIS_3M','CTRB_FRAIS_YTD','CTRB_FRAIS_DEB')

    #Initialisation de la matrice de sortie
    dfOutput = pd.DataFrame(0, index=dfColSortie, columns=listISIN)
    
    #Date du dernier inventaire
    dtMaxInv = pd.DataFrame(dtPrtfs).max().loc[0].to_pydatetime()
    
    #Dictionnaire des dates (utile pour les colonnes du fichier de sortie)
    dictDates = utils.Get_Dict_Dates(dtDeb, dtMaxInv)
    
    #boucle sur l'ensemble des dates de portefeuilles
    #pour remplir les matrices de contributions et de performances
    for dtCalc in vDates:
        dfCurPrtf = dfPrtfs.loc[dfPrtfs['DINV']==dtCalc,:]
        if dtCalc == min(vDates):
            M0 = (dfCurPrtf.loc[:,['CODEISIN','CATV','TYPEVAL','LAST','PRCT_ACT_NET','NOMINAL',
                                 'VALEUR_BOURSIERE','QTEINV_NOMINAL','COUPON_COURU_TOTAL',
                                 'NBJ_NEXT_CPN','D_T_CPN','C_D','CODE_CTR','PRU','ACTIF_NET']]
                      .set_index(['CODEISIN','CATV']))
            dt0 = dtCalc
        else:
            M1 = (dfCurPrtf.loc[:,['CODEISIN','CATV','TYPEVAL','LAST','PRCT_ACT_NET','NOMINAL',
                                 'VALEUR_BOURSIERE','QTEINV_NOMINAL','COUPON_COURU_TOTAL',
                                 'NBJ_NEXT_CPN','D_T_CPN','C_D','CODE_CTR','PRU','ACTIF_NET']]
                      .set_index(['CODEISIN','CATV']))
            dt1 = dtCalc
            
            utils.Calc_Contrib_Position(M0,M1,dfContribPosition,dt0)
            utils.Calc_Portage(M0,M1,dt0,dt1,dfPerfPortage, dfContribPortage,dfPNLPortage)
            
            M0 = M1
            dt0 = dt1
    
    #lissage portage #################  FINIR LE LISSAGE ##################
    utils.Lissage(dfContribPortage, dfPerfPortage, dfPrtfs)
    
    #Calcul les performances
    utils.Calc_Perf_Position(dfPrtfs,dfOutput, dictDates, dtMaxInv)
    
    #Calcul la contribution et le PNL des ventes
    dfContribVente, dfPNLVente, dfFrais, dfCtrbFrais = utils.Calc_Contrib_Vente(dfOpe, dfPrtfs, vDates,listISIN)
    
    #Agregation des matrices de contribution et de performance
    utils.Agreg_Mat(dfContribPosition,dfOutput,dictDates,dtMaxInv,'CTRB_POS_')
    utils.Agreg_Mat(dfContribPortage,dfOutput,dictDates,dtMaxInv,'CTRB_PORT_')
    utils.Agreg_Mat(dfPerfPortage,dfOutput,dictDates,dtMaxInv,'PERF_PORT_')
    utils.Agreg_Mat(dfPNLPortage,dfOutput,dictDates,dtMaxInv,'PNL_PORT_')
    utils.Agreg_Mat(dfContribVente,dfOutput,dictDates,dtMaxInv,'CTRB_VENTE_')
    utils.Agreg_Mat(dfPNLVente,dfOutput,dictDates,dtMaxInv,'PNL_VENTE_')
    utils.Agreg_Mat(dfFrais,dfOutput,dictDates,dtMaxInv,'FRAIS_')
    utils.Agreg_Mat(dfCtrbFrais,dfOutput,dictDates,dtMaxInv,'CTRB_FRAIS_')

    #Enleve les ISIN si toutes lignes sont égales à 0
    #dfOutput = dfOutput.loc[:,dfOutput.sum() != 0]

    #Inverse colonnes et lignes
    dfOutput = dfOutput.T
    
    #Remplace les valeur "inf" (limite infinie) par 0
    dfOutput.replace([np.inf, -np.inf], 0,inplace=True)
    
    #Join les devises
    devises = dfPrtfs.loc[:,['CODEISIN','CATV','C_D']].groupby(['CODEISIN','CATV']).first()
    dfOutput = dfOutput.join(pd.DataFrame(devises))
    dfContribPosition = dfContribPosition.join(pd.DataFrame(devises))
    
    listColContrib = [x for x in dfColSortie if ('CTRB' in x) & ('DEB' in x)]
    print(dfOutput[listColContrib].sum())

    listColPNL = [x for x in dfColSortie if ('PNL' in x) & ('DEB' in x)]
    print(dfOutput[listColPNL].sum())
    
    #Sauvegarde en CSV la sortie
    dfPerfPortage.to_csv('./output/perf_port.csv',sep=';', float_format='%.10f', decimal=',')
    dfContribPosition.to_csv('./output/ctrb_pos.csv',sep=';', float_format='%.10f', decimal=',')
    dfContribPortage.to_csv('./output/ctrb_port.csv',sep=';', float_format='%.10f', decimal=',')
    dfOutput.to_csv('./output/test.csv',sep=';', float_format='%.10f', decimal=',')

    return dfOutput
            
if __name__ == '__main__':
    Calc_Perf()