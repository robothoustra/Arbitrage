import mysql.connector
from mysql.connector import errorcode
from datetime import datetime, date
import pandas as pd
import numpy

def connect_db_hg():
    try:
        config = {
            'user': 'guest',
            'password': 'HugauDB2019_Guest',
            'host': '192.168.9.41',
            'database': 'mydb',
            'raise_on_warnings': True
          }

        cnx = mysql.connector.connect(**config)

    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)
    else:
      return cnx    

def Get_Recordset_From_DB_HG(strSQL, dictParam, saveToCSV,*args, **kwargs):
    #dateDeb et dateFin au format ISO (aaaa-mm-jj)
    
    db_hg = connect_db_hg()
    if db_hg.is_connected():
        db_Info = db_hg.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        
        cur = db_hg.cursor()
        
        cur.execute(strSQL, dictParam)
        dfOutput = pd.DataFrame(cur.fetchall(), columns=cur.column_names)
        if saveToCSV == True:
            strFileName = kwargs.get('FileName','nothing')
            dfOutput.to_csv('./input/' + strFileName, index=False, sep=';')

        db_hg.close()
        
        return dfOutput
    else:
        print("Connection impossible")

def Download_Histo_Ope(dateDeb, dateFin,strFonds, saveToCSV):
    dictParam = {'dateDeb': datetime.strptime(dateDeb,'%Y-%m-%d'),
                 'dateFin': datetime.strptime(dateFin,'%Y-%m-%d'),
                 'strFonds': strFonds}
    
    strSQL = ("SELECT LIBELLE_CLIENT,  "
        "	CODE_PRODUIT,  "
        "    SUPPORT,  "
        "    CODE_VALEUR AS CODEISIN,  "
        "    LIBELLE_VALEUR,  "
        "    SENS,  "
        "    QUANTITE,  "
        "    QUANTITE_MNT,  "
        "    LIBDEV,  "
        "    DATE_EXEC,  "
        "    COURS_EXEC,  "
        "    TX_CONV_RGLT,  "
        "    MNT_BRUT_EUR,  "
        "    MNT_NET_EUR , "
        "    CC, "
        "    FRAIS_TRANSAC_TTC_EUR, "
        "    COM_CIC_RL_HT "
        "FROM mydb.histo_tsdc  "
        "WHERE DATE_INTEGRATION >= %(dateDeb)s AND DATE_INTEGRATION <= %(dateFin)s "
        "AND LIBELLE_CLIENT LIKE %(strFonds)s AND INDIC_ANNULATION<>'A'")
    
    strFileName = 'HISTO_OPE_' + dateDeb.replace('-','') + '_' + dateFin.replace('-','') + '.csv'
    
    return Get_Recordset_From_DB_HG(strSQL, dictParam, saveToCSV, FileName=strFileName)

def Download_Histo_PRTF(dateDeb, dateFin, strFonds, saveToCSV):
    dictParam = {'dateDeb': datetime.strptime(dateDeb,'%Y-%m-%d'),
                 'dateFin': datetime.strptime(dateFin,'%Y-%m-%d'),
                 'strFonds': strFonds}
    
    strSQL = ("SELECT  "
        "	 DATE(DINV) AS DINV, "
        "	 xinv.LIBINV, "
        "    xinv.CODEISIN, "
        "    xinv.LIBINST, "
        "    C_D, "
        "    IF(CODE_CTR='',CATV,'REPO') AS CATV, "
        "    xinv.TYPEVAL, "
        "    xinv.CODE_CTR, "
        "    xinv.QTEINV_NOMINAL, "
        "    xinv.LAST, "
        "    xinv.PRU, "
        "    VALEUR_BOURSIERE, "
        "    VL_BOURSIERE_DG, "
        "    COUPON_COURU_TOTAL, "
        "    IF(D_T_CPN<>'',DATEDIFF(STR_TO_DATE(D_T_CPN,'%Y%m%d'),DINV),0) AS NBJ_NEXT_CPN,"
        "    STR_TO_DATE(D_T_CPN,'%Y%m%d') AS D_T_CPN,"
        "    (VALEUR_BOURSIERE / ACTIF_NET) AS PRCT_ACT_NET, "
        "    xinv.NOMINAL, "
        "    ACTIF_NET "
        "FROM ( "
        "        ("
        "            SELECT DINV, FICHIER_DE_TRAME, E, LIBINV, LIBELLE_PORT_C, CATV, CODEISIN, LIB_INSTRUMENT, LIBINST, C_D, CDG, C_DEPO, M_TRI, CRIT_TRI, TRI_CLE_1, TYPEVAL, TRI_CLE_3, TRI_CLE_4, TRI_CLE_5, TRI_CLE_6, CL_V, O_ALPHA, ST_VAL, ST_LGN, CODE_CTR, QTEINV_NOMINAL, EXP_QTE, EXP_COURS, LAST, F, PRU, PRIX_REVIENT_TOTAL, SUM(VALEUR_BOURSIERE) AS VALEUR_BOURSIERE, SUM(COUPON_COURU_TOTAL) AS COUPON_COURU_TOTAL, SUM(PLUS_OU_MOINS_VALUE) AS PLUS_OU_MOINS_VALUE, D_COT, SUM(PRCT_ACT_NET) AS PRCT_ACT_NET, CRIT_TRI2, CRIT_TRI3, CRIT_TRI4, TRI_PERS1, TRI_PERS2, TRI_PERS3, TRI_PERS4, C_GER, FACTEUR, ST_P, C_GES, PX_REVIENT_DG, SUM(VL_BOURSIERE_DG) AS VL_BOURSIERE_DG, TYVL, VBCE, C_EMET, C_SERV, NOMINAL, D_T_CPN, D_EMIS, TAUX_REF, VAL_TX, A, INT_, COT, LIBELLE_PLACE_COTATION, CODE_BLOOMBERG, NOMINAL2, C_CONTRP, DATE_CHARGEMENT "
        "            FROM mydb.histo_prtfs_new "
        "            WHERE NOT CATV='CAT' "
        "                AND DINV >= %(dateDeb)s AND DINV<= %(dateFin)s "
        "                AND LIBINV=( "
        "                    SELECT Code_Fonds_CMCIC "
        "                    FROM automation.fonds_description "
        "                    WHERE Nom_Court LIKE %(strFonds)s LIMIT 1 "
        "                    )"
        "            GROUP BY DINV,CODEISIN,CATV"
        "        ) "
        "        UNION ALL "
        "        ("
        "            SELECT DINV, FICHIER_DE_TRAME, E, LIBINV, LIBELLE_PORT_C, CATV, CODEISIN, LIB_INSTRUMENT, LIBINST, REPLACE(REPLACE(GROUP_CONCAT(C_D),'EUR',''),',','') AS C_D, CDG, C_DEPO, M_TRI, CRIT_TRI, TRI_CLE_1, TYPEVAL, TRI_CLE_3, TRI_CLE_4, TRI_CLE_5, TRI_CLE_6, CL_V, O_ALPHA, ST_VAL, ST_LGN, CODE_CTR, QTEINV_NOMINAL, EXP_QTE, EXP_COURS, ROUND(SUM(LAST)-1,4) AS LAST, F, PRU, PRIX_REVIENT_TOTAL, SUM(VALEUR_BOURSIERE) AS VALEUR_BOURSIERE, COUPON_COURU_TOTAL, PLUS_OU_MOINS_VALUE, D_COT, MIN(PRCT_ACT_NET) AS PRCT_ACT_NET, CRIT_TRI2, CRIT_TRI3, CRIT_TRI4, TRI_PERS1, TRI_PERS2, TRI_PERS3, TRI_PERS4, C_GER, FACTEUR, ST_P, C_GES, PX_REVIENT_DG, VL_BOURSIERE_DG, TYVL, VBCE, C_EMET, C_SERV, NOMINAL, D_T_CPN, D_EMIS, TAUX_REF, VAL_TX, A, INT_, COT, LIBELLE_PLACE_COTATION, CODE_BLOOMBERG, NOMINAL2, C_CONTRP, DATE_CHARGEMENT "
        "            FROM ("
        "                SELECT * FROM mydb.histo_prtfs_new "
        "                WHERE CATV='CAT' AND DINV >= %(dateDeb)s AND DINV<= %(dateFin)s "
        "                    AND LIBINV=( "
        "                        SELECT Code_Fonds_CMCIC "
        "                        FROM automation.fonds_description "
        "                        WHERE Nom_Court LIKE %(strFonds)s LIMIT 1 "
        "                    )"
        "            ) as xcat "
        "            GROUP BY DINV, CODEISIN "
        "        ) "
        "    )AS xinv "
        "LEFT JOIN ( "
        "        SELECT DINV, SUM(VALEUR_BOURSIERE) AS ACTIF_NET "
        "        FROM mydb.histo_prtfs_new "
        "        WHERE DINV >= %(dateDeb)s AND DINV<= %(dateFin)s "
        "            AND LIBINV=( "
        "                SELECT Code_Fonds_CMCIC "
        "                FROM automation.fonds_description "
        "                WHERE Nom_Court LIKE %(strFonds)s LIMIT 1 "
        "                )"
        "        GROUP BY DINV, LIBINV) AS xact USING (DINV)"
        "ORDER BY DINV ASC")
    
    strFileName = 'HISTO_PRTFS_' + dateDeb.replace('-','') + '_' + dateFin.replace('-','') + '.csv'
    
    dfPRTF = Get_Recordset_From_DB_HG(strSQL, dictParam, False, FileName=strFileName)
    
    Recalc_Nominaux(dfPRTF)
    #Calc_CCNJ(dfPRTF)
    #dfPRTF['PRCT_ACT_NET'] = dfPRTF['PRCT_ACT_NET']/100
    
    if saveToCSV == True:
        #dfPRTF['PRCT_ACT_NET'] = dfPRTF['PRCT_ACT_NET'].map(lambda x: '%.15f' % x if numpy.isnan(x) == False else x)
        dfPRTF.to_csv('./input/' + strFileName, index=False, sep=';')
    
    return dfPRTF

def Recalc_Nominaux(dfPrtf):
    """ Recalul les nominaux pour les VMOB => possibilité d'avoir des sinkable """
    ########### VERIFIER LES DONNEES ############
    dfPrtf.loc[(dfPrtf['NOMINAL']>1) & (dfPrtf['CATV']=='VMOB'),'NOMINAL']=round((dfPrtf['VALEUR_BOURSIERE']-dfPrtf['COUPON_COURU_TOTAL'])/(dfPrtf['LAST']*dfPrtf['QTEINV_NOMINAL']*dfPrtf['VALEUR_BOURSIERE']/dfPrtf['VL_BOURSIERE_DG'])*100)

def Calc_CCNJ(dfPrtf):
    """ Calcul le coupon nominal journalier"""
    dfPrtf.loc[(dfPrtf['TYPEVAL']=='P1'),'CCNJ']= dfPrtf['COUPON_COURU_TOTAL']/(dfPrtf['NOMINAL']*dfPrtf['QTEINV_NOMINAL']*(365,25/dfPrtf['FREQCOUP']-dfPrtf['NBJ_NEXT_CPN']))
    
if __name__ == '__main__':
    #Download_Histo_Ope('2019-12-31','2020-11-30','FCP HUGAU OBLI 1 3',True)
    #Download_Histo_PRTF('2019-12-31','2020-11-30','HO13',True)
    #Download_Histo_Ope('2019-12-31','2020-08-31','FCP HUGAU OBLI 3 5',True)
    #Download_Histo_PRTF('2019-12-31','2020-08-31','HO35',True)
    Download_Histo_Ope('2019-12-31','2020-12-31','FCP HUGAU MONETERME',True)
    Download_Histo_PRTF('2019-12-31','2020-12-31','HMON',True)
    
    # OLD REQUEST :
        
# =============================================================================
#             strSQL = ("SELECT  "
#         "	 DATE(DINV) AS DINV, "
#         "	 xinv.LIBINV, "
#         "    xinv.CODEISIN, "
#         "    xinv.LIBINST, "
#         "    C_D, "
#         "    CATV, "
#         "    xinv.TYPEVAL, "
#         "    xinv.QTEINV_NOMINAL, "
#         "    xinv.LAST, "
#         "    xinv.PRU, "
#         "    IF(CATV='CAT',VALEUR_BOURSIERE_CAT,VALEUR_BOURSIERE) AS VALEUR_BOURSIERE, "
#         "    VL_BOURSIERE_DG, "
#         "    COUPON_COURU_TOTAL, "
#         "    IF(D_T_CPN<>'',DATEDIFF(STR_TO_DATE(D_T_CPN,'%Y%m%d'),DINV),0) AS NBJ_NEXT_CPN,"
#         "    STR_TO_DATE(D_T_CPN,'%Y%m%d') AS D_T_CPN,"
#         "    IF(CATV='CAT',PRCT_ACT_NET_CAT,PRCT_ACT_NET) AS PRCT_ACT_NET, "
#         "    xinv.NOMINAL "
#         "FROM ( "
#         "        ( "
#         "            SELECT *,0 AS VALEUR_BOURSIERE_CAT, 0 AS PRCT_ACT_NET_CAT "
#         "            FROM mydb.histo_prtfs_new "
#         "            WHERE NOT CATV='CAT' "
#         "                AND DINV >= %(dateDeb)s AND DINV<= %(dateFin)s "
#         "                AND LIBINV=( "
#         "                    SELECT Code_Fonds_CMCIC "
#         "                    FROM automation.fonds_description "
#         "                    WHERE Nom_Court LIKE %(strFonds)s LIMIT 1 "
#         "                    )"        
#         "        ) "
#         "        UNION ALL "
#         "        ( "
#         "            SELECT *,SUM(VALEUR_BOURSIERE) AS VALEUR_BOURSIERE_CAT,SUM(PRCT_ACT_NET) AS PRCT_ACT_NET_CAT  "
#         "            FROM mydb.histo_prtfs_new"
#         "            WHERE CATV='CAT' "
#         "                AND DINV >= %(dateDeb)s AND DINV<= %(dateFin)s "
#         "                AND LIBINV=( "
#         "                    SELECT Code_Fonds_CMCIC "
#         "                    FROM automation.fonds_description "
#         "                    WHERE Nom_Court LIKE %(strFonds)s LIMIT 1 "
#         "                    )"    
#         "            GROUP BY CODEISIN, DINV "
#         "        ) "
#         "    )AS xinv "
#         "ORDER BY DINV ASC")
#             
# =============================================================================
# =============================================================================
#     strSQL = ("SELECT  "
#         "	 DATE(DINV) AS DINV, "
#         "	 xinv.LIBINV, "
#         "    xinv.CODEISIN, "
#         "    xinv.LIBINST, "
#         "    C_D, "
#         "    CATV, "
#         "    xinv.TYPEVAL, "
#         "    xinv.QTEINV_NOMINAL, "
#         "    xinv.LAST, "
#         "    xinv.PRU, "
#         "    VALEUR_BOURSIERE, "
#         "    VL_BOURSIERE_DG, "
#         "    COUPON_COURU_TOTAL, "
#         "    IF(D_T_CPN<>'',DATEDIFF(STR_TO_DATE(D_T_CPN,'%Y%m%d'),DINV),0) AS NBJ_NEXT_CPN,"
#         "    STR_TO_DATE(D_T_CPN,'%Y%m%d') AS D_T_CPN,"
#         "    PRCT_ACT_NET, "
#         "    xinv.NOMINAL "
#         "FROM "
#         "    mydb.histo_prtfs_new AS xinv "
#         "WHERE "
#         "    DINV >= %(dateDeb)s AND DINV<= %(dateFin)s AND NOT (CATV LIKE 'CAT%' AND C_D='EUR')"
#         "    AND LIBINV=(SELECT Code_Fonds_CMCIC "
#         "        FROM automation.fonds_description "
#         "        WHERE Nom_Court LIKE %(strFonds)s LIMIT 1)")
# =============================================================================
