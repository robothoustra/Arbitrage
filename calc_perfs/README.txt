type de calculs :
	perf d'un portefeuille entre deux dates (avec tous les portefeuilles entre)
		=> voir pour créer une matrice de cours à partir des cours dans les portefeuilles comptables
		=> Il faut créer un vecteur contenant tous les instrus utilisé entre les deux date.
		et calculer leur performance :
		Input :
			Col x : TICKER
			Col x : COURS
			Col x : POIDS
			Col x : DATE
		Ouput :
			Col x : TICKER
			Col x : PERF
			Col x : Rendement 
			
	perf d'instruments entre deux dates
	
_____________________________________________________________

Nouvelle reflexion :

Ouput calc_perf:
	
	Calcul du PNL :
		PNL Portage (coupon) = Q*CCN*t
		PNL Position = Q*P-Q*PRU
		PNL Vente = SUM(Qv*Pv-Qv*Pvalo)

		OU

		PNL Portage (coupon) = Q*CCN*t
		PNL Position = SUM(Pv*Qv)-SUM(Pa*Qa)+Q*Pvalo

	Calcul du total return (base journalière) :
		(Voir définition Bloom)

*pour le CCN il faut le déduire à partir du nominal du titre, du cc total et de la fréquence de tombée

=> nécessite 
	-l'historique des opérations (tbordremnt + tbordre + histo_blotter) (ou TSDC)
	-l'historique des portefeuilles

Le PRU (en prix) doit être recalculé à partir de l'historique des opérations.

Colonne du fichier de portefeuille : 
DINV, LIBINV, CODEISIN, CATV, TYPEVAL, QTEINV_NOMINAL, LAST, FREQCOUP, CCN

Colonne du fichier (dataframe) des opérations :
DATE_OPE, LIBINV, CODEISIN, LIBINST, CATV, TYPEVAL, SENS, PRIX, QTE_NOMINAL

___________________________________________________________________________________________
BLOOM :

General Methodology: In general, if the holdings of the portfolio are unchanged over the course of an analysis period extending from time (0) to time (T), R (return) can be defined simply as:
R = Value Portfolio (T) / Value Portfolio (0)

Alternatively, if the return of each security in the portfolio over the analysis period is defined as ri(T) and the weight of each security at the beginning of the analysis period is defined as wi(T-1), then R, the return of the portfolio is also equivalent to:
R = w1(T-1) * r1(T) + w2(T-1) * r2(T) + … + wi(T-1) * ri(T)

However, since the portfolio holdings do change over a typical analysis period (e.g. year to date), this simplified view of returns only holds true for the sub-period (t) where the holdings remain unchanged. Hence the returns over the period (T) covering sub-periods t1 through tn is defined as:
R = R(t1) * R(t2) * R(t3) * … * R(tn)

___________________________________________________________________________________________
Présuposés :

- les poids doivent être en valeur décimal, pas en %

