# CBDE-FIB
## NOTES
### AUTOESTUDI 1
6,75/10  
Comentaris: L'explicació de disjoitness and completeness per les fragmentacions horitzontals no justifica perquè garanteix que totes les tuples estan assignades a un i només un fragment.

### AUTOESTUDI 2
9/10  
Comentaris: Els predicats finals es poden simplificar més.

### LABORATORI 1
8/10  
Comentaris: Q1: 0,75 (ok, però hi ha més diferències que no sols els constructors) Q2: 0,5 (FK?) Q3: 1 Q4: 1 Q5: 1 Q6: 1 Q7: 1 (tot i que la frase "independent del llenguatge de programació no és ben bé certa a les OODBMS) Q8: 0,25 (ok, però hi ha d'altres raons tècniques) Q9: 0,75 (falta alguna) Q10: 0,75 (falta una mica de detall)

### LABORATORI 2
4,5/10  
Comentaris: Q1, (0,5 sobre 4): com no cal no cal ni fer UNDO ni REDO, no es necessita log enlloc. Emprar log al commit -1. Emprar el log al abort -2. A part: 1) al ser no steal, el read i el write han de fer pin(p):=1 i en el cas del write, a més, dirty(p):=1. -1 si no s'ha fet o es fa dins de l'if. 2) el commit ha de fer flush de totes les pàgines dirty de la tx. -2 si no heu fet flush de les pàgines. 3) l'abort no fa res, només, posa el pin(p):=0 per a totes les pàgines de la tx abortada. -1 si us heu oblidat de desfer el pin. A l'apartat b) calia dir que aquest sistema genera moltes escriptures i necessita una memòria potencialment infinita. -0,5 per cada criteri no esmentat. -1 si no s'explica cap sistema on tingui sentit. Q2, (3 sobre 3): Al validation del commit de T2: RS(T2): {E,C} WS(T2): {C} Set of committed transactions: {NULL} Al validation del commit de T1: RS(T1): {A,F,D,E} WS(T1): {A,E} Set of committed transactions: {T2} No llegeix cap grànul que hagi escrit algú abans (WS(T2) és disjunt amb RS(T1)) Al validation del commit de T3: RS(T3): {A,F} WS(T3): {A,F} Set of committed transactions: {T1,T2} T3 aborta, ja que (WS(T1) intersecció RS(T3): {A}) Q3, (1 sobre 3): Al TS:70, primer conflicte potencial. S'assigna TS tal que TS(T1) < TS(T3) Al TS:120, possible conflicte. TS(T2) < TS(T1). És a dir, es crea un ordre parcial T2 < T1 < T3 Al TS:150, T2 aborta, ja estan els TS assignats i aplicant l'algoritme d'escriptura de TS la fa abortar

### LABORATORI 3
10/10  
Comentaris: Excel·lent!!

### LABORATORI 4
7,75/10  
Comentaris: Aggregation: Combine should be different from reduce, since Avg is non-associative. Join: Join attribute should be used as key in the output of map.

### LABORATORI 5
8,15/10  
Comentaris:  
[Document]
Inserts/Updates: There is no discussion on the impact of inserts/updates
Queries/Optimizations: There is no discussion on optimizations (e.g., indexes)  
[Code]
Insertions: No massive inserts for testing volume

### LABORATORI 6
8,1/10  
Comentaris:  
[Document]
Design: Very good!
Optimizations/Indexes: OK!  
[Code]
Insertions: No massive inserts to test volume
Q1: OK!
Q2: Incorrect. "p_partkey = ps_partkey" is not evaluated; "p" comes from the external query and "ps" from the internal
Q3: OK!
Q4: Incorrect. "c_nationkey = s_nationkey" is not evaluated
