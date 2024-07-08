M = LOAD '$M' USING PigStorage(',') AS (i:int, j:int, v:double);
N = LOAD '$N' USING PigStorage(',') AS (j:int, k:int, v:double);

K = FOREACH N GENERATE j AS k;
K = DISTINCT K;
MK = CROSS M, K;
MK = FOREACH MK GENERATE i, k, '0' AS tag, j, v;

K = FOREACH M GENERATE i;
K = DISTINCT K;
NK = CROSS N, K;
NK = FOREACH NK GENERATE i, k, '1' AS tag, j, v;

JOIN_D = JOIN MK BY (i, k, j), NK BY (i, k, j);
JOIN_D = FOREACH JOIN_D GENERATE M::i, K::k, (M::v * N::v) as product;
ANS = GROUP JOIN_D BY (i, k);
ANS = FOREACH ANS GENERATE group.i as i, group.k as k, SUM(JOIN_D.product) as sum_product;
STORE ANS INTO '$O' USING PigStorage (',');
