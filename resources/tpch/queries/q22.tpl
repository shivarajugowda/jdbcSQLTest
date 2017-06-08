-- TPC_H Query 22 - Global Sales Opportunity 
-- answers/q22.out
SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_ACCTBAL) AS TOTACCTBAL
FROM (SELECT SUBSTR(C_PHONE,1,2) AS CNTRYCODE, C_ACCTBAL
 FROM CUSTOMER WHERE SUBSTR(C_PHONE,1,2) IN ('13', '31', '23', '29', '30', '18', '17') AND
 C_ACCTBAL > (SELECT AVG(C_ACCTBAL) FROM CUSTOMER WHERE C_ACCTBAL > 0.00 AND
  SUBSTR(C_PHONE,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) AND
 NOT EXISTS ( SELECT * FROM ORDERS WHERE O_CUSTKEY = C_CUSTKEY)) CUSTSALE
GROUP BY CNTRYCODE
ORDER BY CNTRYCODE 
