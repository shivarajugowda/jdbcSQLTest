# jdbcSQLTest
A JDBC based SQL test suite to sanity test a Database. The idea is to levarage Open Source test suites in a consistent way to quickly sanity test a Database using JDBC. The test suite is implemented to be generic instead of tailoring it to one particular database. HSQLDB, Postgres and Oracle are the databases used in the development of these tests. The test suite is mainly aimed for checking correctness. However, the TPC* test suite could easily be used to test performance by increasing the scale factor. The code and the datasets are easier to tweak for different use cases.

The unit test under tests folder is the best way to get started with the tests.


The following test suites are implemented until now.
* Foodmart
* Sqllogictest
* NIST
* TPCH
* TPC-DS

NIST is still in preliminary stage of implementation. For TPC* test suites a scale factor of 0.01 can be used to sanity test the environment before scaling it to a scalefactor of 1 or more.
Each test suite comes with an answer set which is used for validation of the test. 
