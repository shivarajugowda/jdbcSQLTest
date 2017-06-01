-- File: CREATETABLES.SQL 
-- The parser uses semicolon to delimit SQL Statements.
-- 
create table PART 
 (P_PARTKEY int not null, 
 P_NAME varchar(55) not null, 
 P_MFGR char(25) not null, 
 P_BRAND char(10) not null, 
 P_TYPE varchar(25) not null, 
 P_SIZE int not null, 
 P_CONTAINER char(10) not null, 
 P_RETAILPRICE float not null, 
 P_COMMENT varchar(23) not null ) ;

create table SUPPLIER 
 (S_SUPPKEY int not null, 
 S_NAME char(25) not null, 
 S_ADDRESS varchar(40) not null, 
 S_NATIONKEY int not null, 
 S_PHONE char(15) not null, 
 S_ACCTBAL float not null, 
 S_COMMENT varchar(101) not null) ;

create table PARTSUPP 
 (PS_PARTKEY int not null, 
 PS_SUPPKEY int not null, 
 PS_AVAILQTY int not null, 
 PS_SUPPLYCOST float not null, 
 PS_COMMENT varchar(199) not null) ;

create table CUSTOMER 
 (C_CUSTKEY int not null, 
 C_NAME varchar(25) not null, 
 C_ADDRESS varchar(40) not null, 
 C_NATIONKEY int not null, 
 C_PHONE char(15) not null, 
 C_ACCTBAL float not null, 
 C_MKTSEGMENT char(10) not null, 
 C_COMMENT varchar(117) not null) ;
 
create table ORDERS 
 (O_ORDERKEY bigint not null, 
 O_CUSTKEY int not null, 
 O_ORDERSTATUS char(1) not null, 
 O_TOTALPRICE float not null, 
 O_ORDERDATE date not null, 
 O_ORDERPRIORITY char(15) not null, 
 O_CLERK char(15) not null, 
 O_SHIPPRIORITY int not null, 
 O_COMMENT varchar(79) not null) ;

create table LINEITEM 
 (L_ORDERKEY bigint not null, 
 L_PARTKEY int not null, 
 L_SUPPKEY int not null, 
 L_LINENUMBER int not null, 
 L_QUANTITY float not null, 
 L_EXTENDEDPRICE float not null, 
 L_DISCOUNT float not null, 
 L_TAX float not null, 
 L_RETURNFLAG char(1) not null, 
 L_LINESTATUS char(1) not null, 
 L_SHIPDATE date not null, 
 L_COMMITDATE date not null, 
 L_RECEIPTDATE date not null, 
 L_SHIPINSTRUCT char(25) not null, 
 L_SHIPMODE char(10) not null, 
 L_COMMENT varchar(44) not null) ;

create table NATION 
 (N_NATIONKEY int not null, 
 N_NAME char(25) not null, 
 N_REGIONKEY int not null, 
 N_COMMENT varchar(152) not null) ;

create table REGION 
 (R_REGIONKEY int not null, 
 R_NAME char(25) not null, 
 R_COMMENT varchar(152) not null) ;


