DROP TABLE if exists Balance;
create table if not exists Balance(id bigint, balance bigint, PRIMARY KEY(id)) WITH "VALUE_TYPE=balance, CACHE_NAME=balanceCache, TEMPLATE=balanceCacheTemplate";
create table if not exists msgTable(flag varchar(20), val integer, PRIMARY KEY(flag)) WITH "TEMPLATE=REPLICATED";
