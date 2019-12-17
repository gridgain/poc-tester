DROP TABLE if exists Balance;
create table if not exists Balance(id bigint, balance bigint, PRIMARY KEY(id)) WITH "atomicity=TRANSACTIONAL_SNAPSHOT,CACHE_NAME=balanceCache,backups={{BACKUPS}}";
create table if not exists msgTable(flag varchar(20), val integer, PRIMARY KEY(flag)) WITH "TEMPLATE=REPLICATED";
