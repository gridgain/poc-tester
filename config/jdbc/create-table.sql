DROP TABLE if exists Organisation;
DROP TABLE if exists Taxpayers;
create table if not exists Organisation(id bigint, name varchar(50), emplNum bigint, PRIMARY KEY(id)) with "backups={{BACKUPS}}";
create table if not exists Taxpayers(id bigint, org_id bigint, first_name varchar(50), last_name varchar(50), gender varchar(11), state varchar(22), city varchar(30), univ varchar(50), PRIMARY KEY(id, org_id)) with "backups={{BACKUPS}},affinity_key=ORG_ID";
