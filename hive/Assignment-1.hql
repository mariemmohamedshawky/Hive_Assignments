create database assign1_mariem;
describe database assign1_mariem;
create database assign1_loc location '/hp_db/ assign1_loc';
create table if not exists assign1_intern_tab( eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n' ;
describe formatted assign1_intern_tab ;
load data local inpath 'employee.csv' into table assign1_intern_tab ;
!hadoop fs  -mkdir /employee_data;
!hadoop fs  -put employee.csv /employee_data;
load data inpath '/employee_data/employee.csv' into table assign1_intern_tab ;
select * from assign1_intern_tab 
limit 10;
use assign1_loc;
create external table if not exists external_assign1_intern_tab (
  eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 'hdfs://namenode:8020/employee_data';
describe formatted external_assign1_intern_tab;
!hadoop fs  -put employee.csv /employee_data;
drop table assign1_intern_tab ;   
drop table external _assign1_intern_tab;  
use assign1_mariem;
create table if not exists assign1_intern_tab( eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n' ;
use assign1_loc;
create external table if not exists external_assign1_intern_tab (
  eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 'hdfs://namenode:8020/employee_data';
describe formatted assign1_intern_tab;
describe formatted external_assign1_intern_tab;
use assign1_mariem;
create table if not exists staging ( eid int,
  ename string,
  age int,
  jobtype string,
  storeid int,
  storelocation string,
  salary bigint,
  yrsofexp int
)
row format delimited
fields terminated by ','
lines terminated by '\n' ;
load data local inpath 'employee.csv' into table assign1_mariem.staging;
insert into assign1_mariem.assign1_intern_tab select * from assign1_mariem.staging;
insert into assign1_loc.external_assign1_intern_tab select * from assign1_mariem.staging;
describe formatted assign1_mariem.assign1_intern_tab;
describe formatted assign1_loc.external_assign1_intern_tab;
select count(*) from songs;
use assign1_mariem;
create table if not exists songs (   
artist_id string,
artist_latitude float,
artist_location string,
artist_longitude float,
artist_name string,
duration float,
num_songs int,
song_id string,
title string,
year int
)
row format delimited
fields terminated by ','
lines terminated by '\n' ;
load data local inpath 'songs.csv' into table assign1_mariem.songs;
select * from songs limit 10;
select count(*) from songs; 
!hadoop fs  -mkdir /songs_dir;
use assign1_mariem;
create external table if not exists external_songs (   
artist_id string,
artist_latitude float,
artist_location string,
artist_longitude float,
artist_name string,
duration float,
num_songs int,
song_id string,
title string,
year int
)
row format delimited
fields terminated by ','
lines terminated by '\n' 
location 'hdfs://namenode:8020/songs_dir';
!hadoop fs  -put songs.csv /songs_dir;
hive -S -e 'select * from assign1_mariem.external_songs limit 10';
hadoop fs -cat /songs_dir/songs.csv;
DROP TABLE IF EXISTS assign1_intern_tab; 
create table if not exists assign1_intern_tab (
emp_id int ,
 emp_name string,
 age int, 
job_title string,
 dept_id int,
 city string,
 salary int,
 kilos_from_home int
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','; 
load data local inpath 'employee.csv' into table assign1_intern_tab;
hive -f script.hdl
create database test;
use assign1_mariem; 
alter table assign1_intern_tab rename to test.assign1_intern_tab;
describe formatted assign1_intern_tab ;









