create database assign2;

!hadoop fs -mkdir -p /songs_assign2/songs_assign3;
create external table assign1_mariem.songs_external(
artist_id string,
artist_latitude string,
artist_location string,
artist_longitude string,  
duration string,
num_songs string,
song_id string,  
title string
)
PARTITIONED by(artist_name string , year string )
row format delimited
fields terminated by ','
lines terminated by '\n'
location 'hdfs://namenode:8020/songs_assign2';
 

!hadoop fs -put songs.csv /songs_assign2/songs_assign3;
 
select * from songs_external;

alter table songs_external add partition(artist_name='mariem',year= '2022')
location 'hdfs://namenode:8020/songs_assign2/songs_assign3';

!hadoop fs -put songs.csv /songs_assign2/songs_assign3;

!hadoop fs -ls /songs_assign2/songs_assign3;

create table staging_tab (
artist_id string,
artist_latitude string,
artist_location string,
artist_longitude string,
artist_name string,      	
duration string,
num_songs string,
song_id string,  
title string,      	
year string
)
row format delimited
fields terminated by ','
lines terminated by '\n';
load data  local inpath 'songs.csv' into table staging_tab;
 
 
Insert overwrite table assign1_mariem.songs_external partition (artist_name , year)
select artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,   	
duration,
num_songs,
song_id,           	
title,   	
year
From staging_tab;
 

drop table assign1_mariem.songs_external;
Select * from assign1_mariem.songs_external;

Insert overwrite table assign1_mariem.songs_external partition (artist_name , year)
select artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,   	
duration,
num_songs,
song_id,           	
title,   	
year
From staging_tab;

truncate table assign1_mariem.songs_external;
Select * from assign1_mariem.songs_external;

drop table assign1_mariem.songs_external;
create table assign1_mariem.songs_external(
	 artist_id string,
     artist_latitude string,
 	artist_location string,
 	artist_longitude string,
 	duration string,
 	num_songs string,
 	song_id string,
 	title string
 	)
 	Partitioned by (year string,artist_name string)
 	row format delimited
 	fields terminated by ','
 	lines terminated by '\n';
 
 
Insert overwrite table assign1_mariem.songs_external
partition (year='2007', artist_name)
select artist_id,
artist_latitude,
artist_location,
artist_longitude,
artist_name,   	
duration,
num_songs,
song_id,           	
title
From staging_tab WHERE YEAR='2007';