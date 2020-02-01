-- External table
create external table hw1ChessRatings_ext(ID_Number int,Name string,Fed string,Sex string,Tit string,Wtit string,ITit string,FOA string,Rat int,Gms int,K int,B_day int,Flag string,Year int,Mon int)row format delimited fields terminated by '|' lines terminated by '\n' stored as textfile location '/user/matijmic/hw1' tblproperties ("skip.header.line.count"="1");

-- Internal table
-- Creation internal table for hw1
-- Columns: name, fed, sex, rat, gms, bday, year, mon
create table chess_ratings(name string,fed string,rat int,gms int,bday int,year int,mon int) partitioned by(sex string) stored as orc tblproperties("orc.compress"="ZLIB");
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table chess_ratings partition (sex) select name, fed, rat, gms, B_day as bday, year, mon, sex from hw1ChessRatings_ext;

select t.name, t.fed, t.rat, t.gms, t.bday, t.year, t.mon, t.sex from (select *, rank() over (partition by mon order by rat desc) as rank from chess_ratings where sex = 'F') t where rank < 6;

/*
+----------------+--------+--------+--------+---------+---------+--------+--------+--+
|     t.name     | t.fed  | t.rat  | t.gms  | t.bday  | t.year  | t.mon  | t.sex  |
+----------------+--------+--------+--------+---------+---------+--------+--------+--+
| Polgar, Judit  | HUN    | 2675   | 0      | 1976    | 2019    | 10     | F      |
| Hou, Yifan     | CHN    | 2659   | 0      | 1994    | 2019    | 10     | F      |
| Ju, Wenjun     | CHN    | 2586   | 11     | 1991    | 2019    | 10     | F      |
| Polgar, Susan  | HUN    | 2577   | 0      | 1969    | 2019    | 10     | F      |
| Koneru, Humpy  | IND    | 2577   | 11     | 1987    | 2019    | 10     | F      |
| Polgar, Judit  | HUN    | 2675   | 0      | 1976    | 2019    | 11     | F      |
| Hou, Yifan     | CHN    | 2659   | 0      | 1994    | 2019    | 11     | F      |
| Ju, Wenjun     | CHN    | 2586   | 0      | 1991    | 2019    | 11     | F      |
| Polgar, Susan  | HUN    | 2577   | 0      | 1969    | 2019    | 11     | F      |
| Koneru, Humpy  | IND    | 2577   | 0      | 1987    | 2019    | 11     | F      |
| Polgar, Judit  | HUN    | 2675   | 0      | 1976    | 2019    | 12     | F      |
| Hou, Yifan     | CHN    | 2664   | 2      | 1994    | 2019    | 12     | F      |
| Ju, Wenjun     | CHN    | 2580   | 3      | 1991    | 2019    | 12     | F      |
| Polgar, Susan  | HUN    | 2577   | 0      | 1969    | 2019    | 12     | F      |
| Xie, Jun       | CHN    | 2574   | 0      | 1970    | 2019    | 12     | F      |
| Koneru, Humpy  | IND    | 2574   | 6      | 1987    | 2019    | 12     | F      |
+----------------+--------+--------+--------+---------+---------+--------+--------+--+
*/