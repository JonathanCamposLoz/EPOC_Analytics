use db_geolocalitation;

CREATE TABLE pollution(
	co double,
	no double,
	no2 double,
	o3 double,
	so2 double,
	pm2_5 double,
	pm10 double,
	nh3 double,
	lon double,
	lat double,
	aqi double,
	range_h varchar(50),
	color varchar(50),
	Level varchar(50),
	date varchar(50)
);

CREATE TABLE weather(
id int,
main varchar(20),
description varchar(20),
icon varchar(20),
temp double,
feels_like double,
temp_min double,
temp_max double,
pressure double,
humidity double,
speed double,
deg double,
type double,
id_sys double,
country varchar(20),
sunrise double,
sunset double,
base varchar(20),
visibility varchar(20),
date varchar(20),
timezone varchar(20),
name varchar(300)
);

