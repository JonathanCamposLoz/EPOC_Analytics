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

CREATE TABLE usuarios_clinica_sabana(
	documento varchar(250),
	Nombre varchar(500),
	fecha_nacimiento varchar(25),
	sexo varchar(25),
	direccion varchar(50),
	barrio varchar(50),
	municipio varchar(50),
	departamento varchar(50),
    nombre_file varchar(100),
	primary key (documento)
);

DROP TABLE hc_clinca_sabana;
CREATE TABLE hc_clinca_sabana(
    id INT AUTO_INCREMENT,
    documento varchar(250),
    folio varchar(250),
	fecha_folio varchar(250),
    motivo_consulta text,
    triage_motivo_consulta text,
    enfermedad_actual text,
    antecedentes text,
    evolucion_medico text,
    formula_medica text,
    ordenes_imagenes_diagnosticas text,
    ordenes_laboratorio text,
    resultados text,
    terapias text,
    notas_enfermeria text,
    examen_fisico text,
    direccionamiento text,
    diagnostico text,
	num_ingresos integer,
	exacerbacion boolean,
    PRIMARY KEY (id),
    FOREIGN KEY (documento) REFERENCES usuarios_clinica_sabana(documento)
);



CREATE VIEW vw_resumen_datos AS
SELECT 
    documento,
    MIN(DATE_FORMAT(STR_TO_DATE(fecha_folio, '%d/%m/%Y %H:%i:%s'), '%Y-%m-%d')) AS fecha_folio,
    MAX(exacerbacion) AS exacerbacion
FROM hc_clinca_sabana
WHERE 
    DATE_FORMAT(STR_TO_DATE(fecha_folio, '%d/%m/%Y %H:%i:%s'), '%Y-%m-%d') > '2020-01-01'
    AND num_ingresos > 2 
GROUP BY documento;
