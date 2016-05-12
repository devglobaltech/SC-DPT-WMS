
CREATE TABLE dbo.AJUSTES_MASIVOS(
	AJUSTE_ID			bigint identity(1,1) not null,
	CLIENTE_ID			varchar(15)		NOT NULL,
	PRODUCTO_ID			varchar(30)		NOT NULL,
	NRO_SERIE			varchar(50)		NULL,
	EST_MERC_ID			varchar(15)		NULL,
	CAT_LOG_ID			varchar(50)		NULL,
	NRO_BULTO			varchar(50)		NULL,
	NRO_LOTE			varchar(50)		NULL,
	FECHA_VENCIMIENTO	datetime		NULL,
	NRO_DESPACHO		varchar(50)		NULL,
	NRO_PARTIDA			varchar(50)		NULL,
	PROP1				varchar(100)	NULL,
	PROP2				varchar(100)	NULL,
	PROP3				varchar(100)	NULL,
	POSICION_ID			numeric(20,0)	null,
	NAVE_ID				numeric(20,0)	null,
	CANTIDAD_AJUSTE		numeric(20,5)	null,
	OBS_AJUSTE			varchar(4000)	null,
	PROCESADO			CHAR(1) DEFAULT('0'),
	F_PROCESADO			dateTime
);