ALTER TABLE CLIENTE_PARAMETROS
ADD FLG_RECORRIDO_PALLET_FINAL VARCHAR(1)
GO

UPDATE CLIENTE_PARAMETROS SET FLG_RECORRIDO_PALLET_FINAL='0'
GO

INSERT INTO SYS_TABCOLUMNS VALUES('CLIENTE_PARAMETROS',	'FLG_RECORRIDO_PALLET_FINAL',	29,	'CHAR',	'Y',	'1',	NULL,	NULL,	'1',	NULL)
GO

INSERT INTO SYS_DET_TABLA VALUES('CLIENTE_PARAMETROS',	'FLG_RECORRIDO_PALLET_FINAL',	'Picking - Recorrido PF',	'S',	1,	0)