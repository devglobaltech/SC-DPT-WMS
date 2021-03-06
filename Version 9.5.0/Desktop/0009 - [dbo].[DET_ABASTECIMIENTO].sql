Create table [dbo].[DET_ABASTECIMIENTO]
(
	[ABAST_ID]		Bigint NOT NULL IDENTITY(1,1),
	[CLIENTE_ID]	Varchar(15) NOT NULL,
	[PRODUCTO_ID]	Varchar(30) NOT NULL,
	[POSICION_ID]	Bigint NULL,
	[PRIORIDAD]		Bigint NULL,
	[USUARIO]		Varchar(100) NULL,
	[TERMINAL]		Varchar(100) NULL,
	[F_INICIO]		Datetime NULL,
	[EN_PROGRESO]	Char(1) Default '0' NULL,
	[NRO_CONT_TR]	Bigint NULL,
	[FINALIZADO]	Char(1) Default '0' NULL,
	[F_FIN]			Datetime NULL
) 
go

ALTER TABLE DET_ABASTECIMIENTO
ADD CONSTRAINT PK_DET_ABASTECIMIENTO PRIMARY KEY(ABAST_ID);
go

ALTER TABLE DET_ABASTECIMIENTO
ADD CONSTRAINT FK_DET_ABAS_PROD FOREIGN KEY (CLIENTE_ID,PRODUCTO_ID)
REFERENCES PRODUCTO (CLIENTE_ID,PRODUCTO_ID)
go

ALTER TABLE DET_ABASTECIMIENTO
ADD CANT_A_ABASTECER	NUMERIC(20,5)
go

ALTER TABLE DET_ABASTECIMIENTO
ADD CANT_ABASTECIDA	NUMERIC(20,5)