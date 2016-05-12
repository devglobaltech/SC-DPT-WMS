DROP TABLE SNAP_EXISTENCIAS;
GO
CREATE TABLE [SNAP_EXISTENCIAS] (
  [SNAP_ID]             numeric(20,0)  NOT NULL IDENTITY(1,1),
  [F_SNAP]              datetime       NOT  NULL,
  [RL_ID]               numeric(20, 0) NOT  NULL,
  [DOC_TRANS_ID]        numeric(20, 0)      NULL,
  [NRO_LINEA_TRANS]     numeric(10, 0)      NULL,
  [POSICION_ANTERIOR]   numeric(20, 0)      NULL,
  [POSICION_ACTUAL]     numeric(20, 0)      NULL,
  [CANTIDAD]            numeric(20, 5) NOT  NULL,
  [TIPO_MOVIMIENTO_ID]  varchar(5)          NULL,
  [ULTIMA_ESTACION]     varchar(5)          NULL,
  [ULTIMA_SECUENCIA]    numeric(3, 0)       NULL,
  [NAVE_ANTERIOR]       numeric(20, 0)      NULL,
  [NAVE_ACTUAL]         numeric(20, 0)      NULL,
  [DOCUMENTO_ID]        numeric(20, 0)      NULL,
  [NRO_LINEA]           numeric(10, 0)      NULL,
  [DISPONIBLE]          varchar(1)          NULL,
  [DOC_TRANS_ID_EGR]    numeric(20, 0)      NULL,
  [NRO_LINEA_TRANS_EGR] numeric(10, 0)      NULL,
  [DOC_TRANS_ID_TR]     numeric(20, 0)      NULL,
  [NRO_LINEA_TRANS_TR]  numeric(10, 0)      NULL,
  [CLIENTE_ID]          varchar(15)         NULL,
  [CAT_LOG_ID]          varchar(50)         NULL,
  [CAT_LOG_ID_FINAL]    varchar(50)         NULL,
  [EST_MERC_ID]         varchar(15)         NULL
);
GO
ALTER TABLE SNAP_EXISTENCIAS
ADD CONSTRAINT PK_SNAP_EXISTENCIAS PRIMARY KEY(SNAP_ID);
GO
CREATE INDEX IDX_SE_FSNAP ON SNAP_EXISTENCIAS(CLIENTE_ID,F_SNAP);
