
CREATE TABLE [dbo].[AUDITORIA_SYS_DEV](
	[DOCUMENTO_ID] [numeric](20, 0) NULL,
	[DOC_EXT] [varchar](100) NULL,
	[STATUS_DOC_TRANS] [varchar](10) NULL,
	[USUARIO] [varchar](100) NULL,
	[TERMINAL] [varchar](100) NULL,
	[FECHA] [datetime] NULL
) ON [PRIMARY]