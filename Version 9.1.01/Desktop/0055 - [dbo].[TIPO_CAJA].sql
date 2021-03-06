
CREATE TABLE [dbo].[TIPO_CAJA](
	[TIPO_CAJA_ID] [varchar](20) NOT NULL,
	[DESCRIPCION] [varchar](50) NOT NULL,
	[ALTO] [numeric](20, 5) NOT NULL,
	[ANCHO] [numeric](20, 5) NOT NULL,
	[LARGO] [numeric](20, 5) NOT NULL,
	[ACTIVO] [char](1) NULL,
 CONSTRAINT [PK_TIPO_CAJA] PRIMARY KEY CLUSTERED 
(
	[TIPO_CAJA_ID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]