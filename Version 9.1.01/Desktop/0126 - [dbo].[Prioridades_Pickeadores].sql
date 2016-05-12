
CREATE TABLE [dbo].[Prioridades_Pickeadores](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Proceso_Activo] [varchar](1) NULL,
	[Usuario_Activacion] [varchar](20) NOT NULL,
	[Terminal_Activacion] [varchar](20) NULL,
	[Fecha_Activacion] [datetime] NULL,
	[Usuario_Anulacion] [varchar](20) NULL,
	[Terminal_Anulacion] [varchar](20) NULL,
	[Fecha_Anulacion] [datetime] NULL,
 CONSTRAINT [PK_Prioridades_Pickeadores] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
