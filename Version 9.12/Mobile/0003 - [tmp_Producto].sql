

CREATE TABLE [dbo].[tmp_Producto](
	[PROVEEDOR_ID] [varchar](20) NOT NULL,
	[PRODUCTO_ID] [varchar](30) NOT NULL,
	[Descripcion] [varchar](200) NOT NULL,
	[cantidad] [numeric](20, 5) NOT NULL,
	[PROCESADO] [varchar](1) NOT NULL,
	[ID] [varchar](20) NOT NULL,
	[USUARIO] [varchar](20) NOT NULL
) ON [PRIMARY]

