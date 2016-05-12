/****** Object:  Table [dbo].[TEMP_RPT_OCUP_DEP]    Script Date: 11/04/2013 16:23:34 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TEMP_RPT_OCUP_DEP]') AND type in (N'U'))
DROP TABLE [dbo].[TEMP_RPT_OCUP_DEP]
GO


/****** Object:  Table [dbo].[TEMP_RPT_OCUP_DEP]    Script Date: 11/04/2013 16:23:35 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[TEMP_RPT_OCUP_DEP](
	[FECHA_SNAP] [datetime] NULL,
	[CLIENTE_ID] [varchar](15) NULL,
	[NAVE_ID] [varchar](15) NULL,
	[POSICIONES_LIBRES] [int] NULL,
	[VOLUMEN_LIBRE] [float] NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO


