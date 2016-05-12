/****** Object:  StoredProcedure [dbo].[MobGetRemitoTmp]    Script Date: 07/16/2013 12:49:43 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MobGetRemitoTmp]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[MobGetRemitoTmp]
GO

create PROCEDURE [dbo].[MOBGETREMITOTMP]
	@IDPROVEEDOR	VARCHAR(20),
	@Id_remito		varchar(20)
AS
BEGIN
	declare @USUARIO varchar(20)
	
	SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN

	SELECT	REMITO
	FROM    TMP_REMITO
	WHERE	IDPROVEEDOR =@IDPROVEEDOR	and procesado = 0 and id_remito=@Id_remito 
			and usuario=@usuario
END


