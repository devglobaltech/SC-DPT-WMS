/****** Object:  StoredProcedure [dbo].[Mob_Transf_VerificaCatLogContenedora]    Script Date: 09/30/2015 12:04:41 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_Transf_VerificaCatLogContenedora]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_Transf_VerificaCatLogContenedora]
GO

Create  Procedure [dbo].[Mob_Transf_VerificaCatLogContenedora]
@Contenedora	as varchar(100),
@Posicion		as varchar(45),
@Transfiere		as Char(1) Output
As
Begin
	Declare @CatLog		Cursor
	Declare @vCatLog	Varchar(50)

	Set @Transfiere=Null

	Set @CatLog=  Cursor For
		SELECT 	RL.CAT_LOG_ID
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	DD.NRO_BULTO=LTRIM(RTRIM(UPPER(@CONTENEDORA))) 
				AND (RL.NAVE_ACTUAL	 =	(	SELECT 	NAVE_ID		FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@Posicion))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION 	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@Posicion)))))
				AND RL.CANTIDAD >0

	Open @Catlog
	Fetch Next From @CatLog into @vCatLog
	While @@Fetch_Status=0
	Begin
		IF (@vCatLog='TRAN_ING')OR(@vCatLog='TRAN_EGR')
		BEGIN
			Set @Transfiere=0
			RAISERROR('1- No es posible Transferir una contenedora con Categoria Logica %s.',16,1,@vCatLog)
			RETURN
		END
		Fetch Next From @CatLog into @vCatLog
	End
	If (@Transfiere is null)
	Begin
		Set @Transfiere=1
	End
End

GO


