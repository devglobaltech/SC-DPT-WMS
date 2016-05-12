/****** Object:  StoredProcedure [dbo].[Mob_Validacion_Cont_Ubic]    Script Date: 01/16/2015 15:19:22 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_Validacion_Cont_Ubic]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_Validacion_Cont_Ubic]
GO

CREATE PROCEDURE [dbo].[Mob_Validacion_Cont_Ubic] 
@Documento_ID		numeric(20,0),
@Nro_Contenedora	varchar(50),
@Producto_id		varchar(30)=null
AS
begin
	set xact_abort on
	set nocount on
	declare @Cont numeric(20,0);
	
	--1. Valido que exista la contenedora.
	SELECT  @Cont=COUNT(RL.DOC_TRANS_ID)
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	DD.DOCUMENTO_ID=@Documento_ID
			AND DD.NRO_BULTO=@Nro_Contenedora
			AND DD.PRODUCTO_ID=@Producto_id
			
	if (@Cont=0)
	BEGIN
		RAISERROR('La contenedora indicada no existe...',16,1)
		return
	END
				
	--2. Valido el número de la contenedora que ingreso no haya sido ubicada
	IF EXISTS(SELECT  RL.NAVE_ACTUAL
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	DD.DOCUMENTO_ID=@Documento_ID
				AND DD.NRO_BULTO=@Nro_Contenedora
				AND DD.PRODUCTO_ID=@Producto_id
				AND (RL.NAVE_ACTUAL<>'1' OR RL.POSICION_ACTUAL IS NOT NULL))
		BEGIN
			RAISERROR('La contenedora ya se encuentra ubicada, verifique en Ver Pendientes las contenedoras por ubicar ',16,1)
			return
		END

	--3. CONTROLO QUE SI EXISTE ALGUN PRODUCTO EN LA CONTENEDORA QUE REQUIERA SERIE AL INGRESO, SI NO SE LE CARGO ALGUNA QUE NO DEJE UBICARLA.
	IF EXISTS (SELECT 1
				FROM DET_DOCUMENTO DD
				INNER JOIN PRODUCTO P ON (DD.CLIENTE_ID = P.CLIENTE_ID AND DD.PRODUCTO_ID = P.PRODUCTO_ID)
				WHERE	DD.NRO_BULTO = @NRO_CONTENEDORA
						AND ISNULL(P.SERIE_ING,'0') = '1'
						AND DD.NRO_SERIE IS NULL)
	BEGIN
		RAISERROR('Algunos productos de la contenedora requieren serie al ingreso obligatoria. Por favor verifica la carga de las series.',16,1)
		return
	END
end


GO


