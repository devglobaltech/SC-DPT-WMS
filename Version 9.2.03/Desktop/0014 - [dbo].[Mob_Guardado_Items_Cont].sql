/****** Object:  StoredProcedure [dbo].[Mob_Guardado_Items_Cont]    Script Date: 09/12/2014 11:28:07 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_Guardado_Items_Cont]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_Guardado_Items_Cont]
GO

CREATE PROCEDURE [dbo].[Mob_Guardado_Items_Cont] 
@Documento_ID	numeric(20,0),
@Cantidad		numeric(20,5),
@Posicion_Cod	varchar(45),
@Producto_id	varchar(30)=null
AS
begin
	set xact_abort on
	set nocount on
	Declare @Qty		as numeric(20,5)
	Declare @QtyVar		as varchar(max)
	Declare @CurRl		as Cursor
	Declare @RlID		as Numeric(20,0)
	Declare @QtyRL		as numeric(20,5)
	Declare @NewPos		as Numeric(20,0)
	Declare @NewNave	as Numeric(20,0)
	Declare @NewRl		as Numeric(20,0)
	
	--1. Valido que la cantidad ingresada sea menor o igual a la cantidad disponible para guardar.
	IF NOT EXISTS(SELECT RL.NRO_LINEA_TRANS
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	DD.DOCUMENTO_ID=@Documento_ID AND DD.PRODUCTO_ID=@Producto_id AND RL.NRO_LINEA_TRANS=DDT.NRO_LINEA_TRANS
			AND DD.NRO_BULTO=@Cantidad AND RL.NAVE_ACTUAL='1')

	BEGIN
		Set @qtyvar=cast(@Cantidad as varchar)
		RAISERROR('La contenedora a ubicar no existe, verifique en Ver Pend. %s',16,1,'')
		return
	END
	--1.1 Obtengo los Id.
	SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@Posicion_Cod) AS INT)
	SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@Posicion_Cod) AS INT)
	
	IF (@NEWNAVE IS NULL) AND (@NEWPOS IS NULL)
	BEGIN
		RAISERROR('LA UBICACON ESTABLECIDA ES INVALIDA %s',16,1,@Posicion_Cod)
		RETURN
	END
	--2. Ubico la mercaderia sin tener que hacer un split de la rl. :)
	
		update	rl_det_doc_trans_posicion 
				set nave_anterior=nave_actual,
					nave_actual=@NEWNAVE,
					posicion_actual=@NEWPOS
		FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
				ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
		WHERE	DD.DOCUMENTO_ID=@Documento_ID
				AND DD.NRO_BULTO=@Cantidad
				AND RL.NAVE_ACTUAL='1' AND DD.PRODUCTO_ID=@Producto_id
	
end

GO


