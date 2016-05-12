/****** Object:  StoredProcedure [dbo].[VALIDA_CONTENEDORA_SERIE]    Script Date: 04/23/2014 13:13:43 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[VALIDA_CONTENEDORA_SERIE]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[VALIDA_CONTENEDORA_SERIE]
GO

CREATE PROCEDURE [dbo].[VALIDA_CONTENEDORA_SERIE]
@CLIENTE		VARCHAR(15),
@CONTENEDORA	VARCHAR(50),
@RETORNO		VARCHAR(2) OUTPUT
AS
BEGIN
	/*
	retorno:
		0 - La contenedora no existe.
		1 - La contenedora ya se encuentra ubicada.
		2 - La contenedora no posee series pendientes.
		3 - La contenedora no es serializable.
		4 - Todo Ok.
	*/
	DECLARE @RET	VARCHAR(2)
	DECLARE @CONT	NUMERIC
	
	---------------------------------------------------------------------------------
	--1. EXISTE CONTENEDORA
	---------------------------------------------------------------------------------
	SELECT	@CONT=COUNT(dd.NRO_BULTO)
	FROM	DET_DOCUMENTO dd inner join DET_DOCUMENTO_TRANSACCION ddt
			on(dd.DOCUMENTO_ID=ddt.DOCUMENTO_ID and dd.NRO_LINEA=ddt.NRO_LINEA_DOC)
			inner join RL_DET_DOC_TRANS_POSICION rl
			on(ddt.DOC_TRANS_ID=rl.DOC_TRANS_ID and ddt.NRO_LINEA_TRANS=rl.NRO_LINEA_TRANS)
	where	dd.CLIENTE_ID=@CLIENTE
			and dd.NRO_BULTO=@CONTENEDORA	
						
	if @CONT=0 begin
		set @RETORNO='0'
		return
	end
	
	---------------------------------------------------------------------------------
	--Contenedora Ubicada.
	---------------------------------------------------------------------------------
	SELECT	@CONT=COUNT(dd.NRO_BULTO)
	FROM	DET_DOCUMENTO dd inner join DET_DOCUMENTO_TRANSACCION ddt
			on(dd.DOCUMENTO_ID=ddt.DOCUMENTO_ID and dd.NRO_LINEA=ddt.NRO_LINEA_DOC)
			inner join RL_DET_DOC_TRANS_POSICION rl
			on(ddt.DOC_TRANS_ID=rl.DOC_TRANS_ID and ddt.NRO_LINEA_TRANS=rl.NRO_LINEA_TRANS)
			inner join NAVE n
			on(rl.NAVE_ACTUAL=n.NAVE_ID)
	where	dd.CLIENTE_ID=@CLIENTE
			and dd.NRO_BULTO=@CONTENEDORA
			and isnull(n.PRE_INGRESO,'0')='1'
	
	if @CONT=0 begin
		set @RETORNO='1'
		return
	end

	---------------------------------------------------------------------------------
	--Contenedora Ubicada.
	---------------------------------------------------------------------------------
	SELECT	@CONT=COUNT(dd.NRO_BULTO)
	FROM	DET_DOCUMENTO dd inner join DET_DOCUMENTO_TRANSACCION ddt
			on(dd.DOCUMENTO_ID=ddt.DOCUMENTO_ID and dd.NRO_LINEA=ddt.NRO_LINEA_DOC)
			inner join RL_DET_DOC_TRANS_POSICION rl
			on(ddt.DOC_TRANS_ID=rl.DOC_TRANS_ID and ddt.NRO_LINEA_TRANS=rl.NRO_LINEA_TRANS)
	where	dd.CLIENTE_ID=@CLIENTE
			and dd.NRO_BULTO=@CONTENEDORA
			and dd.NRO_SERIE is null		
						
	if @CONT=0 begin
		set @RETORNO='2'
		return
	end		

	
	SELECT	@CONT=COUNT(dd.NRO_BULTO)
	FROM	DET_DOCUMENTO dd inner join DET_DOCUMENTO_TRANSACCION ddt
			on(dd.DOCUMENTO_ID=ddt.DOCUMENTO_ID and dd.NRO_LINEA=ddt.NRO_LINEA_DOC)
			inner join RL_DET_DOC_TRANS_POSICION rl
			on(ddt.DOC_TRANS_ID=rl.DOC_TRANS_ID and ddt.NRO_LINEA_TRANS=rl.NRO_LINEA_TRANS)
			inner join PRODUCTO p
			on(dd.CLIENTE_ID=p.CLIENTE_ID and dd.PRODUCTO_ID=p.PRODUCTO_ID and isnull(p.SERIE_ING,'0')='1')
	where	dd.CLIENTE_ID=@CLIENTE
			and dd.NRO_BULTO=@CONTENEDORA
			and dd.NRO_SERIE is null		
						
	if @CONT=0 begin
		set @RETORNO='3'
		return
	end		
	
	set @RETORNO ='4'					
END

GO


