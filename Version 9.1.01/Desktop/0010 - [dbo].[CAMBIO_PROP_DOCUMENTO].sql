/****** Object:  StoredProcedure [dbo].[CAMBIO_PROP_DOCUMENTO]    Script Date: 10/24/2013 13:00:28 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CAMBIO_PROP_DOCUMENTO]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[CAMBIO_PROP_DOCUMENTO]
GO

CREATE   PROCEDURE	[dbo].[CAMBIO_PROP_DOCUMENTO]	
			 @C_DocId	AS VARCHAR(20) OUTPUT
			,@C_Nro_Linea	AS VARCHAR (10) OUTPUT
			,@C_FVTO	VARCHAR(20) OUTPUT
			,@C_Pallet	VARCHAR(100) OUTPUT
			,@C_Lote 	VARCHAR(50) OUTPUT
			,@C_LoteP	VARCHAR(100) OUTPUT 
			,@C_Bulto 	VARCHAR(50) OUTPUT
			,@C_PROP3	VARCHAR(100) OUTPUT
			,@C_NroTieIn	VARCHAR(100) OUTPUT
			,@C_NroTieInPadre	VARCHAR(100) OUTPUT
			,@C_Serie 	VARCHAR(50) OUTPUT			
			,@C_Despacho 	VARCHAR(50) OUTPUT			
			,@C_Partida 	VARCHAR(50) OUTPUT
			
AS
BEGIN
	DECLARE @StrSql 	AS NVARCHAR(4000) 
	DECLARE @StrSet 	AS NVARCHAR(4000) 

	Set @StrSet = ''

	Set @StrSql = 'UPDATE	DET_DOCUMENTO ' + char(13)
	Set @StrSql = @StrSql + 'SET ' + Char(13) 


	If @C_FVTO is not null  and @C_FVTO <> ''
	Begin
		Set @StrSet = @StrSet + '  FECHA_VENCIMIENTO =  Cast( ' + char(39) +  @C_FVTO + char(39)+ ' as Datetime) ' + Char(13)
	End 


	If @C_Pallet is not null and @C_Pallet <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  PROP1 =  ' + char(39) +  @C_Pallet + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,PROP1 =  ' + char(39) +  @C_Pallet + char(39) + Char(13)
		End 
	End 

	If @C_Lote is not null and @C_Lote <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  NRO_LOTE =  ' + char(39) +  @C_Lote + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,NRO_LOTE =  ' + char(39) +  @C_Lote + char(39) + Char(13)
		End 
	End 

	If @C_LoteP is not null and @C_LoteP <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  PROP2 =  ' + char(39) +  @C_LoteP + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,PROP2 =  ' + char(39) +  @C_LoteP + char(39) + Char(13)
		End 
	End 

	If @C_Bulto is not null and @C_Bulto <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  NRO_BULTO =  ' + char(39) +  @C_Bulto + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,NRO_BULTO =  ' + char(39) +  @C_Bulto + char(39) + Char(13)
		End 
	End 

	If @C_PROP3 is not null and @C_PROP3 <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  PROP3 =  ' + char(39) +  @C_PROP3 + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,PROP3 =  ' + char(39) +  @C_PROP3 + char(39) + Char(13)
		End 
	End 

	If @C_NroTieIn is not null and @C_NroTieIn <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  NRO_TIE_IN =  ' + char(39) +  @C_NroTieIn + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,NRO_TIE_IN =  ' + char(39) +  @C_NroTieIn + char(39) + Char(13)
		End 
	End 

	If @C_NroTieInPadre is not null and @C_NroTieInPadre <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  NRO_TIE_IN_PADRE =  ' + char(39) +  @C_NroTieInPadre + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,NRO_TIE_IN_PADRE =  ' + char(39) +  @C_NroTieInPadre + char(39) + Char(13)
		End 
	End 
							
	If @C_Serie is not null and @C_Serie <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  NRO_SERIE =  ' + char(39) +  @C_Serie + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,NRO_SERIE =  ' + char(39) +  @C_Serie + char(39) + Char(13)
		End 
	End 
	
	If @C_Despacho is not null and @C_Despacho <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  NRO_DESPACHO =  ' + char(39) +  @C_Despacho + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,NRO_DESPACHO =  ' + char(39) +  @C_Despacho + char(39) + Char(13)
		End 
	End 	
	
	If @C_Partida is not null and @C_Partida <> ''
	Begin
		If @StrSet = ''
		Begin
			Set @StrSet = @StrSet + '  NRO_PARTIDA =  ' + char(39) +  @C_Partida + char(39) + Char(13)
		End
		Else
		Begin
			Set @StrSet = @StrSet + ' ,NRO_PARTIDA =  ' + char(39) +  @C_Partida + char(39) + Char(13)
		End 
	End 			
	
	Set @StrSet = @StrSet +  ' WHERE DOCUMENTO_ID =  Cast( ' + char(39) +  @C_DOCID + Char(39) + ' as Numeric (20,0)) AND  NRO_LINEA = Cast(' + Char(39) +  @C_NRO_LINEA + Char(39) + ' as Numeric (10,0)) ' +  Char(13)

	Set @StrSql =  @StrSql +  @StrSet
	EXECUTE SP_EXECUTESQL  @StrSql

END


