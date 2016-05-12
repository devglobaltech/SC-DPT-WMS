
/****** Object:  StoredProcedure [dbo].[OrderByNroLinea]    Script Date: 10/28/2014 12:00:02 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[OrderByNroLinea]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[OrderByNroLinea]
GO

create Procedure [dbo].[OrderByNroLinea]
@Documento_Id	Varchar(30)
As
Begin
	Declare @tCur		Cursor
	Declare @Nro_Linea	Numeric(10,0)
	Declare @vNro_Linea	Numeric(10,0)
	--agregado DFERNANDEZ 24-07-2013
	Declare @Nro_Linea_aux Numeric (10,0)
	Declare @cant Numeric (10,0)
	DECLARE @CANT2		NUMERIC(10,0)
	
	SELECT @CANT2 = COUNT(*) FROM consumo_locator_egr WHERE Documento_Id=@Documento_Id
	IF @CANT2 = 0
		BEGIN
			RETURN
		END

	Set @tCur = Cursor for
		SELECT 	nro_linea 
		--FROM 	det_documento_aux
		FROM 	det_documento
		WHERE 	documento_id=@Documento_Id
		ORDER BY 
				nro_linea		

	Open 	@tCur
	Set 	@vNro_Linea=0 

	Fetch Next From @tCur Into 	@Nro_Linea
	While @@Fetch_Status=0
	Begin
		Set @vNro_Linea=@vNro_Linea +1
		
		--SET @cant=1
		--ME FIJO SI ESTA LA LINEA, SI NO ESTA ES PORQUE SE ELIMINO
		--ENTONCES LE TENGO QUE SUMAR UNO HASTA ENCONTRARLA
		SELECT @cant = COUNT(*) FROM consumo_locator_egr WHERE Documento_Id=@Documento_Id and Nro_Linea=@Nro_Linea
		--SI NO ESTA
		WHILE (@cant = 0)
		BEGIN
			SELECT @cant = COUNT(*) FROM consumo_locator_egr WHERE Documento_Id=@Documento_Id and Nro_Linea=@Nro_Linea
				IF (@cant <> 0)
					--SALGO
					break
				SET @Nro_Linea = @Nro_Linea +1
				CONTINUE
		END
		
		Update Consumo_locator_Egr 	Set Nro_linea=@vNro_Linea Where	Documento_Id=@Documento_Id and Nro_Linea=@Nro_Linea
		Update det_documento_aux 	Set Nro_linea=@vNro_Linea Where	Documento_Id=@Documento_Id and Nro_Linea=@Nro_Linea

		Fetch Next From @tCur Into 	@Nro_Linea
	End

	Close @tCur
	Deallocate @tCur

End --Fin Procedure

GO


