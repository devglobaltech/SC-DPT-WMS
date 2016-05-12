
/****** Object:  StoredProcedure [dbo].[PRINT_ETI_BULTOEGR_T]    Script Date: 01/28/2015 15:08:44 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PRINT_ETI_BULTOEGR_T]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[PRINT_ETI_BULTOEGR_T]
GO

CREATE PROCEDURE [dbo].[PRINT_ETI_BULTOEGR_T]
@VIAJE_ID	VARCHAR(100) OUTPUT,
@TIPO_PICK	CHAR(1)	OUTPUT,
@VH			Varchar(20) Output
AS	
BEGIN
	CREATE TABLE #NEW_ETI( 
		SUCURSAL_ID VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS ,
		OS          VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS ,
		CODIGO_ID   VARCHAR(100)COLLATE SQL_Latin1_General_CP1_CI_AS ,
		DESCRIPCION VARCHAR(100)COLLATE SQL_Latin1_General_CP1_CI_AS ,
		BULTO       VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS ,
		ID_PICK     VARCHAR(100)COLLATE SQL_Latin1_General_CP1_CI_AS ,
		CALLE       VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS ,
		Printer		VarChar(100) COLLATE SQL_Latin1_General_CP1_CI_AS ,
		PRODUCTO_ID VarChar(30) COLLATE SQL_Latin1_General_CP1_CI_AS 
	)

	TRUNCATE TABLE #NEW_ETI
	DECLARE @CUR		CURSOR
	DECLARE @Prod		varchar(30)
	Declare @Desc		varchar(45)
	Declare @Qty		Float
	Declare @Pos		varchar(45)
	Declare @Prop1		varchar(100)
	Declare @Ruta		varchar(100)
	Declare @unidad		Varchar(5)
	Declare @Cliente	Varchar(15)
	Declare @Child		varchar(50)
	Declare @Cont		int
	Declare @ContDoc	int
	Declare @CurDoc		Cursor
	Declare @Documento	numeric(20,0)
	Declare @NroLinea	numeric(10,0)
	Declare @qtyDoc		Float
	Declare @SucursalId	Varchar(20)
	Declare @CodePrincipal varchar(100)
	Declare @Codigo		char(1)
	Declare @Calle		Varchar(50)
	Declare @Printer	as varchar(100)
	Declare @Terminal	as varchar(100)
	Declare @SucCod		as varchar(20)

	Set @terminal=Host_name()
	Select @Printer=Printer_id from sys_printer_default_etiqueta where Terminal_id=@terminal
	
	Set @Cur = Cursor for
		SELECT 	SP.PRODUCTO_ID, SP.DESCRIPCION, SUM(SP.CANTIDAD),SP.POSICION_COD,
				SP.PROP1,SP.RUTA,PROD.UNIDAD_ID, SP.CLIENTE_ID,	SP.HIJO
		FROM 	PICKING SP
				INNER JOIN PRIORIDAD_VIAJE SPV
				ON(LTRIM(RTRIM(UPPER(SPV.VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))
				INNER JOIN PRODUCTO PROD
				ON(PROD.CLIENTE_ID=SP.CLIENTE_ID AND PROD.PRODUCTO_ID=SP.PRODUCTO_ID)
				LEFT JOIN POSICION POS ON(SP.POSICION_COD=POS.POSICION_COD)
		WHERE 	SPV.PRIORIDAD = ( SELECT 	MIN(PRIORIDAD) FROM	PRIORIDAD_VIAJE	WHERE	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))								
				AND (DBO.VERIFICA_PALLET_FINAL(SP.POSICION_COD,SP.VIAJE_ID,SP.RUTA, SP.PROP1)=0)
				AND	SP.VIAJE_ID IN(	SELECT 	VIAJE_ID 
									FROM  RL_VIAJE_USUARIO 
									WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))) 
									)
				AND SP.NAVE_COD	IN(	SELECT 	NAVE_COD
									FROM 	NAVE N INNER JOIN RL_USUARIO_NAVE RLNU
											ON(N.NAVE_ID=RLNU.NAVE_ID)
									WHERE	N.NAVE_COD=SP.NAVE_COD
									)
				AND SP.FIN_PICKING <>'2'
				AND SP.POSICION_COD IN(	SELECT 	POSICION_COD
										FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
												INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
												INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
										WHERE 	VEHICULO_ID='ZE1'
												--AND CN.CALLE_ID IN(SELECT CALLE_ID FROM #TEMP_PARAM_ETI WHERE NAVE_ID IS NULL)
												AND CN.CALLE_COD='01AL'
										UNION 
										SELECT 	NAVE_COD AS POSICION_COD
										FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
												ON(V.NAVE_ID=N2.NAVE_ID)
										WHERE	VEHICULO_ID='ZE1'
												--AND N2.NAVE_ID IN(SELECT nave_id FROM #TEMP_PARAM_ETI WHERE calle_id IS NULL)
												AND N2.NAVE_COD='01AL'
										)
				AND SP.VIAJE_ID='20090618'
		GROUP BY	SP.VIAJE_ID, SP.PRODUCTO_ID,SP.DESCRIPCION, SP.RUTA,SP.POSICION_COD,SP.TIPO_CAJA,SP.PROP1,PROD.UNIDAD_ID,SPV.PRIORIDAD,POS.ORDEN_PICKING
					, SP.CLIENTE_ID, PROD.VAL_COD_EGR, SP.HIJO
		ORDER BY	SPV.PRIORIDAD ASC,SP.TIPO_CAJA DESC, POS.ORDEN_PICKING, SP.POSICION_COD ASC, SP.PRODUCTO_ID

	Open @Cur
	Set @ContDoc=0
	Set @Cont=0
	Fetch Next From @Cur Into 	@Prod,	@Desc,	@Qty,	@Pos,	@Prop1,	@Ruta,	@unidad,	@Cliente,	@Child
	While @@Fetch_Status=0
	Begin
		--Set @calle=Substring(@Pos,1,5)

		Set @CurDoc=Cursor For
			Select	Documento_Id, Nro_Linea, Cantidad
			From	Picking
			Where	Viaje_id=@VIAJE_ID and producto_id=@Prod and posicion_cod=@Pos and prop1=@Prop1 and ruta=@Ruta and cliente_id=@Cliente
		Open @CurDoc
		Fetch Next from @CurDoc Into @Documento, @NroLinea, @QtyDoc
		While @@Fetch_Status=0
		Begin
			
			Select	@SucursalId=D.Sucursal_Destino, @SucCod=s.nombre
			from	picking p inner join det_documento dd on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
					inner join documento d on(d.documento_id=dd.documento_id)
					inner join sucursal s on(s.cliente_id=d.cliente_id and s.sucursal_id=d.sucursal_destino)
			where	p.Documento_id=@Documento and p.nro_linea=@nrolinea

			While @ContDoc<=@QtyDoc -1
			Begin
				Set @ContDoc=@ContDoc+1
				Set @Cont=@Cont +1 

				If @Cont=1
				Begin

					INSERT INTO Etiquetas_Picking (CODE, DOCUMENTO_ID, NRO_LINEA, NRO_BULTO)
					SELECT	@SucursalId +'-'+@prod+'-'+cast(@ContDoc as varchar)+'-'+@Child, @DOCUMENTO, @NROLINEA, @ContDoc
					WHERE	NOT EXISTS (SELECT	* 
										FROM	Etiquetas_Picking 
										WHERE	CODE=@SucursalId +'-'+@prod+'-'+cast(@ContDoc as varchar)+'-'+@Child)

					INSERT INTO #NEW_ETI VALUES(@SucCod,'OS: ' + cast(@Documento as varchar),
												@SucursalId +'-'+@prod+'-'+cast(@ContDoc as varchar)+'-'+@Child,
												@Desc,'BULTO: ' + Cast(@Cont as varchar) +' de ' + cast(@Qty as varchar),
												'COD: ' + Cast(@Child as varchar), @Pos,@Printer,@Prod)	
				End
				If @Cont>1
				Begin

					INSERT INTO Etiquetas_Picking (CODE, DOCUMENTO_ID, NRO_LINEA, NRO_BULTO)
					SELECT	@SucursalId +'-'+@prod+'-'+cast(@ContDoc as varchar)+'-'+@Child,@DOCUMENTO,@NROLINEA, @ContDoc
					WHERE	NOT EXISTS (SELECT	* 
										FROM	Etiquetas_Picking 
										WHERE	CODE=@SucursalId +'-'+@prod+'-'+cast(@ContDoc as varchar)+'-'+@Child)


					INSERT INTO #NEW_ETI VALUES(@SucCod,'OS: ' + cast(@Documento as varchar),
												@SucursalId +'-'+@prod+'-'+cast(@ContDoc as varchar)+'-'+@Child,
												@Desc,'BULTO: ' + Cast(@Cont as varchar) +' de ' + cast(@Qty as varchar),
												null, @Pos, @Printer,@Prod)	
				End								
			End
			set @ContDoc=0
			Fetch Next from @CurDoc Into @Documento, @NroLinea, @QtyDoc
		End
		Close @CurDoc
		Deallocate @CurDoc
		Set @Cont=0
		Fetch Next From @Cur Into 	@Prod,	@Desc,	@Qty,	@Pos,	@Prop1,	@Ruta,	@unidad,	@Cliente,	@Child
	End
	Close @Cur
	Deallocate @Cur



End --procedure

GO


