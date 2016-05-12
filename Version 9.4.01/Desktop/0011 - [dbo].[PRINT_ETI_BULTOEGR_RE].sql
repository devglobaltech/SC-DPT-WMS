
/****** Object:  StoredProcedure [dbo].[PRINT_ETI_BULTOEGR_RE]    Script Date: 01/28/2015 15:08:18 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PRINT_ETI_BULTOEGR_RE]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[PRINT_ETI_BULTOEGR_RE]
GO

CREATE  PROCEDURE [dbo].[PRINT_ETI_BULTOEGR_RE]
@VIAJE_ID	VARCHAR(100) OUTPUT,
@TIPO_PICK	CHAR(1)	OUTPUT
AS	
BEGIN
/*
CREATE TABLE #TEMP_PARAM_ETI(
    NAVE_ID NUMERIC(20,0),
    CALLE_ID Numeric(20, 0)
    )
INSERT INTO #TEMP_PARAM_ETI VALUES(14,NULL)


CREATE TABLE #NEW_ETI(
	ID_TMP                                       NUMERIC(20,0) IDENTITY(1,1) NOT NULL,
	[CLIENTE.CALLE]                              VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.CATEGORIA_CLIENTE_ID]               VARCHAR(5)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.CATEGORIA_IMPOSITIVA_ID]            VARCHAR(5)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.CLIENTE_ID]                         VARCHAR(15)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.CODIGO_POSTAL]                      VARCHAR(10)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.EMAIL]                              VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.FAX]                                VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.LOCALIDAD]                          VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.NOMBRE]                             VARCHAR(60)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.NRO_DOCUMENTO]                      VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS,
	[CLIENTE.NUMERO]                             VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS,
	[CLIENTE.OBSERVACIONES]                      VARCHAR(250)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.PAIS_ID]                            VARCHAR(5)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.PROVINCIA_ID]                       VARCHAR(5)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.RAZON_SOCIAL]                       VARCHAR(60)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.REMITO_ID]                          VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS,
	[CLIENTE.TELEFONO_1]                         VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.TELEFONO_2]                         VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.TELEFONO_3]                         VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[CLIENTE.ZONA_ID]                            VARCHAR(5)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.CANTIDAD]                 VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS,
	[DET_DOCUMENTO_EGR.CAT_LOG_ID_FINAL]         VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.DESCRIPCION]              VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.EST_MERC_ID]              VARCHAR(15)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.FECHA_VENCIMIENTO]        VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.NRO_BULTO]                VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.NRO_DESPACHO]             VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.NRO_LOTE]                 VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.NRO_PARTIDA]              VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.PRODUCTO_ID]              VARCHAR(30)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[DET_DOCUMENTO_EGR.UNIDAD_ID]                VARCHAR(5)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[PICKING.POSICION_COD]                       VARCHAR(45)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[PICKING.PROP1]                              VARCHAR(100)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[PICKING.RUTA]                               VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[PICKING.VIAJE_ID]                           VARCHAR(50)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[SUCURSAL.CALLE]                             VARCHAR(30)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[SUCURSAL.NOMBRE]                            VARCHAR(30)COLLATE SQL_Latin1_General_CP1_CI_AS ,
	[SUCURSAL.SUCURSAL_ID]                       VARCHAR(20)COLLATE SQL_Latin1_General_CP1_CI_AS 
)
*/
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
	Declare @Picking_Id	Numeric(20,0)
	Declare @cli_calle	as varchar(20)
	Declare @CATEGORIA_CLIENTE_ID as varchar(5)
	Declare @CATEGORIA_IMPOSITIVA_ID as varchar(5)
	Declare @CODIGO_POSTAL as varchar(10)
	Declare @EMAIL as varchar(50)
	Declare @FAX as varchar(20)
	Declare @LOCALIDAD as varchar(50)
	Declare @CLI_NOMBRE as varchar(60)
	Declare @NUMERO as varchar(20)
	Declare @NRO_DOCUMENTO as varchar(20)
	Declare @OBSERVACIONES as varchar(250)
	Declare @PAIS_ID as varchar(5)
	Declare @PROVINCIA_ID as varchar(5)
	Declare @RAZON_SOCIAL as varchar(60)
	Declare @REMITO_ID as varchar(20)
	Declare @TELEFONO_1 as varchar(20)
	Declare @TELEFONO_2 as varchar(20)
	Declare @TELEFONO_3 as varchar(20)
	Declare @ZONA_ID as varchar(5)
	Declare @SUC_CALLE as varchar(50)
	Declare @CAT_LOG_ID_FINAL as varchar(50)
	Declare @EST_MERC_ID as varchar(15)
	Declare @FECHA_VENCIMIENTO as varchar(20)
	Declare @NRO_DESPACHO as varchar(50)
	Declare @NRO_BULTO as varchar(20)
	Declare @NRO_LOTE as varchar(50)
	Declare @NRO_PARTIDA as varchar(50)
	Declare @UNIDAD_ID as varchar(50)
	Declare @NroLote as varchar(100)

	Set @terminal=Host_name()
	Select @Printer=Printer_id from sys_printer_default_etiqueta where Terminal_id=@terminal
	
	Set @Cur = Cursor for
		SELECT 	SP.PRODUCTO_ID, SP.DESCRIPCION, SUM(SP.CANTIDAD),SP.POSICION_COD,
				SP.PROP1,SP.RUTA,PROD.UNIDAD_ID, SP.CLIENTE_ID,	SP.HIJO,
				CASE WHEN CP.FLG_SOLICITA_LOTE='1' THEN ISNULL(DD.PROP2,NULL) ELSE NULL END LOTE
		FROM 	PICKING SP
				INNER JOIN PRIORIDAD_VIAJE SPV
				ON(LTRIM(RTRIM(UPPER(SPV.VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))
				INNER JOIN PRODUCTO PROD
				ON(PROD.CLIENTE_ID=SP.CLIENTE_ID AND PROD.PRODUCTO_ID=SP.PRODUCTO_ID)
				LEFT JOIN POSICION POS ON(SP.POSICION_COD=POS.POSICION_COD)
				INNER JOIN DET_DOCUMENTO DD ON(SP.DOCUMENTO_ID=DD.DOCUMENTO_ID AND SP.NRO_LINEA=DD.NRO_LINEA)
				INNER JOIN DOCUMENTO D ON(SP.DOCUMENTO_ID=D.DOCUMENTO_ID)
				INNER JOIN CLIENTE C ON(D.CLIENTE_ID=C.CLIENTE_ID)
				INNER JOIN CLIENTE_PARAMETROS CP ON(C.CLIENTE_ID=CP.CLIENTE_ID)
		WHERE 	SPV.PRIORIDAD = ( SELECT 	MIN(PRIORIDAD) FROM	PRIORIDAD_VIAJE	WHERE	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))))								
				AND (DBO.VERIFICA_PALLET_FINAL(SP.POSICION_COD,SP.VIAJE_ID,SP.RUTA, SP.PROP1)=@TIPO_PICK)
				AND	SP.VIAJE_ID IN(	SELECT 	VIAJE_ID 
									FROM  RL_VIAJE_USUARIO 
									WHERE 	LTRIM(RTRIM(UPPER(VIAJE_ID)))=LTRIM(RTRIM(UPPER(SP.VIAJE_ID))) 
									)
				AND SP.NAVE_COD	IN(	SELECT 	NAVE_COD
									FROM 	NAVE N INNER JOIN RL_USUARIO_NAVE RLNU
											ON(N.NAVE_ID=RLNU.NAVE_ID)
									WHERE	N.NAVE_COD=SP.NAVE_COD
									)
				--AND SP.FIN_PICKING <>'2'
				AND SP.POSICION_COD IN(	SELECT 	POSICION_COD
										FROM 	RL_VEHICULO_POSICION V INNER JOIN POSICION P ON(V.POSICION_ID=P.POSICION_ID)
												INNER JOIN CALLE_NAVE CN ON(P.CALLE_ID=CN.CALLE_ID)
												INNER JOIN NAVE NAV ON(P.NAVE_ID=NAV.NAVE_ID)
										WHERE 	1=1
												AND CN.CALLE_ID IN(SELECT CALLE_ID FROM #TEMP_PARAM_ETI WHERE NAVE_ID IS NULL)
										UNION 
										SELECT 	NAVE_COD AS POSICION_COD
										FROM	RL_VEHICULO_POSICION V INNER JOIN NAVE N2
												ON(V.NAVE_ID=N2.NAVE_ID)
										WHERE	1=1
												AND N2.NAVE_ID IN(SELECT nave_id FROM #TEMP_PARAM_ETI WHERE calle_id IS NULL)
										)
				AND SP.VIAJE_ID=@VIAJE_ID
		GROUP BY	SP.VIAJE_ID, SP.PRODUCTO_ID,SP.DESCRIPCION, SP.RUTA,SP.POSICION_COD,SP.TIPO_CAJA,SP.PROP1,PROD.UNIDAD_ID,SPV.PRIORIDAD,POS.ORDEN_PICKING
					, SP.CLIENTE_ID, PROD.VAL_COD_EGR, SP.HIJO,CP.FLG_SOLICITA_LOTE, DD.PROP2
		ORDER BY	SPV.PRIORIDAD ASC,SP.TIPO_CAJA DESC, POS.ORDEN_PICKING, SP.POSICION_COD ASC, SP.PRODUCTO_ID, DD.PROP2

	Open @Cur
	Set @ContDoc=0
	Set @Cont=0
	Fetch Next From @Cur Into 	@Prod,	@Desc,	@Qty,	@Pos,	@Prop1,	@Ruta,	@unidad,	@Cliente,	@Child, @NroLote
	While @@Fetch_Status=0
	Begin
		--Set @calle=Substring(@Pos,1,5)
		select	@cli_calle = calle,
				@CATEGORIA_CLIENTE_ID = CATEGORIA_CLIENTE_ID,
				@CATEGORIA_IMPOSITIVA_ID = CATEGORIA_IMPOSITIVA_ID,
				@CODIGO_POSTAL = CODIGO_POSTAL,
				@EMAIL = EMAIL,
				@FAX = FAX,
				@LOCALIDAD = LOCALIDAD,
				@CLI_NOMBRE = NOMBRE,
				@NRO_DOCUMENTO = NRO_DOCUMENTO,
				@NUMERO = CAST(NUMERO AS VARCHAR),
				@OBSERVACIONES = OBSERVACIONES,
				@PAIS_ID = PAIS_ID,
				@PROVINCIA_ID = PROVINCIA_ID,
				@RAZON_SOCIAL = RAZON_SOCIAL,
				@REMITO_ID = CAST(REMITO_ID AS VARCHAR),
				@TELEFONO_1 = TELEFONO_1,
				@TELEFONO_2 = TELEFONO_2,
				@TELEFONO_3 = TELEFONO_3,	
				@ZONA_ID = ZONA_ID
				from cliente where cliente_id = @Cliente

		Set @CurDoc=Cursor For
			Select	p.Documento_Id, p.Nro_Linea, p.Cantidad
			From	Picking p Inner join det_documento dd on(p.documento_id=dd.documento_id and p.nro_linea=dd.nro_linea)
			Where	Viaje_id=@VIAJE_ID and p.producto_id=@Prod and posicion_cod=@Pos 
					and ((@NroLote is null)or(dd.prop2=@NroLote))
					and ((@prop1 is null)or(p.prop1=@Prop1))
					and ruta=@Ruta and p.cliente_id=@Cliente
		Open @CurDoc
		Fetch Next from @CurDoc Into @Documento, @NroLinea, @QtyDoc
		While @@Fetch_Status=0
		Begin
			Select	@SucursalId=D.Sucursal_Destino, @SucCod=s.nombre, @Picking_id=P.Picking_Id, @SUC_CALLE = S.CALLE,@CAT_LOG_ID_FINAL = DD.CAT_LOG_ID_FINAL,
					@EST_MERC_ID = DD.EST_MERC_ID,@FECHA_VENCIMIENTO = CONVERT(VARCHAR,DD.FECHA_VENCIMIENTO,103),@NRO_BULTO = STR(DD.NRO_BULTO),
					@NRO_DESPACHO = DD.NRO_DESPACHO,@NRO_LOTE = DD.PROP2,@NRO_PARTIDA = DD.NRO_PARTIDA,@UNIDAD_ID = DD.UNIDAD_ID
			from	picking p inner join det_documento dd on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
					inner join documento d on(d.documento_id=dd.documento_id)
					inner join sucursal s on(d.cliente_id=s.cliente_id and d.sucursal_destino=s.sucursal_id)
			where	p.Documento_id=@Documento and p.nro_linea=@nrolinea

			While @ContDoc<=@QtyDoc -1
			Begin
				Set @ContDoc=@ContDoc+1
				Set @Cont=@Cont +1 

				If @Cont=1
				Begin
					INSERT INTO #NEW_ETI VALUES(@cli_calle,
												@CATEGORIA_CLIENTE_ID,
												@CATEGORIA_IMPOSITIVA_ID,
												@Cliente,
												@CODIGO_POSTAL,
												'COD: ' + Cast(@Child as varchar),
												@FAX,
												@LOCALIDAD,
												@CLI_NOMBRE,
												@NRO_DOCUMENTO,
												@NUMERO,
												@OBSERVACIONES,
												@PAIS_ID,
												@PROVINCIA_ID,
												@RAZON_SOCIAL,
												@REMITO_ID,
												'EP'+cast(@picking_id as varchar) + '-' +cast(@Cont as varchar),
												@TELEFONO_2,
												@TELEFONO_3,
												@ZONA_ID,
												@Qty,
												@CAT_LOG_ID_FINAL,
												@Desc,
												@EST_MERC_ID,
												@FECHA_VENCIMIENTO,
												'BULTO: ' + Cast(@Cont as varchar) +' de ' + cast(@Qty as varchar),
												@NRO_DESPACHO,
												@NroLote,--@NRO_LOTE,
												@NRO_PARTIDA,
												@Prod,
												@UNIDAD_ID,
												@Pos,
												@Prop1,
												@Ruta,
												@VIAJE_ID,
												@SucursalId,
												@SucCod,
												@SUC_CALLE)	
					/*INSERT INTO #NEW_ETI VALUES(@SucCod,'OS: ' + cast(@Documento as varchar),
												cast(@picking_id as varchar) + '-' +cast(@Cont as varchar),
												@Desc,'BULTO: ' + Cast(@Cont as varchar) +' de ' + cast(@Qty as varchar),
												'COD: ' + Cast(@Child as varchar), @Pos,@Printer,@Prod)	*/
				End
				If @Cont>1
				Begin
					INSERT INTO #NEW_ETI VALUES(@cli_calle,
												@CATEGORIA_CLIENTE_ID,
												@CATEGORIA_IMPOSITIVA_ID,
												@Cliente,
												@CODIGO_POSTAL,
												'',
												@FAX,
												@LOCALIDAD,
												@CLI_NOMBRE,
												@NRO_DOCUMENTO,
												@NUMERO,
												@OBSERVACIONES,
												@PAIS_ID,
												@PROVINCIA_ID,
												@RAZON_SOCIAL,
												@REMITO_ID,
												'EP'+cast(@picking_id as varchar) + '-' +cast(@Cont as varchar),
												@TELEFONO_2,
												@TELEFONO_3,
												@ZONA_ID,
												@Qty,
												@CAT_LOG_ID_FINAL,
												@Desc,
												@EST_MERC_ID,
												@FECHA_VENCIMIENTO,
												'BULTO: ' + Cast(@Cont as varchar) +' de ' + cast(@Qty as varchar),
												@NRO_DESPACHO,
												@NroLote,--@NRO_LOTE,
												@NRO_PARTIDA,
												@Prod,
												@UNIDAD_ID,
												@Pos,
												@Prop1,
												@Ruta,
												@VIAJE_ID,
												@SucursalId,
												@SucCod,
												@SUC_CALLE)	
					/*INSERT INTO #NEW_ETI VALUES(@SucCod,'OS: ' + cast(@Documento as varchar),
												cast(@picking_id as varchar) + '-' +cast(@Cont as varchar),
												@Desc,'BULTO: ' + Cast(@Cont as varchar) +' de ' + cast(@Qty as varchar),
												null, @Pos, @Printer,@Prod)	*/
				End								
			End
			set @ContDoc=0
			Fetch Next from @CurDoc Into @Documento, @NroLinea, @QtyDoc
		End
		Set @Cont=0
		Fetch Next From @Cur Into 	@Prod,	@Desc,	@Qty,	@Pos,	@Prop1,	@Ruta,	@unidad,	@Cliente,	@Child, @NroLote
	End
	Close @Cur
	Deallocate @Cur
	--SELECT * FROM #NEW_ETI

End --procedure

GO


