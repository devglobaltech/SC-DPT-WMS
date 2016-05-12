
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 04:40 p.m.
Please back up your database before running this script
*/

PRINT N'Synchronizing objects from V9 to CORAL'
GO

IF @@TRANCOUNT > 0 COMMIT TRANSACTION
GO

SET NUMERIC_ROUNDABORT OFF
SET ANSI_PADDING, ANSI_NULLS, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER ON
GO

IF EXISTS (SELECT * FROM tempdb..sysobjects WHERE id=OBJECT_ID('tempdb..#tmpErrors')) DROP TABLE #tmpErrors
GO

CREATE TABLE #tmpErrors (Error int)
GO

SET XACT_ABORT OFF
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
GO

BEGIN TRANSACTION
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[Mob_AbrirPalletCerrado]
@Pallet numeric(20,0)
as
begin
	update picking set pallet_cerrado='0' where pallet_final=@pallet

end
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[MOB_AFP]
@CLIENTE	VARCHAR(20),
@PRODUCTO	VARCHAR(50),
@VIAJE_ID	VARCHAR(30),
@TIPO		CHAR(1),
@PF			BIGINT,
@USUARIO	VARCHAR(30),
--------OPCIONALES--------
@QTYXCAMA	BIGINT=0,
@QTYCAMA	INT=0,
@QTYSUELTO	INT=0,
@TOTAL		BIGINT=0
AS
BEGIN
	--GENERALES
	DECLARE @PICKING_ID		NUMERIC(20,0)

	--PARA @TIPO='0'
	DECLARE @NEWPICK		NUMERIC(20,0)

	--PARA @TIPO='1'
	DECLARE @CURSOR			CURSOR
	DECLARE @SUMCONTROL		FLOAT
	DECLARE @VSUMCONTROL	VARCHAR(10)
	DECLARE @REMANENTE		FLOAT
	DECLARE @CANT_CONF		FLOAT
	DECLARE @QTY_CONT		FLOAT
	DECLARE @PC				CHAR(1)
	DECLARE @QTY			FLOAT
	SET XACT_ABORT ON
	
	IF @TIPO='0'
	BEGIN
		--Selecciono la primer linea que encuentre disponible
		Select	TOP 1
				@PICKING_ID=PICKING_ID
		From	Picking P (Nolock) 
		Where	p.Cliente_id=@Cliente and P.Producto_id=@Producto
				and p.viaje_id=@viaje_id
				and p.Cant_Confirmada>ISNULL(p.Qty_Controlado,0)
				and p.facturado<>'1' and fecha_inicio is not null and fecha_fin is not null
				and p.cant_confirmada>0
				and ((p.pallet_final is null) or (p.pallet_final=@PF))
				and ISNULL(p.pallet_cerrado,'0')<>'1'
		
		if @picking_ID is null
		begin
			-- Si es nulo me fijo si quedaron pendientes con pallet cerrado
			Select	TOP 1
					@PICKING_ID=PICKING_ID
			From	Picking P (Nolock) 
			Where	p.Cliente_id=@Cliente and P.Producto_id=@Producto
					and p.viaje_id=@viaje_id
					and p.Cant_Confirmada>ISNULL(p.Qty_Controlado,0)
					and p.facturado<>'1' and fecha_inicio is not null and fecha_fin is not null
					and cant_confirmada > 0
					and pallet_final<>@PF
					--and pallet_cerrado='1'

			if @Picking_id is not null
			Begin
				BEGIN TRANSACTION
				--Split de Picking, ya que tengo al menos un producto pendiente en pallet cerrado.
				INSERT INTO PICKING(DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, POSICION_COD, 
									RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
									USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
									TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, 
									USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, QTY_CONTROLADO, PALLET_FINAL, 
									PALLET_CERRADO,USUARIO_PF,TERMINAL_PF,REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS,BULTOS_NO_CONTROLADOS,NRO_LOTE,NRO_PARTIDA,NRO_SERIE)
				SELECT 	DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, 1, NAVE_COD, POSICION_COD, 
						RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, 1, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
						USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
						TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, 
						USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, 1, @PF, 
						PALLET_CERRADO,USUARIO_PF,TERMINAL_PF,REMITO_IMPRESO, NRO_REMITO_PF, ISNULL(PICKING_ID_REF, PICKING_ID), NULL,NULL,NRO_LOTE,NRO_PARTIDA,NRO_SERIE
				FROM 	PICKING
				WHERE	PICKING_ID=@Picking_id
				-- recupero el id insertado
				SELECT @NEWPICK=SCOPE_IDENTITY()
				--actualizo id para no mandar de mas (qty's).
				UPDATE PICKING SET CANTIDAD=CANTIDAD-1, CANT_CONFIRMADA=CANT_CONFIRMADA-1 WHERE PICKING_ID=@Picking_id

				COMMIT TRANSACTION
				SET @Picking_id=@NEWPICK
				UPDATE PICKING SET	PALLET_FINAL=@PF, USUARIO_PF=@USUARIO,
									TERMINAL_PF=HOST_NAME(), PALLET_CERRADO='0'
				WHERE  PICKING_ID=@PICKING_ID		
				RETURN
			End
			IF @PICKING_ID IS NULL
			BEGIN
				RaisError('No se encontro registros para Confirmar.',16,1)
				Return
			END
		end
		UPDATE PICKING SET	QTY_CONTROLADO=ISNULL(QTY_CONTROLADO,0)+1, PALLET_FINAL=@PF, USUARIO_PF=@USUARIO,
							TERMINAL_PF=HOST_NAME(), PALLET_CERRADO='0'
		WHERE  PICKING_ID=@PICKING_ID		
	END --FIN TIPO='0'

	IF @TIPO='1'
	BEGIN
		--CONTROLO LAS CANTIDADES A INGRESAR.
		SELECT	@SUMCONTROL=SUM(CANT_CONFIRMADA) - SUM(ISNULL(QTY_CONTROLADO,0))
		FROM	PICKING P
		WHERE	P.CLIENTE_ID			=@CLIENTE
				AND P.VIAJE_ID			=@VIAJE_ID
				AND P.PRODUCTO_ID		=@PRODUCTO
				AND P.CANT_CONFIRMADA	>ISNULL(P.QTY_CONTROLADO,0)
				AND P.FECHA_INICIO		IS NOT NULL
				AND P.FECHA_FIN			IS NOT NULL
				AND P.FACTURADO			='0'

		IF @SUMCONTROL<@TOTAL
		BEGIN
			SET @VSUMCONTROL=CAST(@SUMCONTROL AS VARCHAR)
			RAISERROR('La cantidad ingresada es mayor a la disponible para controlar. Disponible a controlar %s',16,1,@VSUMCONTROL)
			RETURN
		END
		ELSE
		BEGIN
			SET @CURSOR=CURSOR FOR
				SELECT	PICKING_ID
				FROM	PICKING P 
				WHERE	P.CLIENTE_ID			=@CLIENTE
						AND P.VIAJE_ID			=@VIAJE_ID
						AND P.PRODUCTO_ID		=@PRODUCTO
						AND P.CANT_CONFIRMADA	>ISNULL(P.QTY_CONTROLADO,0)
						AND P.FECHA_INICIO		IS NOT NULL
						AND P.FECHA_FIN			IS NOT NULL
						AND P.FACTURADO			='0'

			SET @REMANENTE=@TOTAL
			OPEN @CURSOR
			FETCH NEXT FROM @CURSOR INTO @PICKING_ID
			WHILE @@FETCH_STATUS=0
			BEGIN
				SELECT	@CANT_CONF=ISNULL(CANT_CONFIRMADA,0), @QTY_CONT=ISNULL(QTY_CONTROLADO,0),@PC=ISNULL(PALLET_CERRADO,0)
				FROM	PICKING 
				WHERE	PICKING_ID=@PICKING_ID
				
				IF(@CANT_CONF>=@QTY_CONT) AND (@PC='1')
				BEGIN
					INSERT INTO PICKING(DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANTIDAD, NAVE_COD, POSICION_COD, 
										RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA, PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
										USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
										TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, 
										USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, QTY_CONTROLADO, PALLET_FINAL, 
										PALLET_CERRADO,USUARIO_PF,TERMINAL_PF,REMITO_IMPRESO, NRO_REMITO_PF, PICKING_ID_REF, BULTOS_CONTROLADOS,BULTOS_NO_CONTROLADOS,NRO_LOTE,NRO_PARTIDA,NRO_SERIE)

					SELECT 	DOCUMENTO_ID, NRO_LINEA, CLIENTE_ID, PRODUCTO_ID, VIAJE_ID, TIPO_CAJA, DESCRIPCION, CANT_CONFIRMADA-ISNULL(QTY_CONTROLADO,0)
							, NAVE_COD, POSICION_COD, 
							RUTA, PROP1, FECHA_INICIO, FECHA_FIN, USUARIO, CANT_CONFIRMADA-ISNULL(QTY_CONTROLADO,0), PALLET_PICKING, SALTO_PICKING, PALLET_CONTROLADO, 
							USUARIO_CONTROL_PICK, ST_ETIQUETAS, ST_CAMION, FACTURADO, FIN_PICKING, ST_CONTROL_EXP, FECHA_CONTROL_PALLET, 
							TERMINAL_CONTROL_PALLET, FECHA_CONTROL_EXP, USUARIO_CONTROL_EXP, TERMINAL_CONTROL_EXP, FECHA_CONTROL_FAC, 
							USUARIO_CONTROL_FAC, TERMINAL_CONTROL_FAC, VEHICULO_ID, PALLET_COMPLETO, HIJO, CANT_CONFIRMADA-ISNULL(QTY_CONTROLADO,0), 999, 
							PALLET_CERRADO,USUARIO_PF,TERMINAL_PF,REMITO_IMPRESO, NRO_REMITO_PF, ISNULL(PICKING_ID_REF, PICKING_ID), NULL,NULL,NRO_LOTE,NRO_PARTIDA,NRO_SERIE
					FROM 	PICKING
					WHERE	PICKING_ID=@PICKING_ID

					-- recupero el id insertado
					SELECT @NEWPICK=SCOPE_IDENTITY()
					--actualizo id para no mandar de mas (qty's).
					UPDATE PICKING SET CANTIDAD=QTY_CONTROLADO, CANT_CONFIRMADA=QTY_CONTROLADO WHERE PICKING_ID=@Picking_id

					SET @Picking_id=@NEWPICK

					UPDATE PICKING SET	PALLET_FINAL=@PF, USUARIO_PF=@USUARIO,
										TERMINAL_PF=HOST_NAME(), PALLET_CERRADO='0'
					WHERE  PICKING_ID=@PICKING_ID	
				END
				IF(@CANT_CONF>=@QTY_CONT) AND (@PC='0')
				BEGIN
					IF @CANT_CONF>@REMANENTE
					BEGIN
						SET @QTY=@REMANENTE
						SET @REMANENTE=0
					END
					IF @CANT_CONF<=@REMANENTE
					BEGIN
						SET @QTY=@CANT_CONF -@QTY_CONT
						SET @REMANENTE=@REMANENTE-(@CANT_CONF-@QTY_CONT)
					END
					UPDATE PICKING SET	QTY_CONTROLADO=ISNULL(QTY_CONTROLADO,0) + @QTY,PALLET_FINAL=@PF, USUARIO_PF=@USUARIO,
										TERMINAL_PF=HOST_NAME(), PALLET_CERRADO='0'
					WHERE  PICKING_ID=@PICKING_ID					

				END
				IF @REMANENTE=0
				BEGIN
					BREAK
				END
				FETCH NEXT FROM @CURSOR INTO @PICKING_ID	
			END
			CLOSE @CURSOR
			DEALLOCATE @CURSOR
		END
	END	--FIN @TIPO='1'
END--FIN PROCEDURE.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER      PROCEDURE [dbo].[Mob_Busca_Usuario]
@usuario_id as nvarchar(20),
@password_handheld as nvarchar(50)
as

select  NOMBRE from SYS_USUARIO where RTRIM(LTRIM(upper(USUARIO_ID))) = upper(@usuario_id) and RTRIM(LTRIM(upper(password_handheld))) = upper(@password_handheld)
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[Mob_ConsultaStock]
@Codigo as nvarchar(100),
@TipoOperacion as integer,
@Cliente as varchar(15)
as


IF @TipoOperacion=1
BEGIN
SELECT X.ProductoID 
     ,cast(sum(X.cantidad)as int) AS Cantidad 
     ,X.EST_MERC_ID
     ,X.CategLogID
     ,X.Nro_Lote
     ,X.prop1 AS Property_1
     ,CONVERT(VARCHAR(23),X.Fecha_Vencimiento , 103) as Fecha_Vencimiento
     ,PR.DESCRIPCION AS PRODUCTO
FROM CLIENTE C, PRODUCTO PR 
     ,(SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID 
             ,cast(sum(rl.cantidad)as int) AS Cantidad 
             ,dd.unidad_id ,dd.moneda_id ,dd.costo 
             ,dd.nro_serie AS Nro_Serie 
             ,dd.Nro_lote AS Nro_Lote, dd.Fecha_vencimiento AS Fecha_Vencimiento 
             ,dd.Nro_Partida 
             ,dd.Nro_Despacho, dd.Nro_Bulto 
             ,dd.Prop1, dd.Prop2, dd.Prop3 
             ,dd.Peso ,dd.Unidad_Peso 
             ,dd.Volumen ,dd.Unidad_Volumen 
             ,prod.kit AS Kit 
             ,dd.tie_in AS TIE_IN, dd.nro_tie_in_padre AS  TIE_IN_PADRE 
             ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id 
             ,ISNULL(n.nave_cod,n2.nave_cod) AS Storage 
             ,ISNULL(rl.nave_actual,p.nave_id) as NaveID 
             ,ISNULL(caln.calle_cod,Null) AS CalleCod 
             ,ISNULL(caln.calle_id,Null) AS CalleID 
             ,ISNULL(coln.columna_cod,Null) AS ColumnaCod 
             ,ISNULL(coln.columna_id,Null) AS ColumnaID
             ,ISNULL(nn.nivel_cod,Null) AS NivelCod 
             ,ISNULL(nn.nivel_id,Null) AS NivelID 
             ,rl.cat_log_id as CategLogID 
     FROM 
         rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt 
         ON  rl.doc_trans_id=ddt.doc_trans_id AND rl.nro_linea_trans=ddt.nro_linea_trans 
         left join nave n  ON rl.nave_actual=n.nave_id 
         left join posicion p  ON rl.posicion_actual=p.posicion_id 
         left join nave n2 ON p.nave_id=n2.nave_id 
         left join calle_nave caln ON  p.calle_id=caln.calle_id 
         left join columna_nave coln ON p.columna_id=coln.columna_id 
         left join nivel_nave nn  ON p.nivel_id=nn.nivel_id 
         inner join det_documento dd ON ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea 
         inner join documento_transaccion dt ON ddt.doc_trans_id=dt.doc_trans_id
         inner join cliente c ON dd.cliente_id=c.cliente_id 
         inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id 
         inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id  AND rl.cliente_id=cl.cliente_id 
     WHERE 1<>0  
   AND dd.Cliente_ID = UPPER(LTRIM(RTRIM(@Cliente)))
   AND dd.prop1 = UPPER(LTRIM(RTRIM(@Codigo)))
GROUP BY dd.cliente_id ,dd.producto_id 
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote, dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3 
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,p.nave_id,n.nave_cod 
     ,n2.nave_cod ,caln.calle_cod ,caln.calle_id 
     ,coln.columna_cod,coln.columna_id ,nn.nivel_cod 
     ,nn.nivel_id,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
UNION ALL  
     SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID  
           ,cast(sum(rl.cantidad)as int) AS Cantidad  
           ,dd.unidad_id ,dd.moneda_id ,dd.costo  
           ,dd.nro_serie AS Nro_Serie  
           ,dd.Nro_lote AS Nro_Lote ,CONVERT(VARCHAR(23), dd.Fecha_vencimiento, 103) AS Fecha_Vencimiento  
           ,dd.Nro_Partida  
           ,dd.Nro_Despacho, dd.Nro_Bulto  
           ,dd.Prop1, dd.Prop2, dd.Prop3  
           ,cast(dd.Peso as float) AS Peso ,dd.Unidad_Peso  
           ,cast(dd.Volumen as float) AS Volumen,dd.Unidad_Volumen  
           ,prod.kit AS Kit  
           ,dd.tie_in AS TIE_IN  ,dd.nro_tie_in_padre AS  TIE_IN_PADRE  
           ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id  
           ,n.nave_cod AS Storage  
           ,rl.nave_actual as NaveID  
           ,null AS CalleCod  
           ,null AS CalleID  
           ,null AS ColumnaCod  
           ,null AS ColumnaID  
           ,null AS NivelCod  
           ,null AS NivelID 
           ,rl.cat_log_id as CategLogID  
     FROM  
           rl_det_doc_trans_posicion rl inner join det_documento dd  
           ON rl.documento_id=dd.documento_id AND rl.nro_linea=dd.nro_linea  
           left join nave n  ON rl.nave_actual=n.nave_id  
           inner join cliente c  ON dd.cliente_id=c.cliente_id  
           inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id  
           inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id AND rl.cliente_id=cl.cliente_id  
     WHERE 1<>0  
 AND dd.Cliente_ID = @Cliente
 AND dd.prop1 = @Codigo
GROUP BY dd.cliente_id ,dd.producto_id 
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote ,dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,n.nave_cod 
     ,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
     ) x 
WHERE C.CLIENTE_ID = X.CLIENTEID 
     AND PR.CLIENTE_ID = X.CLIENTEID 
     AND PR.PRODUCTO_ID = X.PRODUCTOID 
     group by X.ClienteID, X.ProductoID 
     ,X.Storage 
     ,X.NaveID 
     ,X.CalleCod 
     ,X.CalleID 
     ,X.ColumnaCod 
     ,X.ColumnaID 
     ,X.NivelCod 
     ,X.NivelID 
     ,X.EST_MERC_ID 
     ,X.CategLogID 
     ,X.Nro_Serie 
     ,X.Nro_Bulto 
     ,X.Nro_Lote 
     ,X.Nro_Despacho 
     ,X.Nro_Partida 
     ,X.prop1 
     ,X.prop2 
     ,X.prop3 
     ,X.Fecha_Vencimiento 
     ,X.Peso 
     ,X.Unidad_Peso 
     ,X.Volumen 
     ,X.Unidad_Volumen 
     ,X.Kit 
     ,X.TIE_IN  ,X.TIE_IN_PADRE 
     ,X.NRO_TIE_IN 
     ,C.RAZON_SOCIAL 
     ,PR.DESCRIPCION 
     ,X.unidad_id 
     ,X.moneda_id 
     ,x.costo 
END
ELSE
	BEGIN
	IF  @TipoOperacion=2
		BEGIN
		--CONSULTA UBICACION
		SELECT DD.CLIENTE_ID, DD.PRODUCTO_ID, cast(SUM(DD.CANTIDAD)as int) AS CANTIDAD_TOTAL, CONVERT(VARCHAR(23), DD.FECHA_VENCIMIENTO, 103) as FECHA_VENCIMIENTO, DD.NRO_LOTE
		FROM RL_DET_DOC_TRANS_POSICION RL INNER JOIN
	        POSICION P ON RL.POSICION_ACTUAL = P.POSICION_ID INNER JOIN
	        DET_DOCUMENTO_TRANSACCION DDT ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND 
	        RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS INNER JOIN
	        DOCUMENTO_TRANSACCION DT ON DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID INNER JOIN
	        DET_DOCUMENTO DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA
		WHERE     --(RL.POSICION_ACTUAL = @Codigo) SGG
			  P.POSICION_COD=UPPER(LTRIM(RTRIM(@Codigo)))
		GROUP BY DD.CLIENTE_ID, DD.PRODUCTO_ID, DD.FECHA_VENCIMIENTO, DD.NRO_LOTE
		END
	ELSE
BEGIN
		--CONSULTA PRODUCTO
SELECT X.ProductoID 
     ,cast(sum(X.cantidad)as int) AS Cantidad 
     ,X.EST_MERC_ID
     ,X.CategLogID
     ,X.Nro_Lote
     ,X.prop1 AS Property_1
     ,CONVERT(VARCHAR(23),X.Fecha_Vencimiento , 103) as Fecha_Vencimiento
     ,PR.DESCRIPCION AS PRODUCTO
FROM CLIENTE C, PRODUCTO PR 
     ,(SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID 
             ,cast(sum(rl.cantidad)as int) AS Cantidad 
             ,dd.unidad_id ,dd.moneda_id ,dd.costo 
             ,dd.nro_serie AS Nro_Serie 
             ,dd.Nro_lote AS Nro_Lote, dd.Fecha_vencimiento AS Fecha_Vencimiento 
             ,dd.Nro_Partida 
             ,dd.Nro_Despacho, dd.Nro_Bulto 
             ,dd.Prop1, dd.Prop2, dd.Prop3 
             ,cast(dd.Peso as float) as Peso,dd.Unidad_Peso 
             ,cast(dd.Volumen as float) as Volumen ,dd.Unidad_Volumen 
             ,prod.kit AS Kit 
             ,dd.tie_in AS TIE_IN, dd.nro_tie_in_padre AS  TIE_IN_PADRE 
             ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id 
             ,ISNULL(n.nave_cod,n2.nave_cod) AS Storage 
             ,ISNULL(rl.nave_actual,p.nave_id) as NaveID 
             ,ISNULL(caln.calle_cod,Null) AS CalleCod 
             ,ISNULL(caln.calle_id,Null) AS CalleID 
             ,ISNULL(coln.columna_cod,Null) AS ColumnaCod 
             ,ISNULL(coln.columna_id,Null) AS ColumnaID
             ,ISNULL(nn.nivel_cod,Null) AS NivelCod 
             ,ISNULL(nn.nivel_id,Null) AS NivelID 
             ,rl.cat_log_id as CategLogID 
     FROM 
         rl_det_doc_trans_posicion rl inner join det_documento_transaccion ddt 
         ON  rl.doc_trans_id=ddt.doc_trans_id AND rl.nro_linea_trans=ddt.nro_linea_trans 
         left join nave n  ON rl.nave_actual=n.nave_id 
         left join posicion p  ON rl.posicion_actual=p.posicion_id 
         left join nave n2 ON p.nave_id=n2.nave_id 
         left join calle_nave caln ON  p.calle_id=caln.calle_id 
         left join columna_nave coln ON p.columna_id=coln.columna_id 
         left join nivel_nave nn  ON p.nivel_id=nn.nivel_id 
         inner join det_documento dd ON ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea 
         inner join documento_transaccion dt ON ddt.doc_trans_id=dt.doc_trans_id
         inner join cliente c ON dd.cliente_id=c.cliente_id 
         inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id 
         inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id  AND rl.cliente_id=cl.cliente_id 
     WHERE 1<>0  
   AND dd.Cliente_ID = @Cliente
   AND dd.Producto_ID = @Codigo
GROUP BY dd.cliente_id ,dd.producto_id 
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote, dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3 
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,p.nave_id,n.nave_cod 
     ,n2.nave_cod ,caln.calle_cod ,caln.calle_id 
     ,coln.columna_cod,coln.columna_id ,nn.nivel_cod 
     ,nn.nivel_id,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
UNION ALL  
     SELECT dd.cliente_id AS ClienteID,dd.producto_id AS ProductoID  
           ,cast(sum(rl.cantidad)as int) AS Cantidad  
           ,dd.unidad_id ,dd.moneda_id ,dd.costo  
           ,dd.nro_serie AS Nro_Serie  
           ,dd.Nro_lote AS Nro_Lote ,CONVERT(VARCHAR(23),dd.Fecha_vencimiento, 103) AS Fecha_Vencimiento  
           ,dd.Nro_Partida  
           ,dd.Nro_Despacho, dd.Nro_Bulto  
           ,dd.Prop1, dd.Prop2, dd.Prop3  
           ,cast(dd.Peso as float) as Peso ,dd.Unidad_Peso  
           ,cast(dd.Volumen as float) as Volumen ,dd.Unidad_Volumen  
           ,prod.kit AS Kit  
           ,dd.tie_in AS TIE_IN  ,dd.nro_tie_in_padre AS  TIE_IN_PADRE  
           ,dd.nro_tie_in AS NRO_TIE_IN, RL.est_merc_id  
           ,n.nave_cod AS Storage  
           ,rl.nave_actual as NaveID  
           ,null AS CalleCod  
           ,null AS CalleID  
           ,null AS ColumnaCod  
           ,null AS ColumnaID  
           ,null AS NivelCod  
           ,null AS NivelID 
           ,rl.cat_log_id as CategLogID  
     FROM  
           rl_det_doc_trans_posicion rl inner join det_documento dd  
           ON rl.documento_id=dd.documento_id AND rl.nro_linea=dd.nro_linea  
           left join nave n  ON rl.nave_actual=n.nave_id  
           inner join cliente c  ON dd.cliente_id=c.cliente_id  
           inner join producto prod ON dd.cliente_id=prod.cliente_id and dd.producto_id=prod.producto_id  
           inner join categoria_logica cl ON rl.cat_log_id=cl.cat_log_id AND rl.cliente_id=cl.cliente_id  
     WHERE 1<>0    
 AND dd.Cliente_ID = UPPER(LTRIM(RTRIM(@Cliente)))
 AND dd.Producto_ID = UPPER(LTRIM(RTRIM(@Codigo)))
GROUP BY dd.cliente_id ,dd.producto_id 
     ,dd.unidad_id, dd.moneda_id, dd.costo 
     ,dd.Nro_Serie 
     ,dd.Nro_lote ,dd.Fecha_vencimiento 
     ,dd.Nro_Partida, dd.Nro_Despacho, dd.Nro_Bulto 
     ,dd.Prop1, dd.Prop2, dd.Prop3
     ,dd.Peso ,dd.unidad_peso 
     ,dd.Volumen ,dd.unidad_volumen 
     ,rl.nave_actual,n.nave_cod 
     ,prod.kit,dd.tie_in,dd.nro_tie_in_padre 
     ,dd.nro_tie_in , RL.est_merc_id 
     ,rl.cat_log_id 
     ) x 
WHERE C.CLIENTE_ID = X.CLIENTEID 
     AND PR.CLIENTE_ID = X.CLIENTEID 
     AND PR.PRODUCTO_ID = X.PRODUCTOID 
     group by X.ClienteID, X.ProductoID 
     ,X.Storage 
     ,X.NaveID 
     ,X.CalleCod 
     ,X.CalleID 
     ,X.ColumnaCod 
     ,X.ColumnaID 
     ,X.NivelCod 
     ,X.NivelID 
     ,X.EST_MERC_ID 
     ,X.CategLogID 
     ,X.Nro_Serie 
     ,X.Nro_Bulto 
     ,X.Nro_Lote 
     ,X.Nro_Despacho 
     ,X.Nro_Partida 
     ,X.prop1 
     ,X.prop2 
     ,X.prop3 
     ,X.Fecha_Vencimiento 
     ,X.Peso 
     ,X.Unidad_Peso 
     ,X.Volumen 
     ,X.Unidad_Volumen 
     ,X.Kit 
     ,X.TIE_IN  ,X.TIE_IN_PADRE 
     ,X.NRO_TIE_IN 
     ,C.RAZON_SOCIAL 
     ,PR.DESCRIPCION 
     ,X.unidad_id 
     ,X.moneda_id 
     ,x.costo 

	
END

END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER         PROCEDURE [dbo].[Mob_Eliminar_Locator_Ing]
@DOCUMENTO_ID	AS NUMERIC(20),
@NRO_LINEA		AS NUMERIC(20)

AS
BEGIN
	DECLARE @DOC 				AS INTEGER
	DECLARE @DOC_TRANS_ID	AS NUMERIC(20,0)
	DECLARE @PALLET			AS VARCHAR(100)

	SET 	@DOC = (	SELECT 	COUNT (DOCUMENTO_ID) 
					FROM 	SYS_LOCATOR_ING 
					WHERE 	DOCUMENTO_ID=@DOCUMENTO_ID
							AND NRO_LINEA = @NRO_LINEA
				)
/*
IF @DOC = 0
BEGIN
RAISERROR ('NO EXISTE EL DOCUMENTO EN LA BASE', 16, 1)
END
*/
	DELETE FROM SYS_LOCATOR_ING 
	WHERE DOCUMENTO_ID=@DOCUMENTO_ID
	AND NRO_LINEA = @NRO_LINEA

	DELETE 	FROM SYS_LOCATOR_ING 
	WHERE 	POSICION_ID IS NULL AND NAVE_ID IS NULL


	SELECT @PALLET=PROP1 FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA

	SELECT 	@DOC_TRANS_ID= DOC_TRANS_ID
	FROM	DET_DOCUMENTO DD 
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID AND DD.NRO_LINEA=@NRO_LINEA

	UPDATE SYS_LOCK_PALLET SET LOCK='0' WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND PALLET=@PALLET

END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  PROCEDURE [dbo].[MOB_FIN_TRANSFERENCIA]
	@PDOCTRANS 	NUMERIC(20,0),
	@USUARIO 	VARCHAR(20)
AS
BEGIN
	DECLARE @IORDEN 		AS NUMERIC(3,0)
	DECLARE @STATION 		AS VARCHAR(15)
	DECLARE @TRANSACCION_ID AS VARCHAR(15)
	DECLARE @STATUS			AS VARCHAR(3)
	DECLARE @FLG_FIN		AS CHAR(1)
	DECLARE @FLG_ACT_STOCK	AS CHAR(1)
	DECLARE @NEXT_STATION 	AS VARCHAR(15)
	DECLARE @NEXT_ORDEN		AS VARCHAR(15)

	--OBTENGO EL ORDEN DE LA ESTACION.
	SELECT 	@IORDEN=DBO.GETORDENESTACIONFORDOCTRID(@PDOCTRANS)

	SELECT 	@STATION=ESTACION_ACTUAL,@TRANSACCION_ID=TRANSACCION_ID,
			@STATUS=STATUS
	FROM  	DOCUMENTO_TRANSACCION
	WHERE 	DOC_TRANS_ID=@PDOCTRANS


	SELECT 	@FLG_FIN=FIN, @FLG_ACT_STOCK=ACTUALIZA_STOCK
	FROM  	RL_TRANSACCION_ESTACION
	WHERE 	TRANSACCION_ID 	=@TRANSACCION_ID
	     	AND ESTACION_ID	=@STATION
	     	AND ORDEN		=@IORDEN

	/*
	SELECT
			TRANSACCION_ID,
			ESTACION_ACTUAL,
			ORDEN_ESTACION
	FROM 	DOCUMENTO_TRANSACCION
	WHERE 	DOC_TRANS_ID = @PDOCTRANS
			AND TRANSACCION_ID = @TRANSACCION_ID
			AND (STATUS = 'T10' OR STATUS = 'T20')

	SELECT
				RTE.TRANSACCION_ID,
				RTE.ESTACION_ID,
				RTE.ORDEN
	FROM 		RL_TRANSACCION_ESTACION RTE
	WHERE 		RTE.TRANSACCION_ID = @TRANSACCION_ID
	ORDER BY 	TRANSACCION_ID, ORDEN
	*/
	EXEC DBO.UPDATEESTACIONACTUAL_STOCK_TRANS	@DOC_TRANS_ID=@PDOCTRANS, @USUARIO=@USUARIO

	EXEC DBO.UPDATEESTACIONACTUAL  @DOC_TRANS_ID=@PDOCTRANS


END --FIN DEL PROCEDURE
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  Procedure [dbo].[Mob_Get_Printers]
As
Begin

	Select 	Device, Descripcion	 
	From 	sys_impresora
	Where	Activa='1'
	Order By
			Orden	

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  Procedure [dbo].[Mob_GetCantEnvase]
				@Cliente_Id as varchar(15),
				@Variable as numeric(4,0) Output
As
Begin

	Select 	@Variable = Count(*)
	from 	producto
	where 	Cliente_id=@cliente_id and envase='1'

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER   Procedure [dbo].[Mob_GetClient]
				@Viaje_Id as varchar(50),
				@Pallet_Picking as numeric(20,0),
				@Cliente_ID as varchar(15) output
As
Begin

	IF @Pallet_Picking<>0
	BEGIN
		Select @Cliente_ID = Cliente_ID
		From picking 
		Where VIAJE_ID = @Viaje_Id 
			and PALLET_PICKING = @Pallet_Picking
	END 
	ELSE
	BEGIN
		Select DISTINCT @Cliente_ID = Cliente_ID
		From picking 
		Where VIAJE_ID = @Viaje_Id 

	END
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[Mob_GetDocTransId]
@DocumentoId 	as Numeric(20,0),
@DocTransId  	as Numeric(20,0) Output
As
Begin

	Select	@DocTransId=Doc_trans_id
	from	Det_Documento_Transaccion
	where	Documento_id=@DocumentoId

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER       Procedure [dbo].[Mob_GetProdDescCant]
				@Viaje AS varchar(30) output,
				@Secuencia as numeric(20,0)Output

As
Begin

	Declare @ValorSequencia as Numeric(20,0)
	/*
	exec Get_Value_For_Sequence 'VALE_ENVASE', @ValorSequencia Output
	*/
	Select 	P.Producto_id, P.Descripcion, sum(DD.Cantidad) as CANTIDAD,
			d.nro_despacho_importacion as Viaje_ID, @secuencia as Numero,dbo.fx_trunc_Fecha(Getdate()) as Fecha
	From 	Producto P
			inner join det_documento DD on (DD.Producto_id = P.Producto_id)
			inner join documento D on (D.Documento_id = DD.documento_id)
	where 	P.Envase = '1' and d.NRO_DESPACHO_IMPORTACION = ltrim(rtrim(Upper(@Viaje)))
	Group by P.Producto_id, P.Descripcion,d.nro_despacho_importacion
	

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER OFF
GO

ALTER     Procedure [dbo].[Mob_GetProductoEnvase]
	@Cliente_Id as varchar(15)
As
Begin
	Select producto_id as PRODUCTO_ID,descripcion AS DESCRIPCION,0 as QTY
	From producto 
	Where	cliente_id = @Cliente_Id and envase='1'
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  Procedure [dbo].[Mob_GrabarDocumento]
@Viaje_Id 			as varchar(100),
@Documento_Id		as Numeric(20,0)
As
Begin
	
	Declare @Seq	as Numeric(20,0)

	Exec Get_Value_For_Sequence 'VALE_ENVASE', @Seq Output
	
	Insert Into Rl_Env_Documento_Viaje 	(Viaje_id,Documento_Id,Nro_Vale) values	(Ltrim(Rtrim(Upper(@Viaje_id)))	,@Documento_id	,@Seq)

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[Mob_Guardado_Ing_Detalle]
	@Cod_Producto	Varchar(30),
	@Cliente_Id		Varchar(15),
	@Documento_Id	Numeric(20,0),
	@Cantidad		Numeric(20,5)
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	DECLARE @Descripcion Varchar(200)
	DECLARE @Unidad_Id Varchar(5)
	DECLARE @Cat_Logica_Id Varchar(50)
	DECLARE @COUNT AS INT
	DECLARE @NRO_LINEA NUMERIC(10,0)
	DECLARE @NROLINEADOC NUMERIC(10,0)
	DECLARE @CANTIDADDOC NUMERIC(20,5)
	DECLARE @VBEGINDELETE CHAR(1)
	DECLARE @CANTIDAD_TOTAL NUMERIC(20,5) 
	
	
	SET @NRO_LINEA = 1
		
		
	
	SELECT @Descripcion = P.DESCRIPCION, @Unidad_Id =UM.UNIDAD_ID,
			@Cat_Logica_Id =CL.CAT_LOG_ID 
		FROM RL_PRODUCTO_CATLOG RLC INNER JOIN CATEGORIA_LOGICA CL
			ON RLC.CAT_LOG_ID=CL.CAT_LOG_ID
		INNER JOIN PRODUCTO P ON RLC.PRODUCTO_ID = P.PRODUCTO_ID
		AND P.CLIENTE_ID = RLC.CLIENTE_ID
		INNER JOIN UNIDAD_MEDIDA UM ON P.UNIDAD_ID = UM.UNIDAD_ID
	    WHERE RLC.CLIENTE_ID = @Cliente_Id and RLC.PRODUCTO_ID = @Cod_Producto

	IF @Descripcion IS NULL 
	BEGIN
	SELECT @Descripcion = P.DESCRIPCION, @Unidad_Id= UM.UNIDAD_ID,@Cat_Logica_Id=C.CAT_LOG_ID
		FROM PRODUCTO P INNER JOIN UNIDAD_MEDIDA UM 
		ON P.UNIDAD_ID = UM.UNIDAD_ID
		INNER JOIN CATEGORIA_LOGICA C ON
		P.ING_CAT_LOG_ID = C.CAT_LOG_ID
		AND P.CLIENTE_ID = C.CLIENTE_ID
		WHERE P.PRODUCTO_ID=@Cod_Producto AND P.CLIENTE_ID=@Cliente_Id
	
	END
	
	SELECT @COUNT=COUNT(*)
	FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@Documento_Id
	
	IF @COUNT>0 
	BEGIN
		SELECT @NRO_LINEA = NRO_LINEA +1 
		FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@Documento_Id
		
		INSERT INTO DET_DOCUMENTO(DOCUMENTO_ID,NRO_LINEA,CLIENTE_ID,PRODUCTO_ID,CANTIDAD,NRO_SERIE,
				NRO_SERIE_PADRE,EST_MERC_ID,CAT_LOG_ID,NRO_BULTO,DESCRIPCION,NRO_LOTE,FECHA_VENCIMIENTO,
				NRO_DESPACHO,NRO_PARTIDA,UNIDAD_ID,PESO,UNIDAD_PESO,VOLUMEN,UNIDAD_VOLUMEN,BUSC_INDIVIDUAL,
				TIE_IN,NRO_TIE_IN_PADRE,NRO_TIE_IN,ITEM_OK,CAT_LOG_ID_FINAL,MONEDA_ID,COSTO,PROP1,PROP2,
				PROP3,LARGO,ALTO,ANCHO,VOLUMEN_UNITARIO,PESO_UNITARIO,CANT_SOLICITADA,TRACE_BACK_ORDER)
			VALUES(@Documento_Id,@NRO_LINEA,@Cliente_Id,@Cod_Producto,@Cantidad,NULL,
				NULL,NULL,NULL,NULL,@Descripcion,NULL,NULL,
				NULL,NULL,@Unidad_Id,NULL,NULL,NULL,NULL,'1',
				'0',NULL,NULL,'1',@Cat_Logica_Id,NULL,NULL,NULL,NULL,
				NULL,NULL,NULL,NULL,NULL,NULL,@Cantidad,NULL)
				
		SELECT @COUNT=COUNT(*),@CANTIDAD_TOTAL= SUM(CANTIDAD)
		FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@Documento_Id AND PRODUCTO_ID=@Cod_Producto
		
		IF 	@COUNT>1
		BEGIN
			SELECT @NROLINEADOC= MIN(NRO_LINEA)
				FROM DET_DOCUMENTO WHERE DOCUMENTO_ID= @Documento_Id AND PRODUCTO_ID=@Cod_Producto
			
			UPDATE DET_DOCUMENTO SET CANTIDAD=@CANTIDAD_TOTAL, CANT_SOLICITADA=@CANTIDAD_TOTAL
				WHERE DOCUMENTO_ID=@Documento_Id AND PRODUCTO_ID=@Cod_Producto AND NRO_LINEA=@NROLINEADOC	
				
			DELETE FROM DET_DOCUMENTO WHERE DOCUMENTO_ID=@Documento_Id AND PRODUCTO_ID=@Cod_Producto AND
				NRO_LINEA<>@NROLINEADOC
		
		END
	END	
	ELSE
	BEGIN
		INSERT INTO DET_DOCUMENTO(DOCUMENTO_ID,NRO_LINEA,CLIENTE_ID,PRODUCTO_ID,CANTIDAD,NRO_SERIE,
					NRO_SERIE_PADRE,EST_MERC_ID,CAT_LOG_ID,NRO_BULTO,DESCRIPCION,NRO_LOTE,FECHA_VENCIMIENTO,
					NRO_DESPACHO,NRO_PARTIDA,UNIDAD_ID,PESO,UNIDAD_PESO,VOLUMEN,UNIDAD_VOLUMEN,BUSC_INDIVIDUAL,
					TIE_IN,NRO_TIE_IN_PADRE,NRO_TIE_IN,ITEM_OK,CAT_LOG_ID_FINAL,MONEDA_ID,COSTO,PROP1,PROP2,
					PROP3,LARGO,ALTO,ANCHO,VOLUMEN_UNITARIO,PESO_UNITARIO,CANT_SOLICITADA,TRACE_BACK_ORDER)
				VALUES(@Documento_Id,@NRO_LINEA,@Cliente_Id,@Cod_Producto,@Cantidad,NULL,
					NULL,NULL,NULL,NULL,@Descripcion,NULL,NULL,
					NULL,NULL,@Unidad_Id,NULL,NULL,NULL,NULL,'1',
					'0',NULL,NULL,'1',@Cat_Logica_Id,NULL,NULL,NULL,NULL,
					NULL,NULL,NULL,NULL,NULL,NULL,@Cantidad,NULL)
					
					UPDATE DOCUMENTO SET STATUS='D10' WHERE DOCUMENTO_ID=@Documento_Id

	END
	
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[Mob_Guardado_Prod]
	@DOCUMENTO_ID	NUMERIC(20,0),
	@CLIENTE		VARCHAR(30),
	@PRODUCTO		VARCHAR(30) OUTPUT,		--OK
	@DESCRIPCION	VARCHAR(50) OUTPUT,		--OK
	@LINEA			SMALLINT	OUTPUT,		--OK
	@QTY			FLOAT		OUTPUT,		
	@FRACC			CHAR(1)		OUTPUT		--OK
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	--DECLARACION DE VARIABLES.
	DECLARE @COUNT	SMALLINT
	
	--PRIMERO TENGO QUE SABER SI ES EL PRODUCTO O UN EAN/DUN.
	SELECT	@COUNT=COUNT(*)
	FROM	PRODUCTO
	WHERE	CLIENTE_ID=@CLIENTE
			AND PRODUCTO_ID=@PRODUCTO
	
	IF @COUNT=0
	BEGIN
		--NO ES UN PRODUCTO, TENGO QUE SACAR EL EAN/DUN.
		SET @COUNT=NULL
		SELECT	@COUNT=COUNT(*)
		FROM	RL_PRODUCTO_CODIGOS
		WHERE	CLIENTE_ID=@CLIENTE
				AND CODIGO=@PRODUCTO
			 
		
		IF @COUNT=0
		BEGIN
			--NO EXISTE EL PRODUCTO Y EL CODIGO ES INVALIDO.
			RAISERROR('El producto ingresado es inexistente.',16,1)
			RETURN
		END
		ELSE
		BEGIN
			--ENCONTRE EL PRODUCTO A PARTIR DEL CODIGO Y RECUPERO EL PRODUCTO_ID.
			SELECT	@PRODUCTO=PRODUCTO_ID 
			FROM	RL_PRODUCTO_CODIGOS
			WHERE	CLIENTE_ID=@CLIENTE
					AND CODIGO=@PRODUCTO
		END
	END
	--SACO LA DESCRIPCION DEL PRODUCTO Y SI ES O NO FRACCIONABLE.
	SELECT	@DESCRIPCION=DESCRIPCION, @FRACC=ISNULL(FRACCIONABLE,'0')
	FROM	PRODUCTO
	WHERE	CLIENTE_ID=@CLIENTE
			AND PRODUCTO_ID=@PRODUCTO
	
	--OBTENGO EL NUMERO DE LINEA DEL DOCUMENTO.
	SELECT	TOP 1 
			@LINEA=DD.NRO_LINEA
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.CLIENTE_ID=@CLIENTE
			AND DD.PRODUCTO_ID=@PRODUCTO
			AND RL.NAVE_ACTUAL='1'
			AND RL.CAT_LOG_ID='TRAN_ING'
	ORDER BY
			DD.NRO_LINEA ASC;
			
	--OBTENGO LA CANTIDAD A UBICAR DEL PRODUCTO.
	SELECT	@QTY=SUM(RL.CANTIDAD)
	FROM	DET_DOCUMENTO DD INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DD.DOCUMENTO_ID=DDT.DOCUMENTO_ID AND DD.NRO_LINEA=DDT.NRO_LINEA_DOC)
			INNER JOIN RL_DET_DOC_TRANS_POSICION RL
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
	WHERE	DD.DOCUMENTO_ID=@DOCUMENTO_ID
			AND DD.PRODUCTO_ID=@PRODUCTO
			AND DD.NRO_LINEA=@LINEA
			AND RL.NAVE_ACTUAL='1'
			AND RL.CAT_LOG_ID='TRAN_ING'
		
	IF (@QTY IS NULL) OR (@QTY=0)
	BEGIN
		RAISERROR('No hay pendientes de ubicacion para el producto seleccionado.',16,1)
		return
	END
END--FIN PROCEDURE.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[Mob_Guardado_ValProducto]
	@Cod_Producto	Varchar(50)output,
	@Cliente_Id		Varchar(15) output,
	@Descripcion	Varchar(200) output,
	@Unidad			Varchar(50) output,
	@Cat_Logica		Varchar(50) output,
	@Producto_Id	Varchar(30) output,
	@EsFraccionable	Char(1) output
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	DECLARE @CONT SMALLINT

	
	SELECT	@CONT= COUNT(*)
	FROM	PRODUCTO
	WHERE	CLIENTE_ID = @CLIENTE_ID AND PRODUCTO_ID=@Cod_Producto
			
	
	IF @CONT=0
	BEGIN
		SELECT	@CONT= COUNT(*)
		FROM	RL_PRODUCTO_CODIGOS
		WHERE	CLIENTE_ID = @CLIENTE_ID AND CODIGO=@Cod_Producto AND CLIENTE_ID= @Cliente_Id
		
		IF  @CONT=0
		BEGIN
			RAISERROR('El Producto ingresado no existe',16,1)
			RETURN
		END
		
		IF  @CONT>1
		BEGIN
			RAISERROR('Para el Codigo del Producto ingresado existe más de un registro',16,1)
			RETURN		
		END
		
		IF  @CONT=1
		BEGIN
			SELECT @Cod_Producto=PRODUCTO_ID
			FROM RL_PRODUCTO_CODIGOS
			WHERE	CLIENTE_ID = @CLIENTE_ID AND CODIGO=@Cod_Producto AND CLIENTE_ID= @Cliente_Id
		END
	
	END		
	
		
SELECT	@Descripcion = P.DESCRIPCION
		,@Unidad =UM.DESCRIPCION
		,@Cat_Logica =CL.DESCRIPCION
		,@Producto_Id =P.PRODUCTO_ID
		,@EsFraccionable=ISNULL(P.FRACCIONABLE,'0')
FROM RL_PRODUCTO_CATLOG RLC
INNER JOIN CATEGORIA_LOGICA CL ON RLC.CAT_LOG_ID=CL.CAT_LOG_ID AND RLC.CLIENTE_ID=CL.CLIENTE_ID
INNER JOIN PRODUCTO P ON RLC.PRODUCTO_ID = P.PRODUCTO_ID AND P.CLIENTE_ID = RLC.CLIENTE_ID
INNER JOIN UNIDAD_MEDIDA UM ON P.UNIDAD_ID = UM.UNIDAD_ID
WHERE	RLC.CLIENTE_ID = @Cliente_Id
		and RLC.PRODUCTO_ID = @Cod_Producto

	IF @Descripcion IS NULL 
	BEGIN
		SELECT	@Descripcion = P.DESCRIPCION
				,@Unidad= UM.DESCRIPCION
				,@Cat_Logica=C.DESCRIPCION
				,@Producto_Id =P.PRODUCTO_ID
				,@EsFraccionable=ISNULL(P.FRACCIONABLE,'0')
			FROM PRODUCTO P
			INNER JOIN UNIDAD_MEDIDA UM ON P.UNIDAD_ID = UM.UNIDAD_ID
			INNER JOIN CATEGORIA_LOGICA C ON P.ING_CAT_LOG_ID = C.CAT_LOG_ID AND P.CLIENTE_ID = C.CLIENTE_ID
			WHERE	P.CLIENTE_ID = @CLIENTE_ID
					AND P.PRODUCTO_ID=@Cod_Producto

	END
  END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER         Procedure [dbo].[Mob_IngresarViajes_Controlado]
	@ViajeId As Varchar(100)
As
Begin

	Select 	Distinct
			ISNULL(cast(Pallet_Picking as varchar),'') AS NRO_PALLET
	From	Picking (nolock)
	Where	Viaje_Id=Ltrim(Rtrim(Upper(@ViajeID)))
			And St_Control_Exp='1'
	Group By
			Pallet_Picking
	having	sum(cant_confirmada)>0

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER         Procedure [dbo].[Mob_IngresarViajes_Pendiente]
	@ViajeId As Varchar(100)
As
Begin

	Select 	
			ISNULL(cast(Pallet_Picking as varchar),'') As NRO_PALLET
	From	Picking (nolock)
	Where	Viaje_Id=Ltrim(Rtrim(Upper(@ViajeId)))
			And isnull(St_Control_Exp,'0')='0'
	Group by pallet_picking
	Having 	 sum(cant_confirmada)>0

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[MOB_INGRESO_OC_SEL]
	@CLIENTE_ID		varchar(15),
	@ORDEN_COMPRA	varchar(100)
AS

SELECT     INGRESO_OC.PRODUCTO_ID as PRODUCTO, INGRESO_OC.CANTIDAD AS CANTIDAD, ISNULL(INGRESO_OC.CANT_CONTENEDORAS,0) AS CANT_CONTENEDORAS, PRODUCTO.DESCRIPCION as DESCRIPCION, INGRESO_OC.NRO_LOTE AS NRO_LOTE, INGRESO_OC.NRO_PARTIDA AS NRO_PARTIDA
FROM         INGRESO_OC INNER JOIN
                      PRODUCTO ON INGRESO_OC.PRODUCTO_ID = PRODUCTO.PRODUCTO_ID 
			-- Catalina Castillo.24/01/2012.Se agrega filtro con cliente_id porque estan saliendo
			-- mas registros sin filtrar.
			AND INGRESO_OC.CLIENTE_ID = PRODUCTO.CLIENTE_ID
WHERE     (INGRESO_OC.CLIENTE_ID = @CLIENTE_ID) AND (INGRESO_OC.ORDEN_COMPRA = @ORDEN_COMPRA and procesado ='0')
ORDER BY INGRESO_OC.PRODUCTO_ID
	/* SET NOCOUNT ON */ 
	/*RETURN*/
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   procedure [dbo].[Mob_IngVerificaIntermedia]
@Doc_trans_id numeric(20,0) output,
@Out int output
As
Begin

	Declare @vRlId  as Numeric(20,0)
	Declare @Q1		as int
	Declare @Q2		as int
	Declare @Return as int

	Declare C_VerIntIng cursor For
		Select 	Rl_id
		from	Rl_Det_Doc_trans_posicion rl inner join Det_documento_transaccion ddt
				on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans =ddt.nro_linea_trans)
		Where	ddt.doc_trans_id=@Doc_trans_id


	Open C_VerIntIng
		
	Fetch Next from C_VerIntIng Into @vRlId
	While @@Fetch_Status=0
		Begin
		
			SELECT 	@Q1=COUNT(RL_ID)
			FROM	RL_DET_DOC_TRANS_POSICION RL
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ACTUAL=N.NAVE_ID)
					LEFT JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE	RL.RL_ID=@vRlId
					AND N.INTERMEDIA='1'
		
		
		
		
			SELECT 	@Q2=COUNT(RL_ID)
			FROM	RL_DET_DOC_TRANS_POSICION RL
					INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
					ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
					INNER JOIN DET_DOCUMENTO DD
					ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
					LEFT JOIN NAVE N
					ON(RL.NAVE_ACTUAL=N.NAVE_ID)
					LEFT JOIN POSICION P
					ON(RL.POSICION_ACTUAL=P.POSICION_ID)
			WHERE	RL.RL_ID=@vRlId
					AND P.INTERMEDIA='1'
		
			
		
			If @Q1=1 Or @Q2=1
				Begin
					set @Return=1
					Break
				End
			Else
				Begin
					set @Return=0
				End
	
			Fetch Next from C_VerIntIng Into @vRlId
					
		End --Fin While
	set @Out=@Return

	Close C_VerIntIng
	deallocate C_VerIntIng
End --Fin Procedure
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER Procedure [dbo].[MOB_INSERT_SERIE_PICKING]
	@Picking_id NUMERIC(20,0) OUTPUT,
	@Nro_Serie VARCHAR(100) OUTPUT
AS
BEGIN

INSERT INTO [Produ].[dbo].[SeriePicking]
           ([Picking_id]
           ,[Nro_Serie]
           ,[Fecha])
     VALUES
           (@Picking_id
           ,@Nro_Serie
           ,GETDATE())

END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  PROCEDURE [dbo].[Mob_Permisos_Menu]
@usuario_id as nvarchar(20),
@codigo_id as integer
as

SELECT CODIGO_MENU
FROM SYS_PERMISOS_HH
WHERE (USUARIO_ID = @usuario_id AND CODIGO_MENU = @codigo_id)
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[Mob_Pwd_Correcto]
@password_handheld as nvarchar(50)
as

select NOMBRE from SYS_USUARIO where  RTRIM(LTRIM(upper(password_handheld))) = upper(@password_handheld)
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[Mob_Transf_Verifica_Cat_Log]
--@Pallet			as varchar(100),
@Posicion		as varchar(45),
@Producto_id	as varchar(30),
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
		WHERE	(RL.NAVE_ACTUAL	 =	(	SELECT 	NAVE_ID		FROM NAVE 		WHERE	NAVE_COD	=LTRIM(RTRIM(UPPER(@Posicion))))
				OR RL.POSICION_ACTUAL=	(	SELECT 	POSICION_ID	FROM POSICION 	WHERE	POSICION_COD=LTRIM(RTRIM(UPPER(@Posicion)))))
				and dd.producto_id=@Producto_id
				AND RL.CANTIDAD >0
	--PROP1=LTRIM(RTRIM(UPPER(@PALLET))) AND 
	
	Open @Catlog
	Fetch Next From @CatLog into @vCatLog
	While @@Fetch_Status=0
	Begin
		IF (@vCatLog='TRAN_ING')OR(@vCatLog='TRAN_EGR')
		BEGIN
			Set @Transfiere=0
			RAISERROR('1- No es posible Transferir con Categoria Logica %s.',16,1,@vCatLog)
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

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  Procedure [dbo].[Mob_Transf_VerificaCatLog]
@Pallet			as varchar(100),
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
		WHERE	PROP1=LTRIM(RTRIM(UPPER(@PALLET))) 
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
			RAISERROR('1- No es posible Transferir un pallet con Categoria Logica %s.',16,1,@vCatLog)
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

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[MOB_TRANSFERENCIA_PROD]
	@CLIENTE_ID AS VARCHAR(50),
	@POSICION_O	AS VARCHAR(45),
	@POSICION_D AS VARCHAR(45),
	@Producto_id as varchar(30),
	@USUARIO 	AS VARCHAR(30),
	@Cantidad	numeric(20,5),
	@CAT_LOG_ID VARCHAR(100)
AS
BEGIN
	DECLARE @vDocNew		AS NUMERIC(20,0)
	DECLARE @Producto		AS VARCHAR(50)
	DECLARE @vPOSLOCK		AS INT
	DECLARE @vDOCLOCK		AS NUMERIC(20,0)
	DECLARE @vRLID			AS NUMERIC(20,0)
	DECLARE @VCANTIDAD		AS NUMERIC(20,5)
	DECLARE @ICANTIDAD		AS NUMERIC(20,5)
	DECLARE @DIFERENCIA		AS NUMERIC(20,5)
	DECLARE @CAT_LOG_FIN	AS VARCHAR(50)
	DECLARE @DISP_TRANS		AS CHAR(1)
	DECLARE @CONT_LINEA		AS NUMERIC(10,0)
	DECLARE @NEWNAVE		AS NUMERIC(20,0)
	DECLARE @NEWPOS			AS NUMERIC(20,0)
	DECLARE @EXISTE			AS NUMERIC(1,0)	
	DECLARE @LIM_CONT		AS NUMERIC(20,0)
	DECLARE @CONTROL		AS INT
	DECLARE @OUT			AS CHAR(1)
	DECLARE @CAT_LOG		AS VARCHAR(30)
	DECLARE @DISP_TRANF		AS INT
	DECLARE @PICKING		AS CHAR(1)
	DECLARE @TRANSFIERE		AS CHAR(1)	
	DECLARE @vNEW_RLID		AS NUMERIC(20,0)
	declare @msg			as varchar(max)
	DECLARE @CANT_ORIG		AS numeric(20,5)	


	begin try
		SET @CANT_ORIG = @Cantidad

		EXEC VERIFICA_LOCKEO_POS @POSICION_D,@OUT
		IF @OUT='1'
			BEGIN
				RETURN
			END

		DECLARE CUR_RL_TR CURSOR FOR
		SELECT rl.rl_id
		FROM rl_det_doc_trans_posicion rl
			inner join	det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
			inner join	det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
			left join	posicion p on (rl.posicion_actual=p.posicion_id)
			left join	nave n on (rl.nave_actual=n.nave_id)
			inner join	categoria_logica cl on (rl.cat_log_id=cl.cat_log_id and cl.disp_transf='1' and cl.cliente_id =dd.cliente_id)
			left join	estado_mercaderia_rl em on (rl.est_merc_id=em.est_merc_id and em.cliente_id=dd.cliente_id and (em.disp_transf='1' or em.disp_transf is null))		
		WHERE	dd.producto_id=@Producto_id
		and (n.nave_cod=@POSICION_O or p.posicion_cod=@POSICION_O) and rl.cantidad > 0
		and rl.disponible='1'	
		AND RL.CAT_LOG_ID = @CAT_LOG_ID
		AND RL.CLIENTE_ID = @CLIENTE_ID
		and rl.doc_trans_id_egr is null and rl.nro_linea_trans_egr is null
		and rl.doc_trans_id_tr is null and rl.nro_linea_trans_tr is null
		and rl.documento_id is null and rl.nro_linea is null



			

			
			SELECT @EXISTE = COUNT(*)
			FROM  TRANSACCION T
				  INNER JOIN  RL_TRANSACCION_CLIENTE RTC  ON T.TRANSACCION_ID=RTC.TRANSACCION_ID
				  INNER JOIN  RL_TRANSACCION_ESTACION RTE  ON T.TRANSACCION_ID=RTE.TRANSACCION_ID
				  AND RTC.CLIENTE_ID IN (SELECT CLIENTE_ID FROM CLIENTE
										 WHERE (SELECT (CASE WHEN (COUNT (CLIENTE_ID))> 0 THEN 1 ELSE 0 END) AS VALOR 
											   FROM   RL_SYS_CLIENTE_USUARIO
											   WHERE  CLIENTE_ID = RTC.CLIENTE_ID
											   AND USUARIO_ID=LTRIM(RTRIM(UPPER(@USUARIO)))) = 1)WHERE T.TIPO_OPERACION_ID='TR' AND RTE.ORDEN=1	
			IF @EXISTE = 0
			BEGIN
				RAISERROR('El usuario %s no posee clientes asignados',16,1,@USUARIO)
				return
			END

			--GENERO EL DOCUMENTO DE TRANSACCION .--
			EXEC CREAR_DOC_TRANSFERENCIA @USUARIO=@USUARIO

			--OBTENGO EL DOC_TRANS_ID INSERTADO.--
			SET @VDOCNEW=@@IDENTITY

			UPDATE DOCUMENTO_TRANSACCION SET TR_POS_COMPLETA= '0' WHERE DOC_TRANS_ID=@VDOCNEW

			--ABRO EL CURSOR PARA SU POSTERIOR USO	
			OPEN CUR_RL_TR

			SET @CONT_LINEA= 0
			SET @LIM_CONT=0
			SET @ICANTIDAD=@CANTIDAD

			FETCH NEXT FROM CUR_RL_TR INTO @VRLID--,@CLIENTE_ID
			WHILE (@@FETCH_STATUS=0)
			BEGIN
					SET @CONT_LINEA= @CONT_LINEA + 1 
					INSERT INTO DET_DOCUMENTO_TRANSACCION (
							DOC_TRANS_ID,     NRO_LINEA_TRANS,
							DOCUMENTO_ID,     NRO_LINEA_DOC,
							MOTIVO_ID,        EST_MERC_ID,
							CLIENTE_ID,       CAT_LOG_ID,
							ITEM_OK,          MOVIMIENTO_PENDIENTE,
							DOC_TRANS_ID_REF, NRO_LINEA_TRANS_REF)
					VALUES (
							@VDOCNEW
							,@CONT_LINEA --NRO DE LINEA DE DET_DOCUMENTO_TRANSACCION
							,NULL   ,NULL   ,NULL     ,NULL
							,@CLIENTE_ID
							,NULL ,'0' ,'0' ,NULL     ,NULL)

			
					SELECT @NEWPOS=CAST(DBO.GET_POS_ID_TR(@POSICION_D) AS INT)
					SELECT @NEWNAVE=CAST(DBO.GET_NAVE_ID_TR(@POSICION_D) AS INT)

					
					select @VCANTIDAD=cantidad from RL_DET_DOC_TRANS_POSICION where RL_ID = @vRLID
					
					
					if @cantidad >0 
					begin
						IF @CANTIDAD = @VCANTIDAD
							BEGIN
								INSERT INTO RL_DET_DOC_TRANS_POSICION
								   (DOC_TRANS_ID,
									NRO_LINEA_TRANS,
									POSICION_ANTERIOR,
									POSICION_ACTUAL,
									CANTIDAD,
									TIPO_MOVIMIENTO_ID,
									ULTIMA_SECUENCIA,
									NAVE_ANTERIOR,
									NAVE_ACTUAL,
									DOCUMENTO_ID,
									NRO_LINEA,
									DISPONIBLE,
									DOC_TRANS_ID_TR,
									NRO_LINEA_TRANS_TR,
									CLIENTE_ID,
									CAT_LOG_ID,
									EST_MERC_ID)
									(SELECT   DOC_TRANS_ID 
											, NRO_LINEA_TRANS
											, POSICION_ACTUAL
											, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
											, @VCANTIDAD--, CANTIDAD
											, NULL
											, NULL
											, NAVE_ACTUAL
											, @NEWNAVE
											, NULL
											, NULL
											, 0
											, @vDocNew
											, 1 
											, CLIENTE_ID
											, CAT_LOG_ID
											, EST_MERC_ID
									 FROM RL_DET_DOC_TRANS_POSICION
									 WHERE RL_ID = @vRLID
									 ) 
						             					             
									 
									 EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE, @CANTIDAD
						             
									 DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
						             
									 SET @CANTIDAD=0
						             
							END
						ELSE
							IF @CANTIDAD < @VCANTIDAD --CANTIDAD A TRANSFERIR ES MENOR A CANT RL
								BEGIN						
									
									SET @DIFERENCIA=@VCANTIDAD - @CANTIDAD
									
									INSERT INTO RL_DET_DOC_TRANS_POSICION
								   (DOC_TRANS_ID,
									NRO_LINEA_TRANS,
									POSICION_ANTERIOR,
									POSICION_ACTUAL,
									CANTIDAD,
									TIPO_MOVIMIENTO_ID,
									ULTIMA_SECUENCIA,
									NAVE_ANTERIOR,
									NAVE_ACTUAL,
									DOCUMENTO_ID,
									NRO_LINEA,
									DISPONIBLE,
									DOC_TRANS_ID_TR,
									NRO_LINEA_TRANS_TR,
									CLIENTE_ID,
									CAT_LOG_ID,
									EST_MERC_ID)
									(SELECT   DOC_TRANS_ID 
											, NRO_LINEA_TRANS
											, POSICION_ACTUAL
											, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
											, @CANTIDAD  -- CANTIDAD TRANSFERIDA
											, NULL
											, NULL
											, NAVE_ACTUAL
											, @NEWNAVE
											, NULL
											, NULL
											, 0
											, @vDocNew
											, 1 
											, CLIENTE_ID
											, CAT_LOG_ID
											, EST_MERC_ID
									 FROM RL_DET_DOC_TRANS_POSICION
									 WHERE RL_ID = @vRLID
									 ) 
									 
									 
									 EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE, @CANTIDAD
						             
									 UPDATE RL_DET_DOC_TRANS_POSICION SET cantidad=@DIFERENCIA --CANTIDAD REMANENTE EN LA RL
									 WHERE RL_ID = @vRLID
									 
									 
									 
									 
									 
									 SET @CANTIDAD=0
								END
							ELSE
								BEGIN		--CANTIDAD CANTIDAD A TRANSFERIR MAYOR A LA RL
									SET @DIFERENCIA=@CANTIDAD - @VCANTIDAD	
									set @CANTIDAD =@CANTIDAD - @VCANTIDAD	--@CANTIDAD AHORA ES EL RESTO A TRANSFERIR
									INSERT INTO RL_DET_DOC_TRANS_POSICION
								   (DOC_TRANS_ID,
									NRO_LINEA_TRANS,
									POSICION_ANTERIOR,
									POSICION_ACTUAL,
									CANTIDAD,
									TIPO_MOVIMIENTO_ID,
									ULTIMA_SECUENCIA,
									NAVE_ANTERIOR,
									NAVE_ACTUAL,
									DOCUMENTO_ID,
									NRO_LINEA,
									DISPONIBLE,
									DOC_TRANS_ID_TR,
									NRO_LINEA_TRANS_TR,
									CLIENTE_ID,
									CAT_LOG_ID,
									EST_MERC_ID)
									(SELECT   DOC_TRANS_ID 
											, NRO_LINEA_TRANS
											, POSICION_ACTUAL
											, @NEWPOS	--(SELECT CAST(DBO.GET_POS_ID(@POSICION_D) AS INT))
											, @VCANTIDAD--, CANTIDAD RL
											, NULL
											, NULL
											, NAVE_ACTUAL
											, @NEWNAVE
											, NULL
											, NULL
											, 0
											, @vDocNew
											, 1 
											, CLIENTE_ID
											, CAT_LOG_ID
											, EST_MERC_ID
									 FROM RL_DET_DOC_TRANS_POSICION
									 WHERE RL_ID = @vRLID
									 ) 								 
									 
									 EXEC AUDITORIA_HIST_MOB_TR @vRLID, @NEWPOS, @NEWNAVE, @VCANTIDAD
						             
						             
									 DELETE RL_DET_DOC_TRANS_POSICION WHERE RL_ID =@VRLID
			
								END
					END --if @cantidad >0
					
				FETCH NEXT FROM CUR_RL_TR INTO @VRLID --,@CLIENTE_ID
				
			END
			
			CLOSE CUR_RL_TR
			DEALLOCATE CUR_RL_TR

			
			IF @CANTIDAD > 0 
				begin
					set @msg = 'Solo se pueden transferir ' + cast((@CANT_ORIG - @cantidad) as varchar) + ' .Por favor cancele la operación y comience de nuevo.'
					RAISERROR(@msg,16,1)		
				end


			INSERT INTO IMPRESION_RODC VALUES(@VDOCNEW,0,'D',0,'')
			
			UPDATE POSICION SET POS_VACIA='0' 
						WHERE POSICION_ID IN (SELECT DISTINCT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)

			UPDATE POSICION SET POS_VACIA='1' 
						WHERE POSICION_ID  NOT IN (SELECT DISTINCT POSICION_ACTUAL FROM RL_DET_DOC_TRANS_POSICION WHERE POSICION_ACTUAL IS NOT NULL)		

			-- DEVOLUCION
			EXEC SYS_DEV_TRANSFERENCIA @VDOCNEW

			--FINALIZA LA TRANSFERENCIA	
			EXEC DBO.MOB_FIN_TRANSFERENCIA @PDOCTRANS=@VDOCNEW,@USUARIO=@USUARIO
	end try
	begin catch
		exec usp_RethrowError
	end catch
		
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  PROCEDURE [dbo].[Mob_UbicacionMercaderia]
@NroPallet AS VARCHAR(100)
AS
SELECT     NRO_LINEA
FROM         DET_DOCUMENTO
--WHERE     (PROP1 = @NroPallet)
WHERE     (DOCUMENTO_ID= @NroPallet)
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  PROCEDURE [dbo].[Mob_Usuario_Correcto]
@usuario_id as nvarchar(20)
as

select  NOMBRE from SYS_USUARIO where RTRIM(LTRIM(upper(USUARIO_ID))) = upper(@usuario_id)
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[Mob_Validacion_Cont_Ubic] 
@Documento_ID	numeric(20,0),
@Nro_Contenedora numeric(10,0),
@Producto_id	varchar(30)=null
AS
begin
	set xact_abort on
	set nocount on
		
	--1. Valido el número de la contenedora que ingreso no haya sido ubicada
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

	--2 CONTROLO QUE SI EXISTE ALGUN PRODUCTO EN LA CONTENEDORA QUE REQUIERA SERIE AL INGRESO, SI NO SE LE CARGO ALGUNA QUE NO DEJE UBICARLA.
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

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   Procedure [dbo].[Mob_Verifica_Existencia]
@Cliente_Id 	as Varchar(15),
@Producto_id	as Varchar(30),
@Solicitada		as Float,
@Documento_Id	as Numeric(20,0),
@Control		as Char(1) OUTPUT
As
Begin
	Declare @Total 			as Float
	Declare @Reservados		as Float
	Declare @Real			as Float

	--De aca saco la existencia total.	
	SELECT 
			@Total=IsNull(Sum(rl.cantidad), 0)
	FROM  	rl_det_doc_trans_posicion             rl
			inner join det_documento_transaccion  ddt 
			on ddt.doc_trans_id    = rl.doc_trans_id and ddt.nro_linea_trans = rl.nro_linea_trans
			inner join det_documento               dd 
			on dd.documento_id     = ddt.documento_id and dd.nro_linea        = ddt.nro_linea_doc
			inner join categoria_logica            cl 
			on cl.cliente_id       = rl.cliente_id	   and cl.cat_log_id       = rl.cat_log_id
			left join nave                    n on n.nave_id           = rl.nave_actual
			left join posicion                p on p.posicion_id       = rl.posicion_actual
			left join nave                   n2 on n2.nave_id          = p.nave_id
			left join calle_nave           caln on caln.calle_id       = p.calle_id
			left join columna_nave         coln on coln.columna_id     = p.columna_id
			left join estado_mercaderia_rl emrl on emrl.cliente_id     = dd.cliente_id
			and emrl.est_merc_id    = rl.est_merc_id
			,(select null as fecha_cpte,null as fecha_alta_gtw) as x
	WHERE 	dd.cliente_id = @Cliente_Id
			and dd.producto_id = @Producto_Id
			and rl.disponible = '1' and cl.disp_egreso = '1' and isnull(n.disp_egreso, isnull(n2.disp_egreso, '1')) = '1' and isnull(p.pos_lockeada, '0') = '0'
			and isnull(emrl.disp_egreso, '1') = '1'
	
	--De aca saco los reservados.
	select 
			@reservados= isnull (sum(t2.cantidad), 0)
	from (
			select 
					dd.cliente_id,
					dd.producto_id,
					sum(isnull(dd.cantidad,0)) as cantidad,
					dd.nro_serie,
					dd.nro_lote, dd.nro_partida,
					dd.nro_despacho,
					dd.nro_bulto,
					dd.fecha_vencimiento,
					dd.peso,
					dd.volumen,
					dd.tie_in,
					dd.cat_log_id_final,
					dd.prop1,
					dd.prop2,
					dd.prop3,
					dd.unidad_id,
					dd.unidad_peso,
					dd.unidad_volumen,
					dd.est_merc_id,
					dd.moneda_id,dd.costo
			from 	documento d,
					det_documento dd,
					categoria_logica cl
			where 	1 <> 0
					and d.documento_id = dd.documento_id
					and dd.cat_log_id = cl.cat_log_id
					and dd.cliente_id = cl.cliente_id
					and d.status = 'D20' and cl.categ_stock_id = 'TRAN_EGR'
					and dd.documento_id <> @Documento_Id
			group by 
					dd.cliente_id,
					dd.producto_id,
					dd.nro_serie,
					dd.nro_lote,
					dd.nro_partida,
					dd.nro_despacho,
					dd.nro_bulto,
					dd.fecha_vencimiento,
					dd.peso,
					dd.volumen,
					dd.tie_in,
					dd.cat_log_id_final,
					dd.prop1,
					dd.prop2,
					dd.prop3,
					dd.unidad_id,
					dd.unidad_peso,
					dd.unidad_volumen,
					dd.est_merc_id,
					dd.moneda_id,
					dd.costo
	
			union all
	
			select 
					dd.cliente_id,
					dd.producto_id,
					sum(isnull(dd.cantidad,0)) as cantidad,
					dd.nro_serie,
					dd.nro_lote,
					dd.nro_partida,
					dd.nro_despacho,
					dd.nro_bulto,
					dd.fecha_vencimiento,
					dd.peso,
					dd.volumen,
					dd.tie_in,
					dd.cat_log_id_final,
					dd.prop1,
					dd.prop2,
					dd.prop3,
					dd.unidad_id,
					dd.unidad_peso,
					dd.unidad_volumen,
					dd.est_merc_id,
					dd.moneda_id,
					dd.costo
			from 	det_documento dd,
					categoria_logica cl,
					det_documento_transaccion ddt,
					documento_transaccion dt
			where 	1 <> 0
					and ddt.cliente_id = cl.cliente_id
					and ddt.cat_log_id = cl.cat_log_id
					and cl.categ_stock_id = 'TRAN_EGR'
					and dd.cliente_id = cl.cliente_id
					and ddt.documento_id = dd.documento_id
					and ddt.nro_linea_doc = dd.nro_linea
					and dt.doc_trans_id = ddt.doc_trans_id
					and dt.status = 'T10'
					and dd.documento_id <> @Documento_Id
			group by 
					dd.cliente_id,
					dd.producto_id,
					dd.nro_serie,
					dd.nro_lote,
					dd.nro_partida,
					dd.nro_despacho,
					dd.nro_bulto,
					dd.fecha_vencimiento,
					dd.peso,
					dd.volumen,
					dd.tie_in,
					dd.cat_log_id_final,
					dd.prop1,
					dd.prop2,
					dd.prop3,
					dd.unidad_id,
					dd.unidad_peso,
					dd.unidad_volumen,
					dd.est_merc_id,
					dd.moneda_id,
					dd.costo
					) t2
	where 	1 <> 0 and t2.cliente_id=@Cliente_Id and t2.producto_id=@Producto_id

	--Aca saco la cantidad real que puedo usar.
	Set @Real= @Total - @Reservados


	If @Solicitada<=@Real
	Begin
		Set @Control='1' --Todo Ok hay existencias.
	End
	If @Solicitada>@Real
	Begin
		--Aviso del error.
		Set @Control='0'
		Raiserror('No hay suficientes articulos del producto %s',16,1,@Producto_id)
	End
End --Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  Procedure 	[dbo].[Mob_Verifica_Nave_Pre] 
					@Pos_Nave_cod 	varchar (40),
					@Flag 				varchar (1) output
As
Begin
	Declare @Cant numeric (5,0)

	select @Cant = sum(x.Cantidad) from
	(
	Select	Count(*) as Cantidad
	From	Nave n
			Inner join Posicion P on (n.nave_ID = p.nave_iD)
	Where	n.pre_ingreso <> '1'
			and n.pre_egreso <> '1'
			and n.disp_transf = '1'
			and p.posicion_cod =  @Pos_Nave_cod
	
	Union all
	
	Select	Count(*) as Cantidad
	From	Nave n
	Where	n.pre_ingreso <> '1'
			and n.pre_egreso <> '1'
			and n.disp_transf = '1'
			and n.nave_cod =  @Pos_Nave_cod
	) as X
	
	If @Cant = 0 
		Begin
			Set @Flag = '0'
		End
	Else
		Begin
			Set @Flag = '1'
		End

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  PROCEDURE [dbo].[MOB_VERIFICA_PALLET]
@DOCUMENTO_ID	AS NUMERIC(20,0),
@NRO_LINEA		AS NUMERIC(10,0),
@NROPALLET		AS VARCHAR(100)

AS
BEGIN
	BEGIN
		IF @DOCUMENTO_ID IS NULL
			RAISERROR ('EL PARAMETRO @DOCUMENTO_ID NO PUEDE SER NULO. SQLSERVER', 16, 1)
	END
	BEGIN
		IF @NRO_LINEA IS NULL
			RAISERROR ('EL PARAMETRO @NRO_LINEA NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END
	BEGIN
		IF @NROPALLET IS NULL
			RAISERROR ('EL PARAMETRO @NROPALLET NO PUEDE SER NULO. SQLSERVER', 16, 1)			
	END

	BEGIN
		SELECT 	LI.POSICION_ID,P.POSICION_COD
		FROM 	SYS_LOCATOR_ING LI
				INNER JOIN POSICION P
				ON (LI.POSICION_ID=P.POSICION_ID)
		WHERE 	LI.DOCUMENTO_ID=@DOCUMENTO_ID 
				AND LI.NRO_LINEA=@NRO_LINEA
				AND NRO_PALLET=UPPER(LTRIM(RTRIM(@NROPALLET)))

	END 
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER Procedure [dbo].[Mob_Verifica_Posiciones_Permitidas]
@Cliente_id	as Varchar(15),
@Producto_Id	as Varchar(30),
@Posicion		as Varchar(45),
@PosOk		as Char(1) Output
As
Begin
	
	Declare @Control 	as  Numeric(10,0)

	Select @Control=Count(*)
	From Rl_Producto_Posicion_Permitida
	Where	Cliente_id=Ltrim(Rtrim(Upper(@Cliente_Id)))
				and Producto_id=Ltrim(Rtrim(Upper(@Producto_Id)))

	If @Control>0 
	Begin
		Select Distinct @Control=Count( x.Posicion)
		From (	
			Select 	Distinct
					N.Nave_cod as Posicion
			From	Rl_producto_posicion_permitida Rppp
					Inner Join Nave N
					On(Rppp.Nave_id=N.Nave_id)
			Where	Cliente_id=Ltrim(Rtrim(Upper(@Cliente_Id)))
					and Producto_id=Ltrim(Rtrim(Upper(@Producto_Id)))
			
			Union All
		
			Select 	Distinct
					P.Posicion_Cod as Posicion
			From	Rl_Producto_Posicion_Permitida Rppp
					Inner Join Posicion P
					On(Rppp.Posicion_id=P.Posicion_Id)
			Where	Cliente_id=Ltrim(Rtrim(Upper(@Cliente_Id)))
					and Producto_id=Ltrim(Rtrim(Upper(@Producto_Id)))
		) As X
		Where	x.Posicion=Ltrim(Rtrim(Upper(@Posicion)))
		
		If @Control>0
		Begin
			Set @PosOk='1'
		End
		Else
		Begin
			Set @PosOk='0'
		End
	End
	Else
	Begin
		Set @PosOk='1'
	End	
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[Mob_VerificaIntermedia]
@Pallet as varchar(100),
@Out 	as INT output
As 
Begin
	Declare @Return as int
	Declare @Q1		as int
	Declare @Q2		as int

	SELECT 	@Q1=COUNT(RL_ID)
	FROM	RL_DET_DOC_TRANS_POSICION RL
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD
			ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			LEFT JOIN NAVE N
			ON(RL.NAVE_ACTUAL=N.NAVE_ID)
			LEFT JOIN POSICION P
			ON(RL.POSICION_ACTUAL=P.POSICION_ID)
	WHERE	DD.PROP1=@pallet
			AND N.INTERMEDIA='1'




	SELECT 	@Q2=COUNT(RL_ID)
	FROM	RL_DET_DOC_TRANS_POSICION RL
			INNER JOIN DET_DOCUMENTO_TRANSACCION DDT
			ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS)
			INNER JOIN DET_DOCUMENTO DD
			ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
			LEFT JOIN NAVE N
			ON(RL.NAVE_ACTUAL=N.NAVE_ID)
			LEFT JOIN POSICION P
			ON(RL.POSICION_ACTUAL=P.POSICION_ID)
	WHERE	DD.PROP1=@pallet
			AND P.INTERMEDIA='1'

	

	If @Q1=1 Or @Q2=1
		Begin
			set @Return=1
		End
	Else
		Begin
			set @Return=0
		End

	SET @Out= @Return

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

IF @@TRANCOUNT > 0
BEGIN
   IF EXISTS (SELECT * FROM #tmpErrors)
       ROLLBACK TRANSACTION
   ELSE
       COMMIT TRANSACTION
END
GO

DROP TABLE #tmpErrors
GO