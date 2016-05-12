
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 05:45 p.m.
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

ALTER Procedure [dbo].[Mostrador]
As
Begin
DECLARE @MSG AS VARCHAR(MAX)

	BEGIN TRY
		/*
		Objetivos del proceso.
		===================================================
		1) Creo el Documento.					| Listo.
		2) Creo el Detalle del Documento.		| Listo.
		3) Autocompleto con la nave "Mostrador".| Listo.
		4) Completo Picking.					| Listo.
		5) Apruebo el documento de egreso.		| Listo.
		6) Realizo la Devolucion al E.R.P.		| Listo.
		*/
		Declare @CurViajes		cursor
		Declare @ViajeOld		varchar(100)	--para el corte de control

		--variables para el cursor de viajes.
		Declare @Viaje_id		varchar(100)
		Declare @Cliente_id		varchar(15)
		Declare @Agente_id		varchar(20)
		Declare @Doc_Ext		varchar(100)
		Declare @Producto_id	varchar(30)
		Declare @Cantidad		numeric(20,5)
		Declare @Unidad_ID		varchar(5)
		Declare @Documento_id	numeric(20,0)
		Declare @QtyDoc			numeric(20,5)
		Declare @QtySol			Numeric(20,5)
		Declare @Usr			varchar(100)
		Declare @PalletPicking	Numeric(30,0)

		Set @Usr='USER'

		--tablas temporales.
		CREATE TABLE #temp_gproductos_viajes (
		viaje_id			VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
		grupo_producto_id   VARCHAR(10)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
		)

		CREATE TABLE #temp_usuario_loggin (
			usuario_id            			VARCHAR(20)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
			terminal              			VARCHAR(100)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
			fecha_loggin       				DATETIME,
			session_id            			VARCHAR(60)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
			rol_id                			VARCHAR(5)  	COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
			emplazamiento_default 			VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
			deposito_default      			VARCHAR(15)  	COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
		)
		EXEC FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN @Usr

		SET @CURVIAJES = CURSOR FOR
			SELECT  DISTINCT
					S.CLIENTE_ID,S.CODIGO_VIAJE, S.DOC_EXT
			FROM    SYS_INT_DOCUMENTO S INNER JOIN SYS_INT_DET_DOCUMENTO SD
					ON(S.CLIENTE_ID=SD.CLIENTE_ID AND S.DOC_EXT=SD.DOC_EXT)
			WHERE   S.TIPO_DOCUMENTO_ID='E07'
					AND SD.ESTADO_GT IS NULL
					AND SD.FECHA_ESTADO_GT IS NULL
					AND S.ESTADO_GT IS NULL
					AND S.FECHA_ESTADO_GT IS NULL
					AND S.DOC_EXT NOT IN (	SELECT	DISTINCT DOC_EXT  
											FROM	(	SELECT	X.DOC_EXT, CASE WHEN (X.DIF<0  OR X.DIF IS NULL) THEN 0 ELSE 1 END AS ESP 
														FROM	(	select	g.doc_ext,g.producto_id,g.descripcion,g.cantidad as cantidadoriginal,
																			g.fecha_estado_gt,g.estado_gt,v.cantidad as cantidadstock,v.nave as deposito,
																			v.cantidad - g.cantidad AS DIF,G.FECHA_ESTADO
																	from    dbo.sys_int_det_documento as g
																			INNER JOIN DBO.sys_int_documento S ON (S.CLIENTE_ID =G.CLIENTE_ID AND S.DOC_EXT = G.DOC_EXT)
																			LEFT join (	SELECT	NAVE, [cod. producto], SUM(CANTIDAD) AS CANTIDAD
																						FROM	dbo.vstock v inner join nave n
																								on(v.nave=n.nave_cod)
																						WHERE	n.c_mostrador='1'
																								AND [CAT. LOG.] IN (SELECT	CAT_LOG_ID 
																													FROM	CATEGORIA_LOGICA 
																													WHERE	PICKING ='1' AND DISP_EGRESO ='1')
																								AND (([EST. MERC.] IS NULL) OR ([EST. MERC.] IN	(	SELECT	EST_MERC_ID 
																																					FROM	ESTADO_MERCADERIA_RL 
																																					WHERE	PICKING ='1' 
																																							AND DISP_EGRESO ='1')))
																						GROUP BY NAVE, [cod. producto]
																			) as v on v.[cod. producto] = g.producto_id
																   where	1=1
																			AND S.TIPO_DOCUMENTO_ID = 'E07'
																			AND g.estado_gt IS NULL
														)X
			GROUP BY 
					DOC_EXT,CASE WHEN (X.DIF<0  OR X.DIF IS NULL) THEN 0 ELSE 1 END) XX
			WHERE XX.ESP = 0)			

		open @curviajes

		Fetch Next from @curviajes into @Cliente_id,@viaje_id, @Doc_Ext
		While @@Fetch_Status=0
		Begin
			BEGIN TRY
				Begin Transaction

				Insert into DOCUMENTOS_E07 values(@Doc_Ext, getdate())

				Update sys_int_documento set tipo_documento_id='E04' where doc_ext=@Doc_Ext
				
				--1) Creacion del Documento.
				insert into documento(cliente_id, tipo_comprobante_id, sucursal_destino, fecha_cpte, fecha_pedida_ent, status, anulado, nro_remito, nro_despacho_importacion, prioridad_picking,tipo_operacion_id,det_tipo_operacion_id)		
				select	cliente_id, 'E04', agente_id, getdate(), getdate(), 'D05', '0', 
						DOC_EXT, CODIGO_VIAJE,'1','EGR','MAN'
				from	sys_int_documento
				where	codigo_viaje=@viaje_id AND DOC_EXT = @Doc_Ext

				--Obtengo el documento_id
				set @Documento_id= Scope_identity()
				
				--2) Creacion de los detalles del documento.
				insert into det_documento (documento_id, nro_linea, cliente_id, producto_id, cantidad, tie_in, unidad_id, descripcion, busc_individual, item_ok, cant_solicitada, nro_lote, nro_partida, nro_serie)
				select	@documento_id, ROW_NUMBER() OVER (ORDER BY SD.NRO_LINEA), sd.cliente_id, sd.producto_id, sd.cantidad_solicitada,'0',
						p.unidad_id, p.descripcion, null, null,sd.cantidad_solicitada, sd.nro_lote, sd.nro_partida, sd.prop3
				from	sys_int_det_documento sd inner join producto p
						on(sd.cliente_id=p.cliente_id and sd.producto_id=p.producto_id)
						inner join sys_int_documento s
						on(sd.cliente_id=s.cliente_id and sd.doc_ext=s.doc_ext)
				where	s.codigo_viaje=@viaje_id AND SD.DOC_EXT = @Doc_Ext
				
				--3) Asigno 
				Exec LocatorEgreso_Mostrador @Cliente_id, @viaje_id
				Exec Dbo.Ingresa_Picking @Documento_id

				Select	@QtyDoc=Sum(Cantidad)
				from	picking
				where	viaje_id=@Viaje_ID
						and documento_id=@Documento_id

				Select	@QtySol=Sum(Cantidad_solicitada)
				from	sys_int_documento s inner join sys_int_det_documento sd
						on(s.cliente_id=sd.cliente_id and s.doc_ext=sd.doc_ext)
				where	s.codigo_viaje=@Viaje_ID
						and s.doc_ext=@Doc_Ext

				if @QtySol=@QtyDoc
				begin

					exec Dbo.Get_Value_For_Sequence 'PALLET_PICKING',@PalletPicking Output
					Exec dbo.Frontera_UpdateSysIntDetDocumento @Cliente_ID, @Doc_Ext, @Documento_ID
					Exec dbo.Frontera_Viaje_Proceso @viaje_id, 'L'

					update	sys_int_documento set estado_gt='P', fecha_estado_gt=getdate() where	doc_ext=@doc_ext

					update	sys_int_det_documento set estado_gt='P', fecha_estado_gt=getdate(), documento_id=@documento_id where	doc_ext=@doc_ext					

					Exec DBO.CREATE_CHILD @Viaje_ID

					--4 Completo en Picking
					Update	picking set
							fecha_inicio=getdate(),
							fecha_fin=getdate(),
							Usuario=@Usr,
							Cant_confirmada=cantidad,
							pallet_picking=@PalletPicking,
							st_camion='1',
							st_control_exp='1',
							usuario_control_exp=@usr,
							fecha_control_exp=getdate(),
							terminal_control_exp=host_name(),
							fecha_control_fac=getdate(),
							terminal_control_fac=host_name(),
							usuario_control_fac=@usr,
							facturado='1',
							fin_picking='2'
					where	viaje_id=@Viaje_ID
					
					exec Dbo.Jb_Close_Documents_Egr
					
					exec Dbo.SYS_DEV_EGRESO_MOSTRADOR @Viaje_ID, @doc_ext
					
					commit transaction
				end
				else
				begin
					Print ('No se encontro Stock en la/s nave/s de tipo mostrador.')
					rollback transaction
					--return
				end
				
				Set @QtySol			=Null
				Set @QtyDoc			=Null
				Set @Documento_id	=Null
				Set @Cliente_ID		=Null 
				Set @Doc_Ext		=Null
				set @PalletPicking	=Null
				
				END TRY
				BEGIN CATCH
					IF @@TRANCOUNT > 0 ROLLBACK
					SET @MSG = 'DOCUMENTO: ' + @VIAJE_ID + ', LINEA : ' + CAST (ERROR_LINE() AS VARCHAR) + '#, DESCRIPCION DEL ERROR :' + ERROR_MESSAGE()
					INSERT INTO ERRORES_LOG ([mensaje],[fecha],SP_ORIGEN,TERMINAL,FLG_ENVIADO) VALUES(@MSG,GETDATE(),'[Mostrador]',HOST_NAME(),'0')
			END CATCH
			Fetch Next from @curviajes into @viaje_id, @Doc_Ext
		End--Fin Cursor Viajes.

		close @curviajes
		deallocate @curviajes
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK
		EXEC usp_RethrowError
			END CATCH

End--Fin del procedimiento almacenado.
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