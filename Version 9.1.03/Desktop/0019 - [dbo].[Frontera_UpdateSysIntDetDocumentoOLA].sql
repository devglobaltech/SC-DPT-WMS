IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Frontera_UpdateSysIntDetDocumentoOLA]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Frontera_UpdateSysIntDetDocumentoOLA]
GO

CREATE       PROCEDURE [dbo].[Frontera_UpdateSysIntDetDocumentoOLA]
@cliente_id 	varchar(50)		output,
@viaje_id 		varchar(100)	output,
@pdocumento_id	numeric(20,0)	output,
@pdoc_ext		varchar(100)	output
AS
BEGIN
	declare @Qty			numeric(20,0)
	declare @QtyProc		numeric(20,0)
	DECLARE @CURDOC			CURSOR
	DECLARE @DOCUMENTO_ID	NUMERIC(20,0)
	DECLARE @DOC_EXT		VARCHAR(100)

	SET @CURDOC = CURSOR FOR
		SELECT DOCUMENTO_ID, NRO_REMITO FROM DOCUMENTO WHERE NRO_DESPACHO_IMPORTACION = @VIAJE_ID AND DOCUMENTO_ID=@PDOCUMENTO_ID
		UNION
		SELECT	NULL,DOC_EXT 
		FROM	SYS_INT_DOCUMENTO (NOLOCK) 
		WHERE	CODIGO_VIAJE=@VIAJE_ID
				AND DOC_EXT NOT IN(	SELECT	NRO_REMITO 
									FROM	DOCUMENTO (NOLOCK) 
									WHERE	NRO_DESPACHO_IMPORTACION = @VIAJE_ID 
											AND DOCUMENTO_ID=@PDOCUMENTO_ID)

	OPEN @CURDOC
	FETCH NEXT FROM @CURDOC INTO @DOCUMENTO_ID, @DOC_EXT

	WHILE @@FETCH_STATUS = 0
	BEGIN
		update	sys_int_det_documento set estado_gt='P',fecha_estado_gt=getdate(),documento_id=@documento_id
		where	cliente_id=@cliente_id
				and doc_ext=@doc_ext
				and documento_id is null
				

		update sys_int_det_documento set estado_gt='P',fecha_estado_gt=getdate(),documento_id=@documento_id
		where
			 cliente_id=@cliente_id
			 and doc_ext=@doc_ext
			 and documento_id is null


		select @Qty=isnull(count(producto_id),0) 
		from sys_int_det_documento
		where
			 cliente_id=@cliente_id
			 and doc_ext=@doc_ext
			 and estado_gt is null
		
		if (@Qty=0) begin
				update sys_int_documento set estado_gt='P',fecha_estado_gt=getdate()
				where	     
					cliente_id=@cliente_id
					and doc_ext=@doc_ext
		end --if

		--Si no tengo pedidos pendientes y ninguno ingreso a picking ejecuto sys_dev_egreso

		--Obtengo el Codigo de Viaje
		select @viaje_id=codigo_viaje from sys_int_documento where cliente_id=@cliente_id and doc_ext=@doc_ext
		
		set @Qty=0
		
		select	@Qty=count(dd.cliente_id)
		from	sys_int_documento d inner join sys_int_det_documento dd 
				on (d.cliente_id=dd.cliente_id and d.doc_ext=dd.doc_ext)
		where	d.codigo_viaje=@viaje_id and dd.estado_gt is null and dd.DOC_EXT=@pdoc_ext

		if (@Qty=0) begin --significa que no quedan pendientes de procesar

			set @QtyProc=0
			
			select	@QtyProc=count(cliente_id) 
			from	sys_int_det_documento 
			where	documento_id in (	select	documento_id 
										from	documento 
										where	nro_despacho_importacion=@viaje_id
												and DOCUMENTO_ID=@pdocumento_id)
			
			if (@QtyProc=0) begin

				exec dbo.SYS_DEV_EGRESO @viaje_id
			end --if
		end --if

		FETCH NEXT FROM @CURDOC INTO @DOCUMENTO_ID, @DOC_EXT
	END

	CLOSE @CURDOC
	DEALLOCATE @CURDOC
END