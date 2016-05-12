IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LocatorEgreso_GARANTIAS]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[LocatorEgreso_GARANTIAS]
GO

CREATE Procedure [dbo].[LocatorEgreso_GARANTIAS]      
@pDocumento_id		as Numeric(20,0) Output,      
@pViaje_id			as varchar(100)  Output,
@pCliente_ID	    as varchar(30)   output 
As      
Begin      
      
declare @Fecha_Vto			as datetime      
declare @OrdenPicking		as numeric(10,0)      
declare @Tipo_Posicion		as varchar(10)      
declare @Codigo_Posicion	as varchar(100)      
declare @Cliente_id			as varchar(15)      
declare @Producto_id		as varchar(30)      
declare @Cantidad			as numeric(20,5)      
declare @Aux				as varchar(50)      
declare @NewProducto		as varchar(30)      
declare @OldProducto		as varchar(30)      
declare @vQtyResto			as numeric(20,5)      
declare @vRl_id				as numeric(20)      
declare @QtySol				as numeric(20,5)      
declare @vNroLinea			as numeric(20)      
declare @NRO_BULTO			as varchar(50)      
declare @NRO_LOTE			as varchar(50)      
declare @EST_MERC_ID		as varchar(15)      
declare @NRO_DESPACHO		as varchar(50)      
declare @NRO_PARTIDA		as varchar(50)      
declare @UNIDAD_ID			as varchar(5)      
declare @PROP1				as varchar(100)      
declare @PROP2				as varchar(100)      
declare @PROP3				as varchar(100)      
declare @DESC				as varchar(200)      
declare @CAT_LOG_ID			as varchar(50)      
declare @id					as numeric(20,0)      
declare @Documento_id		as Numeric(20,0)      
declare @Saldo				as numeric(20,5)      
declare @TipoSaldo			as varchar(20)      
declare @Doc_Trans			as numeric(20)      
declare @QtyDetDocumento	as numeric(20)      
declare @vUsuario_id		as varchar(50)      
declare @vTerminal			as varchar(50)      
declare @RsExist			as Cursor      
declare @RsActuRL			as Cursor      
declare @Crit1				as varchar(30)      
declare @Crit2				as varchar(30)      
declare @Crit3				as varchar(30)      
declare @fecha_alta_gtw		as datetime      
declare @RSDOCEGR			as cursor
declare @DOCIDPIVOT			as numeric(20,0)
declare @NROLINEAPIVOT		as numeric(20,0)
declare @PESOPROPS			as numeric(5,0)      
declare @Msg				as varchar(100)      
      
SET NOCOUNT ON;      
      
BEGIN TRY      
	
	CREATE TABLE #SDDPESO
    (CLIENTE_ID		VARCHAR(15)
    ,CODIGO_VIAJE	VARCHAR(100)
    ,DOCUMENTO_ID	NUMERIC(20,0)
    ,NRO_LINEA		NUMERIC(20,0)
    ,PESO INT)


	INSERT	INTO #SDDPESO
	SELECT	D.CLIENTE_ID, D.NRO_DESPACHO_IMPORTACION, DD.DOCUMENTO_ID, DD.NRO_LINEA,
			CAST((CASE
			WHEN ISNULL(NRO_LOTE,'')='' AND ISNULL(NRO_PARTIDA,'')='' AND ISNULL(PROP3,'')='' THEN 0
			WHEN ISNULL(NRO_LOTE,'')='' AND ISNULL(NRO_PARTIDA,'')='' AND ISNULL(PROP3,'')<>'' THEN 1
			WHEN ISNULL(NRO_LOTE,'')='' AND ISNULL(NRO_PARTIDA,'')<>'' AND ISNULL(PROP3,'')='' THEN 1
			WHEN ISNULL(NRO_LOTE,'')='' AND ISNULL(NRO_PARTIDA,'')<>'' AND ISNULL(PROP3,'')<>'' THEN 2
			WHEN ISNULL(NRO_LOTE,'')<>'' AND ISNULL(NRO_PARTIDA,'')='' AND ISNULL(PROP3,'')='' THEN 1
			WHEN ISNULL(NRO_LOTE,'')<>'' AND ISNULL(NRO_PARTIDA,'')='' AND ISNULL(PROP3,'')<>'' THEN 2
			WHEN ISNULL(NRO_LOTE,'')<>'' AND ISNULL(NRO_PARTIDA,'')<>'' AND ISNULL(PROP3,'')='' THEN 2
			WHEN ISNULL(NRO_LOTE,'')<>'' AND ISNULL(NRO_PARTIDA,'')<>'' AND ISNULL(PROP3,'')<>'' THEN 3
			ELSE 0
			END) AS INT) AS PESO
	FROM    DET_DOCUMENTO DD
			INNER JOIN DOCUMENTO D ON (DD.CLIENTE_ID = D.CLIENTE_ID AND DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
	WHERE   D.CLIENTE_ID = @pCliente_id 
			AND D.NRO_DESPACHO_IMPORTACION = @pViaje_id
			and DD.DOCUMENTO_ID=@PDocumento_id
			AND NOT EXISTS (SELECT	1 
							FROM	PICKING PIK 
							WHERE	PIK.DOCUMENTO_ID=DD.DOCUMENTO_ID 
									AND PIK.NRO_LINEA=DD.NRO_LINEA)
		        
	--Obtengo los criterios de ordenamiento.      
	Select	@Crit1=CRITERIO_1, @Crit2=CRITERIO_2, @Crit3=CRITERIO_3      
	From	RL_CLIENTE_LOCATOR      
	Where	Cliente_id=(select Cliente_id from documento where documento_id=@pDocumento_id)      
	  
	if (@Crit1 is null) and (@Crit2 is null) and (@Crit3 is null) begin      
		--Si todos son nulos entonces x default salgo con orden de picking.      
		Set @Crit1='ORDEN_PICKING'      
	end      

	SET @RSDOCEGR = CURSOR FOR
	  SELECT  DD.DOCUMENTO_ID, DD.NRO_LINEA, P.PESO
	  FROM    DET_DOCUMENTO DD
			  INNER JOIN #SDDPESO P ON (DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)
	  WHERE   DD.CLIENTE_ID = @pCliente_id 
			  AND P.CODIGO_VIAJE = @pViaje_id
			  and dd.documento_id=@PDocumento_id
	  ORDER BY 
			  P.PESO DESC, P.DOCUMENTO_ID ASC, P.NRO_LINEA ASC

	OPEN @RSDOCEGR
	FETCH NEXT FROM @RSDOCEGR INTO @DOCIDPIVOT, @NROLINEAPIVOT, @PESOPROPS

	WHILE @@FETCH_STATUS = 0
	BEGIN	

		Set @Msg='Linea: ' + CAST(@NROLINEAPIVOT as varchar)
		SET @QtySol=0
		set @QtySol=dbo.GetQtySol(@DOCIDPIVOT,@NROLINEAPIVOT,@pCliente_id)
		set @vQtyResto=@QtySol
    
		Set @RsExist = Cursor For      
		  Select	X.*      
		  from (    SELECT  dd.fecha_vencimiento      
							,isnull(n.orden_locator,999) as ORDEN_PICKING      
							,'NAV' as ubicacion      
							,n.nave_cod as posicion      
							,dd.cliente_id      
							,dd.producto_id as producto      
							,rl.cantidad      
							,rl.rl_id      
							,dd.NRO_BULTO      
							,dd.NRO_LOTE      
							,RL.EST_MERC_ID      
							,dd.NRO_DESPACHO      
							,dd.NRO_PARTIDA      
							,dd.UNIDAD_ID      
							,dd.PROP1      
							,dd.PROP2      
							,dd.PROP3      
							,dd.DESCRIPCION      
							,RL.CAT_LOG_ID      
							,d.fecha_alta_gtw      
					FROM	rl_det_doc_trans_posicion rl      
							inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)      
							inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)      
							inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )      
							inner join nave n on (rl.nave_actual=n.nave_id)      
							left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)       
							inner join documento d on(dd.documento_id=d.documento_id)      
					WHERE	rl.doc_trans_id_egr is null      
							and rl.nro_linea_trans_egr is null      
							and exists (select 1 from nave where FLG_GARANTIA = '1' and nave_cod = n.nave_cod )--'8'      
							and rl.disponible='1'      
							and isnull(em.disp_egreso,'1')='1'      
							and isnull(em.picking,'1')='1'      
							and rl.cat_log_id<>'TRAN_EGR'      
							and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' --and n.picking='1' --no aplica para electropelba por eso lo comento.      
							and cl.disp_egreso='1' and cl.picking='1'      
							--and dd.producto_id in (select producto_id from det_documento where documento_id=@pDocumento_id)
							and exists (select  1 
										from    det_documento ddegr
										where	ddegr.documento_id = @DOCIDPIVOT 
												AND ddegr.nro_linea = @NROLINEAPIVOT
												and ddegr.producto_id = dd.producto_id
												and ((isnull(ddegr.nro_lote,'')='')		or (ddegr.nro_lote = dd.nro_lote))
												and ((isnull(ddegr.nro_partida,'')='')	or (ddegr.nro_partida = dd.nro_partida))
												and ((isnull(ddegr.nro_serie,'')='')	or (ddegr.nro_serie = dd.nro_serie)))							
							)X        
		   order by
					(CASE WHEN 1   = 1						THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.      
					(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO' THEN x.FECHA_VENCIMIENTO END),      
					(CASE WHEN @Crit1 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),      
					(CASE WHEN @Crit1 = 'NRO_BULTO'			THEN x.NRO_BULTO END),      
					(CASE WHEN @Crit1 = 'NRO_LOTE'			THEN x.NRO_LOTE END),      
					(CASE WHEN @Crit1 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),      
					(CASE WHEN @Crit1 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),      
					(CASE WHEN @Crit1 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),      
					(CASE WHEN @Crit1 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),      
					(CASE WHEN @Crit1 = 'PROP1'				THEN x.PROP1 END),      
					(CASE WHEN @Crit1 = 'PROP2'				THEN x.PROP2 END),      
					(CASE WHEN @Crit1 = 'PROP3'				THEN x.PROP3 END),      
					(CASE WHEN @Crit1 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),      
					(CASE WHEN @Crit1 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),      
					--2      
					(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO' THEN x.FECHA_VENCIMIENTO END),      
					(CASE WHEN @Crit2 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),      
					(CASE WHEN @Crit2 = 'NRO_BULTO'			THEN x.NRO_BULTO END),      
					(CASE WHEN @Crit2 = 'NRO_LOTE'			THEN x.NRO_LOTE END),      
					(CASE WHEN @Crit2 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),      
					(CASE WHEN @Crit2 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),      
					(CASE WHEN @Crit2 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),      
					(CASE WHEN @Crit2 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),      
					(CASE WHEN @Crit2 = 'PROP1'				THEN x.PROP1 END),      
					(CASE WHEN @Crit2 = 'PROP2'				THEN x.PROP2 END),      
					(CASE WHEN @Crit2 = 'PROP3'				THEN x.PROP3 END),      
					(CASE WHEN @Crit2 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),      
					(CASE WHEN @Crit2 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END),      
					--3      
					(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO' THEN x.FECHA_VENCIMIENTO END),      
					(CASE WHEN @Crit3 = 'ORDEN_PICKING'		THEN x.ORDEN_PICKING END),      
					(CASE WHEN @Crit3 = 'NRO_BULTO'			THEN x.NRO_BULTO END),      
					(CASE WHEN @Crit3 = 'NRO_LOTE'			THEN x.NRO_LOTE END),      
					(CASE WHEN @Crit3 = 'EST_MERC_ID'		THEN x.EST_MERC_ID END),      
					(CASE WHEN @Crit3 = 'NRO_DESPACHO'		THEN x.NRO_DESPACHO END),      
					(CASE WHEN @Crit3 = 'NRO_PARTIDA'		THEN x.NRO_PARTIDA END),      
					(CASE WHEN @Crit3 = 'UNIDAD_ID'			THEN x.UNIDAD_ID END),      
					(CASE WHEN @Crit3 = 'PROP1'				THEN x.PROP1 END),      
					(CASE WHEN @Crit3 = 'PROP2'				THEN x.PROP2 END),      
					(CASE WHEN @Crit3 = 'PROP3'				THEN x.PROP3 END),      
					(CASE WHEN @Crit3 = 'CAT_LOG_ID'		THEN x.CAT_LOG_ID END),      
					(CASE WHEN @Crit3 = 'FECHA_ALTA_GTW'	THEN x.FECHA_ALTA_GTW END)      
		         
		 Open @RsExist      
		 Fetch Next From @RsExist into	@Fecha_Vto, @OrdenPicking, @Tipo_Posicion, @Codigo_Posicion, @Cliente_id, @Producto_id,      
										@Cantidad,  @vRl_id, @NRO_BULTO, @NRO_LOTE, @EST_MERC_ID,@NRO_DESPACHO,@NRO_PARTIDA,         
										@UNIDAD_ID, @PROP1, @PROP2, @PROP3, @DESC, @CAT_LOG_ID, @fecha_alta_gtw      


		 set @NewProducto=@Producto_id      
		 set @OldProducto=''      
		 set @vNroLinea=0      
		 While @@Fetch_Status=0      
		 Begin       
			if (@NewProducto<>@OldProducto) begin      
							     
				set @OldProducto=@NewProducto      
				--set @QtySol=dbo.GetQtySol(@pDocumento_id,@Cliente_id,@Producto_id) 
				SELECT	@QtySol=DD.CANTIDAD
				FROM	DET_DOCUMENTO DD
				WHERE	DD.DOCUMENTO_ID=@DOCIDPIVOT
						AND DD.NRO_LINEA=@NROLINEAPIVOT
						
				set @vQtyResto=@QtySol      
			end --if         
			/*
			SET @Msg='@vQtyResto: ' + CAST(@vQtyResto AS VARCHAR)
			RAISERROR(@MSG,16,1)		 		        
			*/
		  if (@vQtyResto>0) begin         
			if (@vQtyResto>=@Cantidad) begin      
			 --set @vNroLinea=@vNroLinea+1      
			 SET @vNroLinea=@NROLINEAPIVOT
			 
			 set @vQtyResto=@vQtyResto-@Cantidad      
			 
			 insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)       
				values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')      
			 --Insert con todas las propiedades en det_documento      
			 insert into det_documento_aux       
			   ( documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,      
				cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,      
				unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada)      
			 values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC      
			   ,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'      
			   ,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol)        
			end      
			else begin      
			 --set @vNroLinea=@vNroLinea+1      
			 SET @vNroLinea=@NROLINEAPIVOT
			 
			 insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)      
				values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@vQtyResto,@vRl_id,@Cantidad-@vQtyResto,'2',getdate(),'N')      
			 --Insert con todas las propiedades en det_documento      
			 insert into det_documento_aux (      
				documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,      
				cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,      
				unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada)      
				values       
				(@pDocumento_id,@vNroLinea      
				,@Cliente_id,@Producto_id,@vQtyResto,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC      
				,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'      
				,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol)       
			 set @vQtyResto=0      
			end --if      
		  end --if      
		  Fetch Next From @RsExist into @Fecha_Vto, @OrdenPicking, @Tipo_Posicion, @Codigo_Posicion, @Cliente_id, @Producto_id,      
										@Cantidad,  @vRl_id, @NRO_BULTO, @NRO_LOTE, @EST_MERC_ID, @NRO_DESPACHO, @NRO_PARTIDA,         
										@UNIDAD_ID, @PROP1,  @PROP2, @PROP3, @DESC, @CAT_LOG_ID, @fecha_alta_gtw
										
		  set @NewProducto=@Producto_id      
		 End --End While @RsExist.      
		 CLOSE @RsExist      
		 DEALLOCATE @RsExist  
		 
		 
		FETCH NEXT FROM @RSDOCEGR INTO @DOCIDPIVOT, @NROLINEAPIVOT, @PESOPROPS	     
	end--fin @rsdocegr
	CLOSE @RSDOCEGR
	DEALLOCATE @RSDOCEGR  
 
	--Borro det_documento y lo vuelvo a insertar con las nuevas propiedades      
	delete det_documento where documento_id=@pDocumento_id      
	insert into det_documento --select * from det_documento_aux where documento_id=@pDocumento_id      
	SELECT  [DOCUMENTO_ID],ROW_NUMBER() OVER (ORDER BY NRO_LINEA),[CLIENTE_ID],[PRODUCTO_ID],[CANTIDAD],[NRO_SERIE],[NRO_SERIE_PADRE],[EST_MERC_ID]      
			,[CAT_LOG_ID],[NRO_BULTO],[DESCRIPCION],[NRO_LOTE],[FECHA_VENCIMIENTO],[NRO_DESPACHO],[NRO_PARTIDA],[UNIDAD_ID]      
			,[PESO],[UNIDAD_PESO],[VOLUMEN],[UNIDAD_VOLUMEN],[BUSC_INDIVIDUAL],[TIE_IN],[NRO_TIE_IN_PADRE],[NRO_TIE_IN]      
			,[ITEM_OK],[CAT_LOG_ID_FINAL],[MONEDA_ID],[COSTO],[PROP1],[PROP2],[PROP3],[LARGO],[ALTO] ,[ANCHO],[VOLUMEN_UNITARIO]      
			,[PESO_UNITARIO],[CANT_SOLICITADA],[TRACE_BACK_ORDER]      
	FROM	DET_DOCUMENTO_AUX      
	WHERE	DOCUMENTO_ID=@PDOCUMENTO_ID      
	------      
	update documento set status='D20' where documento_id=@pDocumento_id      
	Exec Asigna_Tratamiento#Asigna_Tratamiento_EGR @pDocumento_id      
	select distinct @Doc_Trans=doc_trans_id from det_documento_transaccion where documento_id=@pDocumento_id      
	--Hago la reserva en RL      
	Set @RsActuRL = Cursor For select [id],documento_id,Nro_Linea,Cliente_id,Producto_id,Cantidad,rl_id,saldo,tipo from consumo_locator_egr where procesado='N' and Documento_id=@pDocumento_id      
	Open @RsActuRL      
	Fetch Next From @RsActuRL into       
           @id,      
           @Documento_id,      
           @vNroLinea,      
           @Cliente_id,      
           @Producto_id,      
           @Cantidad,      
           @vRl_id,      
           @Saldo,      
           @TipoSaldo      
      
 While @@Fetch_Status=0      
 Begin      
  if (@Saldo=0) begin      
   update rl_det_doc_trans_posicion set doc_trans_id_egr=@Doc_Trans, nro_linea_trans_egr=@vNroLinea,disponible='0'      
               ,cat_log_id='TRAN_EGR',nave_anterior=nave_actual,posicion_anterior=posicion_actual      
               ,nave_actual='2',posicion_actual=null where rl_id=@vRl_id      
   update consumo_locator_egr set procesado='S' where [id]=@id      
  end --if       
      
  if (@Saldo>0) begin      
   insert into rl_det_doc_trans_posicion (doc_trans_id,nro_linea_trans,posicion_anterior,posicion_actual,cantidad,tipo_movimiento_id,      
                ultima_estacion,ultima_secuencia,nave_anterior,nave_actual,documento_id,nro_linea,      
                disponible,doc_trans_id_egr,nro_linea_trans_egr,doc_trans_id_tr,nro_linea_trans_tr,      
                cliente_id,cat_log_id,cat_log_id_final,est_merc_id)      
        select doc_trans_id,nro_linea_trans,posicion_anterior,posicion_actual,@Saldo,tipo_movimiento_id,      
         ultima_estacion,ultima_secuencia,nave_anterior,nave_actual,documento_id,nro_linea,      
         disponible,doc_trans_id_egr,nro_linea_trans_egr,doc_trans_id_tr,nro_linea_trans_tr,      
         cliente_id,cat_log_id,cat_log_id_final,est_merc_id      
        from rl_det_doc_trans_posicion       
        where rl_id=@vRl_id        
   update rl_det_doc_trans_posicion set cantidad=@Cantidad,doc_trans_id_egr=@Doc_Trans, nro_linea_trans_egr=@vNroLinea,disponible='0'      
               ,cat_log_id='TRAN_EGR',nave_anterior=nave_actual,posicion_anterior=posicion_actual      
               ,nave_actual='2',posicion_actual=null where rl_id=@vRl_id      
   update consumo_locator_egr set procesado='S' where [id]=@id      
  end --if       
      
  Fetch Next From @RsActuRL into       
           @id,      
           @Documento_id,      
           @vNroLinea,      
           @Cliente_id,      
           @Producto_id,      
           @Cantidad,      
           @vRl_id,      
           @Saldo,      
           @TipoSaldo      
 End --End While @RsActuRL.      
 CLOSE @RsActuRL      
 DEALLOCATE @RsActuRL      
      
 --Si no hay existencia de ningun producto del documento lo borro para que no quede solo cabecera      
 select @QtyDetDocumento=count(documento_id) from det_documento where documento_id=@pDocumento_id      
 if (@QtyDetDocumento=0) begin      
  delete documento where documento_id=@pDocumento_id       
 end else begin      
  select @vUsuario_id=usuario_id, @vTerminal=Terminal from #temp_usuario_loggin      
  insert into docxviajesprocesados values (@pViaje_id,@pDocumento_id,'P',getdate(),@vUsuario_id,@vTerminal)      
 end --if      
END TRY      
BEGIN CATCH      
 EXEC usp_RethrowError      
END CATCH      
      
Set NoCount Off;      
End -- Fin Procedure.


GO


