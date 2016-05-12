/****** Object:  StoredProcedure [dbo].[LocatorEgreso_OLA]    Script Date: 05/29/2014 16:30:49 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LocatorEgreso_OLA]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[LocatorEgreso_OLA]
GO

CREATE        Procedure [dbo].[LocatorEgreso_OLA]
@PDocumento_id	as Numeric(20,0) Output,
@pCliente_id	as varchar(15) Output,
@pViaje_id		as varchar(100) Output
As
Begin
  declare @Fecha_Vto			as datetime
  declare @OrdenPicking			as numeric(10,0)
  declare @Tipo_Posicion		as varchar(10)
  declare @Codigo_Posicion		as varchar(100)
  declare @Producto_id			as varchar(30)
  declare @Cantidad				as numeric(20,5)
  declare @Aux					as varchar(50)
  declare @NewProducto			as varchar(30)
  declare @OldProducto			as varchar(30)
  declare @vQtyResto			as numeric(20,5)
  declare @vRl_id				as numeric(20)
  declare @QtySol				as numeric(20,5)
  declare @vNroLinea			as numeric(20)
  declare @NRO_BULTO			as varchar(50)
  declare @NRO_LOTE				as varchar(50)
  declare @EST_MERC_ID			as varchar(15)
  declare @NRO_DESPACHO			as varchar(50)
  declare @NRO_PARTIDA			as varchar(50)
  declare @UNIDAD_ID			as varchar(5)
  declare @PROP1				as varchar(100)
  declare @PROP2				as varchar(100)
  declare @PROP3				as varchar(100)
  declare @DESC					as varchar(200)
  declare @CAT_LOG_ID			as varchar(50)
  declare @id					as numeric(20,0)
  declare @Documento_id 		as Numeric(20,0)
  declare @Saldo				as numeric(20,5)
  declare @TipoSaldo			as varchar(20)
  declare @Doc_Trans 			as numeric(20)
  declare @QtyDetDocumento		as numeric(20)
  declare @vUsuario_id			as varchar(50)
  declare @vTerminal			as varchar(50)
  declare @RsExist				as Cursor
  declare @RsActuRL				as Cursor
  declare @Crit1				as varchar(30)
  declare @Crit2				as varchar(30)
  declare @Crit3				as varchar(30)
  declare @fecha_alta_gtw		as datetime
  declare @nro_serie			as varchar(50)
  declare @NewLoteProveedor		as varchar(100)
  declare @OldLoteProveedor		as varchar(100)
  declare @NewNroPartida		as varchar(100)
  declare @OldNroPartida		as varchar(100)
  declare @NewNroSerie			as varchar(50)
  declare @OldNroSerie			as varchar(50)
  declare @RSDOCEGR				as cursor
  declare @DOCIDPIVOT			as numeric(20,0)
  declare @NROLINEAPIVOT		as numeric(20,0)
  DECLARE @PESOPROPS			as numeric(5,0)
  declare @PALLET_COMPLETO		as varchar(10)
  Set xAct_abort On

  --Busco todos documentos de egreso generados para el viaje.
  --Genero una tabla ordenando todos los detalles de los documentos teniendo en cuenta los pesos relativos segun propiedades.
  --Ejecuto el locator por cada documento/nro_linea en el orden del peso.

  --ESTO ES SOLO PARA PRUEBAS DESDE SQL.
	/*
  	CREATE TABLE #temp_usuario_loggin(  usuario_id				    VARCHAR(15),
                                        terminal				      VARCHAR(100),
                                        fecha_loggin			    DATETIME,
                                        session_id				    VARCHAR(15),
                                        rol_id					      VARCHAR(15),
                                        emplazamiento_default	VARCHAR(15),
                                        deposito_default		  VARCHAR(15)
                                      )
                                      
  	EXEC Funciones_Loggin_Api#Registra_Usuario_Loggin 'ADMIN'
  	*/
  -----------------------------------------

	IF EXISTS(	SELECT	1
				FROM	SYS_INT_DOCUMENTO SD INNER JOIN DOCUMENTO D 
						ON D.NRO_REMITO = SD.DOC_EXT AND D.DOCUMENTO_ID = @pDocumento_id AND D.CLIENTE_ID = @pCliente_id
				WHERE	TIPO_DOCUMENTO_ID = 'E08')
	BEGIN
		EXEC LocatorEgreso_GARANTIAS @pDocumento_id,@pViaje_id,@pCliente_id 
		RETURN
	END
			
  --#SDDPESO ASIGNA A CADA CLIENTE_ID | DOC_EXT | NRO LINEA UN PESO LOGICO DE ACUERDO A LAS PROPIEDADES NRO_LOTE, NRO_PARTIDA Y NRO_SERIE
  CREATE TABLE #SDDPESO
    (CLIENTE_ID		VARCHAR(15)
    ,CODIGO_VIAJE	VARCHAR(100)
    ,DOCUMENTO_ID	NUMERIC(20,0)
    ,NRO_LINEA		NUMERIC(20,0)
    ,PESO INT)


  INSERT INTO #SDDPESO
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
		  AND NOT EXISTS (SELECT 1 FROM PICKING PIK WHERE PIK.DOCUMENTO_ID=DD.DOCUMENTO_ID AND PIK.NRO_LINEA=DD.NRO_LINEA)
		  
  SET NOCOUNT ON;

  SELECT	@PALLET_COMPLETO = FLG_PALLET_COMPLETO 
  FROM		CLIENTE_PARAMETROS
  WHERE		CLIENTE_ID = @pCliente_id
	  

  IF @PALLET_COMPLETO='1' BEGIN

	EXEC [dbo].[LocatorEgreso_pallet_completo] @pDocumento_id output, @pCliente_id	output, @pViaje_id Output	
	RETURN
  END

  SET @vNroLinea = 0
  ----------------------------------------------------------------
  --Obtengo los criterios de ordenamiento.
  ----------------------------------------------------------------
  Select	@Crit1=CRITERIO_1, @Crit2=CRITERIO_2, @Crit3=CRITERIO_3
  From		RL_CLIENTE_LOCATOR
  Where		Cliente_id=@pCliente_id
  ----------------------------------------------------------------
  if (@Crit1 is null) and (@Crit2 is null) and (@Crit3 is null)
  begin
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
    
    SET @QtySol=0
    set @QtySol=dbo.GetQtySol(@DOCIDPIVOT,@NROLINEAPIVOT,@pCliente_id)
    set @vQtyResto=@QtySol
  
    Set @RsExist = Cursor For
      Select	X.*
      from	( SELECT	dd.fecha_vencimiento
                      ,isnull(p.orden_picking,999) as ORDEN_PICKING
                      ,'POS' as ubicacion
                      ,p.posicion_cod as posicion
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
                      ,dd.nro_serie
              FROM	  rl_det_doc_trans_posicion rl
                      inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
                      inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
                      inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
                      inner join posicion p on (rl.posicion_actual=p.posicion_id)
                      left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
                      inner join documento d on(dd.documento_id=d.documento_id)
              WHERE	  rl.doc_trans_id_egr is null
                      and rl.nro_linea_trans_egr is null
                      and rl.disponible='1'
                      and isnull(em.disp_egreso,'1')='1'
                      and isnull(em.picking,'1')='1'
                      and p.pos_lockeada='0' and p.picking='1'
					  and cl.disp_egreso='1' and cl.picking='1'
                      and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
                      --and dd.producto_id in (select producto_id from det_documento where documento_id=@DOCIDPIVOT)
                      and exists (select  1 
                                  from    det_documento ddegr
                                  where	  ddegr.documento_id = @DOCIDPIVOT 
                                          AND ddegr.nro_linea = @NROLINEAPIVOT
										  and ddegr.producto_id = dd.producto_id
										  and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
										  and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
										  and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie))
									)
					  and d.cliente_id = @pCliente_id
					  and not exists (select 1 from consumo_locator_egr where rl_id = rl.rl_id)
              UNION
              SELECT	dd.fecha_vencimiento
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
                      ,dd.nro_serie
              FROM	  rl_det_doc_trans_posicion rl
                      inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
                      inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
                      inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
                      inner join nave n on (rl.nave_actual=n.nave_id)
                      left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
                      inner join documento d on(dd.documento_id=d.documento_id)
              WHERE	  rl.doc_trans_id_egr is null
                      and rl.nro_linea_trans_egr is null
                      and rl.disponible='1'
                      and isnull(em.disp_egreso,'1')='1'
                      and isnull(em.picking,'1')='1'
                      and rl.cat_log_id<>'TRAN_EGR'
                      and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1'
                      and cl.disp_egreso='1' and cl.picking='1'
                      --and dd.producto_id in (select producto_id from det_documento where documento_id=@DOCIDPIVOT)
                      and exists (select  1 
                                  from    det_documento ddegr
                                  where	  ddegr.documento_id = @DOCIDPIVOT 
										  AND ddegr.nro_linea = @NROLINEAPIVOT
										  and ddegr.producto_id = dd.producto_id
										  and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
										  and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
										  and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie))
								 )
                      and d.cliente_id = @pCliente_id
                      and not exists (select 1 from consumo_locator_egr where rl_id = rl.rl_id)
        )X		
        order by--order by producto,dd.fecha_vencimiento asc,orden  
            (CASE WHEN 1	  = 1					            THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
            (CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
            (CASE WHEN @Crit1 = 'ORDEN_PICKING'		  THEN x.ORDEN_PICKING END),
            (CASE WHEN @Crit1 = 'NRO_BULTO'			    THEN x.NRO_BULTO END),
            (CASE WHEN @Crit1 = 'NRO_LOTE'			    THEN x.NRO_LOTE END),
            (CASE WHEN @Crit1 = 'EST_MERC_ID'		    THEN x.EST_MERC_ID END),
            (CASE WHEN @Crit1 = 'NRO_DESPACHO'		  THEN x.NRO_DESPACHO END),
            (CASE WHEN @Crit1 = 'NRO_PARTIDA'		    THEN x.NRO_PARTIDA END),
            (CASE WHEN @Crit1 = 'UNIDAD_ID'			    THEN x.UNIDAD_ID END),
            (CASE WHEN @Crit1 = 'PROP1'				      THEN x.PROP1 END),
            (CASE WHEN @Crit1 = 'PROP2'				      THEN x.PROP2 END),
            (CASE WHEN @Crit1 = 'PROP3'				      THEN x.PROP3 END),
            (CASE WHEN @Crit1 = 'CAT_LOG_ID'		    THEN x.CAT_LOG_ID END),
            (CASE WHEN @Crit1 = 'FECHA_ALTA_GTW'	  THEN x.FECHA_ALTA_GTW END),
            --2
            (CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
            (CASE WHEN @Crit2 = 'ORDEN_PICKING'		  THEN x.ORDEN_PICKING END),
            (CASE WHEN @Crit2 = 'NRO_BULTO'			    THEN x.NRO_BULTO END),
            (CASE WHEN @Crit2 = 'NRO_LOTE'			    THEN x.NRO_LOTE END),
            (CASE WHEN @Crit2 = 'EST_MERC_ID'		    THEN x.EST_MERC_ID END),
            (CASE WHEN @Crit2 = 'NRO_DESPACHO'		  THEN x.NRO_DESPACHO END),
            (CASE WHEN @Crit2 = 'NRO_PARTIDA'		    THEN x.NRO_PARTIDA END),
            (CASE WHEN @Crit2 = 'UNIDAD_ID'			    THEN x.UNIDAD_ID END),
            (CASE WHEN @Crit2 = 'PROP1'				      THEN x.PROP1 END),
            (CASE WHEN @Crit2 = 'PROP2'				      THEN x.PROP2 END),
            (CASE WHEN @Crit2 = 'PROP3'				      THEN x.PROP3 END),
            (CASE WHEN @Crit2 = 'CAT_LOG_ID'		    THEN x.CAT_LOG_ID END),
            (CASE WHEN @Crit2 = 'FECHA_ALTA_GTW'	  THEN x.FECHA_ALTA_GTW END),
            --3
            (CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
            (CASE WHEN @Crit3 = 'ORDEN_PICKING'		  THEN x.ORDEN_PICKING END),
            (CASE WHEN @Crit3 = 'NRO_BULTO'			    THEN x.NRO_BULTO END),
            (CASE WHEN @Crit3 = 'NRO_LOTE'			    THEN x.NRO_LOTE END),
            (CASE WHEN @Crit3 = 'EST_MERC_ID'		    THEN x.EST_MERC_ID END),
            (CASE WHEN @Crit3 = 'NRO_DESPACHO'		  THEN x.NRO_DESPACHO END),
            (CASE WHEN @Crit3 = 'NRO_PARTIDA'		    THEN x.NRO_PARTIDA END),
            (CASE WHEN @Crit3 = 'UNIDAD_ID'			    THEN x.UNIDAD_ID END),
            (CASE WHEN @Crit3 = 'PROP1'				      THEN x.PROP1 END),
            (CASE WHEN @Crit3 = 'PROP2'				      THEN x.PROP2 END),
            (CASE WHEN @Crit3 = 'PROP3'				      THEN x.PROP3 END),
            (CASE WHEN @Crit3 = 'CAT_LOG_ID'		    THEN x.CAT_LOG_ID END),
            (CASE WHEN @Crit3 = 'FECHA_ALTA_GTW'	  THEN x.FECHA_ALTA_GTW END)
        
    Open @RsExist
    Fetch Next From @RsExist into	@Fecha_Vto,
                    @OrdenPicking,
                    @Tipo_Posicion,
                    @Codigo_Posicion,
                    @pCliente_id,
                    @Producto_id,
                    @Cantidad,
                    @vRl_id,
                    @NRO_BULTO,
                    @NRO_LOTE,				
                    @EST_MERC_ID,			
                    @NRO_DESPACHO,		
                    @NRO_PARTIDA,			
                    @UNIDAD_ID,			
                    @PROP1,					
                    @PROP2,					
                    @PROP3,
                    @DESC,
                    @CAT_LOG_ID,
                    @fecha_alta_gtw,
                    @nro_serie
  
  
    While @@Fetch_Status=0 AND @vQtyResto>0
    Begin	
  
      if (@vQtyResto>0) begin   
         if (@vQtyResto>=@Cantidad) begin
            set @vNroLinea=isnull((select max(nro_linea) from consumo_locator_egr where documento_id = @DOCIDPIVOT),0)+1
            set @vQtyResto=@vQtyResto-@Cantidad
            insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
                  values  (@DOCIDPIVOT,@vNroLinea,@pCliente_id,@Producto_id,@Cantidad,@vRl_id,0,'1',getdate(),'N')
            --Insert con todas las propiedades en det_documento
            insert into det_documento_aux 
                (	documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
                  cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
                  unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
            values  (@DOCIDPIVOT,@vNroLinea,@pCliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
                ,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
                ,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)	
  
              
          end
          else begin
            set @vNroLinea=isnull((select max(nro_linea) from consumo_locator_egr where documento_id = @DOCIDPIVOT),0)+1
            insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)
                  values  (@DOCIDPIVOT,@vNroLinea,@pCliente_id,@Producto_id,@vQtyResto,@vRl_id,0,'2',getdate(),'N')
            --Insert con todas las propiedades en det_documento
            insert into det_documento_aux (
                  documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
                  cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
                  unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
                  values 
                  (@DOCIDPIVOT,@vNroLinea
                  ,@pCliente_id,@Producto_id,@vQtyResto,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
                  ,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
                  ,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)	
            
            --este procedimiento realiza un split en RL, deja la cantidad que se neceista en la RL actual y genera una nueva RL con el resto.
            EXEC SPLIT_RL_CONSUMO_EGRESO @vRl_id, @vQtyResto
  
            set @vQtyResto=0
          end --if
      end --if
      Fetch Next From @RsExist into	@Fecha_Vto,
                      @OrdenPicking,
                      @Tipo_Posicion,
                      @Codigo_Posicion,
                      @pCliente_id,
                      @Producto_id,
                      @Cantidad,
                      @vRl_id,
                      @NRO_BULTO,
                      @NRO_LOTE,				
                      @EST_MERC_ID,			
                      @NRO_DESPACHO,		
                      @NRO_PARTIDA,			
                      @UNIDAD_ID,			
                      @PROP1,					
                      @PROP2,					
                      @PROP3,
                      @DESC,
                      @CAT_LOG_ID,
                      @fecha_alta_gtw,
                      @nro_serie
    End	--End While @RsExist.
  
    CLOSE @RsExist
    DEALLOCATE @RsExist
    
    
    FETCH NEXT FROM @RSDOCEGR INTO @DOCIDPIVOT, @NROLINEAPIVOT, @PESOPROPS
  END
  CLOSE @RSDOCEGR
  DEALLOCATE @RSDOCEGR
  
  
  --GUARDO SERIES INICIALES
  --SELECT DISTINCT NRO_SERIE INTO #TMPSERIES FROM DET_DOCUMENTO WHERE DOCUMENTO_ID = @DOCIDPIVOT
  
  --Borro det_documento y lo vuelvo a insertar con las nuevas propiedades
  DECLARE @CURDOCS CURSOR
  SET @CURDOCS = CURSOR FOR
    select	DISTINCT DD.DOCUMENTO_ID
    FROM	DET_DOCUMENTO DD
			INNER JOIN DOCUMENTO D ON (DD.CLIENTE_ID = D.CLIENTE_ID AND DD.DOCUMENTO_ID = D.DOCUMENTO_ID)
    WHERE	D.CLIENTE_ID = @pCliente_id 
			AND D.NRO_DESPACHO_IMPORTACION = @pViaje_id
			and dd.documento_id=@PDocumento_id
			AND NOT EXISTS (SELECT 1 FROM PICKING PIK WHERE PIK.DOCUMENTO_ID=DD.DOCUMENTO_ID AND PIK.NRO_LINEA=DD.NRO_LINEA)
    order by 
			DD.documento_id
  
  OPEN @CURDOCS
  FETCH NEXT FROM @CURDOCS INTO @DOCIDPIVOT
  WHILE @@FETCH_STATUS = 0
  BEGIN
  
  
    delete det_documento where documento_id = @DOCIDPIVOT
    insert into det_documento select * from det_documento_aux where documento_id=@DOCIDPIVOT
  
  
    update documento set status='D20' where documento_id=@DOCIDPIVOT
    Exec Asigna_Tratamiento#Asigna_Tratamiento_EGR @DOCIDPIVOT
    select distinct @Doc_Trans=doc_trans_id from det_documento_transaccion where documento_id=@DOCIDPIVOT
    --Hago la reserva en RL
    Set @RsActuRL = Cursor For select [id],documento_id,Nro_Linea,Cliente_id,Producto_id,Cantidad,rl_id,saldo,tipo from consumo_locator_egr where procesado='N' and Documento_id=@DOCIDPIVOT
    Open @RsActuRL
    Fetch Next From @RsActuRL into 
                        @id,
                        @Documento_id,
                        @vNroLinea,
                        @pCliente_id,
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
                        @pCliente_id,
                        @Producto_id,
                        @Cantidad,
                        @vRl_id,
                        @Saldo,
                        @TipoSaldo
    End	--End While @RsActuRL.
    CLOSE @RsActuRL
    DEALLOCATE @RsActuRL
  
    --Si no hay existencia de ningun producto del documento lo borro para que no quede solo cabecera
    select @QtyDetDocumento=count(documento_id) from det_documento where documento_id=@DOCIDPIVOT
    if (@QtyDetDocumento=0) begin
      delete documento where documento_id=@DOCIDPIVOT 
    end else begin
      select @vUsuario_id=usuario_id, @vTerminal=Terminal from #temp_usuario_loggin
      insert into docxviajesprocesados values (@pViaje_id,@DOCIDPIVOT,'P',getdate(),@vUsuario_id,@vTerminal)
    end --if
  
    FETCH NEXT FROM @CURDOCS INTO @DOCIDPIVOT
  END
  
  CLOSE @CURDOCS
  DEALLOCATE @CURDOCS
  Set NoCount Off;
End -- Fin Procedure.





GO


