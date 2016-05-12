/****** Object:  StoredProcedure [dbo].[PrintEtiPalletPickingGroup]    Script Date: 06/13/2014 13:00:23 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PrintEtiPalletPickingGroup]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[PrintEtiPalletPickingGroup]
GO

CREATE Procedure [dbo].[PrintEtiPalletPickingGroup]  
@CLIENTE_ID		varchar(100)	output, 
@EtiquetaId		varchar(20)		output
--@UsuarioImp		varchar(20)		output -- LRojas TrackerID 3806 05/03/2012: Impresora por Usuario
as  
Begin  
	Select	@EtiquetaId=ISNULL(etiqueta_id,0) 
	from	etiqueta_producto   
	where	cliente_id=@CLIENTE_ID  
			and Producto_id='ETI_PALLET_EGR' and tipo_operacion_id='EGR'  
			
	If @EtiquetaId is null  
	Begin  
		Set @EtiquetaId=0  
	End   
	 --Obtengo el grupo de datos.  
	 --SELECT  row_number() over (order by )  as numerador     
	  
	  
	-- SELECT  row_number() over (order by [PICKING.VIAJE_ID])  as numerador ,* FROM   
	-- (SELECT  P.VIAJE_ID        [PICKING.VIAJE_ID]  
	--   ,P.PALLET_PICKING       [PICKING.PALLET_PICKING]  
	--   --,SUM(CANT_CONFIRMADA)     [PICKING.CANT_CONFIRMADA]  
	--   ,sum(CANT_CONFIRMADA)     [PICKING.CANT_CONFIRMADA]  
	--   ,D.NRO_REMITO       [DOCUMENTO_EGR.NRO_REMITO]  
	-- FROM PICKING P(NOLOCK) INNER JOIN DOCUMENTO D  
	--   ON(D.DOCUMENTO_ID=P.DOCUMENTO_ID)  
	--   inner join   
	--     (SELECT distinct pp.VIAJE_ID, i.pallet FROM IMPRESION_APF I  
	--     INNER JOIN    
	--       (SELECT VIAJE_ID, CLIENTE_ID, PALLET_PICKING FROM PICKING   
	--       WHERE FECHA_INICIO IS NOT NULL AND FECHA_FIN IS NOT NULL AND USUARIO IS NOT NULL AND CANT_CONFIRMADA IS NOT NULL and cliente_id = @CLIENTE_ID  
	--       GROUP BY VIAJE_ID, CLIENTE_ID, PALLET_PICKING) PP  
	--         ON (PP.PALLET_PICKING = I.PALLET )  
	--     WHERE ((I.IMPRESO IS NULL) OR(I.IMPRESO='0')) AND I.TIPO_ETI = 2  
	--     --ORDER BY P.CLIENTE_ID, I.PALLET  
	--     ) x   
	--    on (x.viaje_id = p.viaje_id and x.pallet = p.pallet_picking)  
	-- WHERE p.cliente_id =@CLIENTE_ID  
	-- GROUP BY  
	--   P.VIAJE_ID,P.PALLET_PICKING, D.NRO_REMITO  
	-- ) XX  
	-- ORDER BY XX.[PICKING.VIAJE_ID],XX.[PICKING.PALLET_PICKING]  
  
 SELECT  row_number() over (order by [PICKING.VIAJE_ID])  as numerador ,* FROM   
 (SELECT  P.VIAJE_ID        [PICKING.VIAJE_ID]  
   ,P.PALLET_PICKING       [PICKING.PALLET_PICKING]  
   --,SUM(CANT_CONFIRMADA)     [PICKING.CANT_CONFIRMADA]  
   ,sum(CANT_CONFIRMADA)     [PICKING.CANT_CONFIRMADA]  
   ,' '           [DOCUMENTO_EGR.NRO_REMITO]  
 FROM PICKING P(NOLOCK)   
   inner join   
     (SELECT distinct pp.VIAJE_ID, i.pallet FROM IMPRESION_APF I  
     INNER JOIN    
       (SELECT VIAJE_ID, CLIENTE_ID, PALLET_PICKING FROM PICKING   
       WHERE FECHA_INICIO IS NOT NULL AND FECHA_FIN IS NOT NULL AND USUARIO IS NOT NULL AND CANT_CONFIRMADA IS NOT NULL and cliente_id = @CLIENTE_ID  
       GROUP BY VIAJE_ID, CLIENTE_ID, PALLET_PICKING) PP  
         ON (PP.PALLET_PICKING = I.PALLET )  
     WHERE ((I.IMPRESO IS NULL) OR(I.IMPRESO='0')) AND I.TIPO_ETI = 2  
			--AND I.USUARIO_IMP = @UsuarioImp -- LRojas TrackerID 3806 05/03/2012: Impresora por Usuario
     --ORDER BY P.CLIENTE_ID, I.PALLET  
     ) x   
    on (x.viaje_id = p.viaje_id and x.pallet = p.pallet_picking)  
 WHERE p.cliente_id =@CLIENTE_ID  
 GROUP BY  
   P.VIAJE_ID,P.PALLET_PICKING  
 ) XX  
 ORDER BY XX.[PICKING.VIAJE_ID],XX.[PICKING.PALLET_PICKING]  
  
  
   
-------------------------------------------------------------------  
End--Fin procedure.

GO


