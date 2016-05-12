/****** Object:  StoredProcedure [dbo].[SetUCDesconsolidacionxproducto_split]    Script Date: 09/18/2013 17:33:55 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SetUCDesconsolidacionxproducto_split]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SetUCDesconsolidacionxproducto_split]
GO

CREATE PROCEDURE [dbo].[SetUCDesconsolidacionxproducto_split]              
@picking_id varchar(20) output,              
@cantidad_desconsolidacion numeric(5,0) output,              
@nro_ucdesconsolidacion varchar(50) output              
              
as              
begin              
        
declare @usuario as varchar(100)              
declare @terminal as varchar(100)              
declare @Dif as numeric(5,0)              
        
SELECT  @usuario = Su.nombre, @terminal = tul.Terminal        
FROM    #TEMP_USUARIO_LOGGIN TUL        
INNER JOIN SYS_USUARIO SU        
ON (TUL.USUARIO_ID = SU.USUARIO_ID)        
              
SET @Dif = (select cant_confirmada - @cantidad_desconsolidacion from picking where picking_id = @picking_id)              
              
UPDATE PICKING SET cantidad = @cantidad_desconsolidacion, CANT_CONFIRMADA = @CANTIDAD_DESCONSOLIDACION, NRO_UCDESCONSOLIDACION = @NRO_UCDESCONSOLIDACION,      
fecha_desconsolidacion = getdate(), usuario_desconsolidacion = @usuario, terminal_desconsolidacion = @terminal      
WHERE PICKING_ID = @PICKING_ID              
              
Insert into Picking              
 Select   Documento_id   ,Nro_Linea   ,Cliente_Id   ,Producto_id              
   ,Viaje_Id    ,Tipo_Caja   ,Descripcion  ,@DIF              
   ,Nave_Cod    ,Posicion_cod  ,Ruta    ,prop1              
   ,Fecha_inicio      ,fecha_fin ,usuario   ,@DIF                
   ,pallet_picking     ,0     ,'0'    ,null                
   ,'0'     ,'0'    ,'0'    ,fin_picking              
   ,'0'     ,null    ,null    ,null              
   ,null     ,null    ,null    ,null              
   ,null     ,null    ,null    ,hijo              
   ,null     ,pallet_final    ,null    ,null              
   ,null     ,Remito_Impreso  ,Nro_Remito_PF  ,ISNULL(PICKING_ID_REF,PICKING_ID)              
   ,null     ,BULTOS_NO_CONTROLADOS     ,FLG_PALLET_HOMBRE          
   ,TRANSF_TERMINADA,NULL,NULL,'1',null,null,null,null,null,null        
 From Picking              
 Where Picking_id=@picking_id              
end



GO


