
/****** Object:  StoredProcedure [dbo].[Mob_BuscarDocID]    Script Date: 10/03/2014 11:58:18 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_BuscarDocID]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_BuscarDocID]
GO

CREATE PROCEDURE [dbo].[Mob_BuscarDocID]  
 @Documento_Id  as Numeric(20,0),  
 @VALUE    AS NUMERIC(38) OUTPUT,  
 @SUCURSAL_ID  AS VARCHAR(20) OUTPUT,
 @CLIENTE_ID	AS VARCHAR(15) output
  
AS  
begin  
/* parametro quitado:  --@CLIENTE_ID AS VARCHAR(20) OUTPUT */

 declare @Id_oc  varchar(20)   
 declare @Id_remito varchar(20)   
 declare @Id   NUMERIC(38)  
 declare @usuario varchar(15)  
   
 select @usuario=usuario_id from #temp_usuario_loggin   
 --set @usuario ='ADMIN'  
   
 select 
		@Id_oc=d.orden_de_compra, 
		@Id_remito=d.nro_remito, 
		@SUCURSAL_ID=sucursal_origen,
		@CLIENTE_ID = CLIENTE_ID
 from documento d where d.documento_id = @Documento_Id  
     
 if @Id_oc is null and @Id_remito is null   
  begin  
   raiserror('Este documento no tiene OC o Remito',16,1)  
   return  
  end  
 else    
 begin  
  exec get_value_for_sequence  'NRO_OC', @Id Output  
  set @value =@Id     
    
  insert into tmp_oc    
    SELECT @Id, IdProveedor, OC, '0',  @USUARIO  
    FROM  TMP_OC  
    where id_oc=@id_remito--@id_oc  
  
--  INSERT INTO TMP_OC  
--    SELECT @Id,D.SUCURSAL_ORIGEN,DD.PROP3,'0',@usuario   
--    FROM DOCUMENTO D  
--     INNER JOIN DET_DOCUMENTO DD ON(D.DOCUMENTO_ID=DD.DOCUMENTO_ID)  
--    WHERE DD.DOCUMENTO_ID=@Documento_Id  
      
  insert into tmp_remito  
   SELECT @Id, IDPROVEEDOR, REMITO, '0', @USUARIO  
   FROM   TMP_REMITO  
   where id_remito=@id_remito  
     
 end   
end




GO


