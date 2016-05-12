
/****** Object:  StoredProcedure [dbo].[SetHojadeCarga]    Script Date: 10/06/2014 11:01:07 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SetHojadeCarga]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[SetHojadeCarga]
GO

CREATE PROCEDURE [dbo].[SetHojadeCarga]                            
@GUIAS AS VARCHAR(2000) OUTPUT,                  
@HOJACARGA AS NUMERIC(15) OUTPUT                  
                      
AS                     
BEGIN                         
BEGIN TRAN                 
 /*            
 DECLARE @GUIAS AS VARCHAR(200)                    
 SET @GUIAS = '36,38'                    
 DECLARE @HOJACARGA AS INT                           
 */            
            
 DECLARE @VIAJE AS VARCHAR(50)                     
 DECLARE @cVIAJE AS VARCHAR(50)                       
 --DECLARE @PICKID AS NUMERIC(20,0)                      
 DECLARE @UCEMPAQUE AS VARCHAR(100)                      
 DECLARE @PARAM AS VARCHAR(MAX)                          
 DECLARE @VAL AS VARCHAR(MAX)                
 DECLARE @PEDIDO AS VARCHAR(30)      
 DECLARE @CLIENTE_ID AS VARCHAR(15)
 DECLARE @DOCUMENTO_ID AS NUMERIC(20,0) 
                           
 SET @PARAM = @GUIAS                          
 IF (SELECT OBJECT_ID('tempdb.dbo.#TBL','U')) IS NULL                    
 BEGIN                          
  CREATE TABLE #TBL (A  VARCHAR(50))                          
 END                     
 --PARCEO LA LISTA DE GUIAS A UNA TABLA                           
 WHILE LEN( @param ) > 0                     
  BEGIN                           
  IF CHARINDEX( ',', @param ) > 0                           
   SELECT @val = LEFT( @param, CHARINDEX( ',', @param )  - 1 ) ,                          
       @param = RIGHT( @param, LEN( @param ) - CHARINDEX( ',', @param ) )                           
  ELSE                           
   SELECT @val = @param, @param = SPACE(0)                          
  EXEC('INSERT #TBL VALUES (' + @val + ')' )                           
 END                          
                    
 --OBTENGO NRO DE HOJA DE CARGA                          
 SET @HOJACARGA = (SELECT VALOR + 1 FROM SECUENCIA WHERE NOMBRE = 'HOJA_CARGA')                            
 UPDATE SECUENCIA SET VALOR = @HOJACARGA WHERE NOMBRE = 'HOJA_CARGA'                            
                    
 --GUARDO EL NRO DE HOJA DE CARGA EN CADA GUIA                    
 UPDATE UC_EMPAQUE SET NRO_HOJACARGA = @hojacarga, FECHA_HOJACARGA = GETDATE() WHERE NRO_GUIA IN (SELECT A COLLATE Latin1_General_CI_AS FROM #TBL)                           
 
 DECLARE CURR CURSOR FOR                       
  SELECT DISTINCT P.NRO_UCEMPAQUETADO,P.VIAJE_ID                      
  FROM PICKING P                       
  INNER JOIN UC_EMPAQUE U                       
  ON U.UC_EMPAQUE = P.NRO_UCEMPAQUETADO                       
  WHERE U.NRO_GUIA IN (SELECT A COLLATE Latin1_General_CI_AS FROM #TBL)                    
  ORDER BY P.VIAJE_ID            
 OPEN CURR                      
 FETCH NEXT FROM CURR INTO @UCEMPAQUE,@VIAJE                    
                       
 WHILE @@FETCH_STATUS = 0                      
 BEGIN                     
   --AGRUPO POR HOJADECARGA EN PICKING                      
   UPDATE PICKING SET VIAJE_ID = @HOJACARGA, PALLET_PICKING = @UCEMPAQUE WHERE NRO_UCEMPAQUETADO = @UCEMPAQUE 
   --SE AGREGA PARA QUE ACTUALICE TAMBIEN LOS QUE FUERON PICKEADOS POR CERO
   SELECT @DOCUMENTO_ID = DOCUMENTO_ID FROM PICKING WHERE NRO_UCEMPAQUETADO = @UCEMPAQUE
   UPDATE PICKING SET VIAJE_ID = @HOJACARGA WHERE DOCUMENTO_ID = @DOCUMENTO_ID AND CANT_CONFIRMADA = '0'
   
 FETCH NEXT FROM CURR INTO @UCEMPAQUE,@VIAJE                      
 END                      
 CLOSE CURR;                      
 DEALLOCATE CURR;                    
            
    --CERRAR PICK                      
    
    --EXEC SYS_DEV_EGRESO_CON_HOJACARGA @HOJACARGA                      
    --EXEC FRONTERA_FINALIZAR_VIAJE_STATION @HOJACARGA                      
    --EXEC INGRESO_TERCERISTA_POR_EGRESO @HOJACARGA                       
    
       
DECLARE CURR_DOC CURSOR FOR            
 SELECT DISTINCT      
  P.CLIENTE_ID,      
  D.NRO_REMITO      
 FROM PICKING P      
  INNER JOIN DOCUMENTO D       
   ON D.DOCUMENTO_ID = P.DOCUMENTO_ID      
  INNER JOIN UC_EMPAQUE U       
   ON P.NRO_UCEMPAQUETADO = U.UC_EMPAQUE      
 WHERE      
  U.NRO_HOJACARGA = @HOJACARGA      
OPEN CURR_DOC      
FETCH NEXT FROM CURR_DOC INTO @CLIENTE_ID,@PEDIDO      
WHILE @@FETCH_STATUS = 0                      
BEGIN    
 --NOTIFICACION DE EGRESO PARA EL ERP       
  --EXEC ERP_CARGAR_DATOS @CLIENTE_ID, @PEDIDO      
  FETCH NEXT FROM CURR_DOC INTO @CLIENTE_ID,@PEDIDO      
END       
CLOSE CURR_DOC      
DEALLOCATE CURR_DOC      
      
      
COMMIT TRAN                      
 --DROP TABLE #TBL                
END

GO


