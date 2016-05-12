/****** Object:  StoredProcedure [dbo].[Asignacion_Automatica_Prioridad_Picking]    Script Date: 10/18/2013 15:36:07 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Asignacion_Automatica_Prioridad_Picking]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Asignacion_Automatica_Prioridad_Picking]
GO

CREATE PROCEDURE [dbo].[Asignacion_Automatica_Prioridad_Picking]              
AS      
BEGIN                      
	--Declaracion de Variables              
	DECLARE @Estado_Proceso varchar(1)              
	DECLARE @Escalar_Picking int              
	DECLARE @Maxima_Prioridad int              
	DECLARE @Counter int              
	DECLARE @Cod_Viaje   varchar(100)              
	DECLARE @cantRlViaje int              
	DECLARE @CURRVIAJES CURSOR    
	DECLARE @CURSOR_ASIGNACION_AUTOMATICA_PRIORIDADES cursor              
	DECLARE @CLIENTE_ID NUMERIC (13)    
	DECLARE @DOC_EXT  VARCHAR(100)    

                  
    --Inicializacion de Variables              
                  
    SET @Escalar_Picking = (SELECT	VALOR 
							FROM	SYS_PARAMETRO_PROCESO               
                            WHERE	PROCESO_ID='WARP' AND SUBPROCESO_ID='PICKING_CN'               
									AND PARAMETRO_ID='PICKING_ESCALAR')
									
	SET @Estado_Proceso = (	SELECT	Proceso_Activo 
							FROM	Prioridades_Pickeadores               
							WHERE	ID = (SELECT max(ID) FROM Prioridades_Pickeadores))              
							
    SET @Maxima_Prioridad =(SELECT	max(pv.prioridad )       
							FROM	SYS_INT_DOCUMENTO as SID LEFT JOIN Prioridad_Viaje as PV               
									ON SID.CODIGO_VIAJE = PV.VIAJE_ID               
                            WHERE	SID.ESTADO_GT is  NULL AND SID.FECHA_ESTADO_GT IS NULL               
									AND SID.TIPO_DOCUMENTO_ID IN               
									(	SELECT	tipo_comprobante_id 
										FROM	tipo_comprobante 
										WHERE	tipo_operacion_id='EGR')              
									AND PV.PRIORIDAD is not null)              
    --------------------------------------------------------------------                
	--Primera Fila de Prioridades_Pickeadores              
	--------------------------------------------------------------------
	IF @Maxima_Prioridad > '0' BEGIN             
		SET @Counter = @Maxima_Prioridad + @Escalar_Picking           
	END        
	ELSE BEGIN        
		SET @Counter = '1'        
	END          
       
  IF @Maxima_Prioridad is null               
	BEGIN              
		set @Maxima_Prioridad = '1'              
	END              
    --Primera Linea de Prioridades_Pickeadores              
    IF @Estado_Proceso is null              
       BEGIN              
           set @Estado_Proceso = '1'              
    END              
    --------------------------------------------------------------------
    -- Condicion de Asignacion Automatica de Pickeadores 
    --------------------------------------------------------------------            
    IF (@Estado_Proceso = '1' ) BEGIN              
		/*    
		DELETE FROM PRIORIDAD_VIAJE    
		DELETE FROM RL_VIAJE_USUARIO WHERE VIAJE_ID = 'TEST7'    
		UPDATE SYS_INT_DOCUMENTO  SET ESTADO_GT = NULL, FECHA_ESTADO_GT = NULL WHERE CODIGO_VIAJE = 'TEST7'        
		*/    
		SET @CURSOR_ASIGNACION_AUTOMATICA_PRIORIDADES = cursor FOR  --Primer Cursor para el el primer Insert               
			SELECT	Codigo_Viaje    
			FROM	SYS_INT_DOCUMENTO as SID LEFT JOIN Prioridad_Viaje as PV               
					ON SID.CODIGO_VIAJE = PV.VIAJE_ID               
			WHERE	SID.ESTADO_GT is NULL     
					AND SID.FECHA_ESTADO_GT IS NULL               
					AND SID.TIPO_DOCUMENTO_ID IN (	SELECT	tipo_comprobante_id 
													FROM	tipo_comprobante 
													WHERE	tipo_operacion_id='EGR')              
					AND PV.PRIORIDAD is NULL                
					AND CODIGO_VIAJE IS NOT NULL      
    
		OPEN @CURSOR_ASIGNACION_AUTOMATICA_PRIORIDADES         
		FETCH NEXT FROM @CURSOR_ASIGNACION_AUTOMATICA_PRIORIDADES INTO @Cod_Viaje               
                                             
                  
    WHILE(@@fetch_status = 0 ) BEGIN               
		BEGIN TRY    
			BEGIN TRANSACTION    
			--PRIORIDAD    
			INSERT INTO PRIORIDAD_VIAJE (VIAJE_ID,PRIORIDAD)VALUES( @Cod_Viaje , @counter)              
			--PICKEADORES    
			INSERT INTO RL_VIAJE_USUARIO (VIAJE_ID,USUARIO_ID)               
			(SELECT DISTINCT 
					@Cod_Viaje
					,HH.USUARIO_ID 
			 FROM	SYS_HANDHELD_MENU H INNER JOIN SYS_PERMISOS_HH HH 
					ON H.CODIGO_ID=HH.CODIGO_MENU AND H.DESCRIPCION = 'PICKING')
			COMMIT TRANSACTION      
		END TRY    
		BEGIN CATCH    
			IF @@TRANCOUNT > 0 BEGIN     
				ROLLBACK     
			END     
		END CATCH      
    
		FETCH NEXT FROM @CURSOR_ASIGNACION_AUTOMATICA_PRIORIDADES INTO @Cod_Viaje              
		SET @Counter =(@counter + @Escalar_Picking ) --Contador de Prioridades              
	END              

	CLOSE @CURSOR_ASIGNACION_AUTOMATICA_PRIORIDADES              
	DEALLOCATE @CURSOR_ASIGNACION_AUTOMATICA_PRIORIDADES              

	END                    
END  


GO


