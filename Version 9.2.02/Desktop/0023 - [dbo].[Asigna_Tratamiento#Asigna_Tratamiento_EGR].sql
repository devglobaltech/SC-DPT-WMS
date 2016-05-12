
/****** Object:  StoredProcedure [dbo].[Asigna_Tratamiento#Asigna_Tratamiento_EGR]    Script Date: 08/08/2014 16:10:05 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Asigna_Tratamiento#Asigna_Tratamiento_EGR]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Asigna_Tratamiento#Asigna_Tratamiento_EGR]
GO



CREATE          Procedure [dbo].[Asigna_Tratamiento#Asigna_Tratamiento_EGR]
 							@P_Doc_Id as numeric(20,0)
As
Begin
	Declare @xSTatus varchar(10)
    Declare @VTipoDoc varchar(15)
	Declare @VTipoOp varchar(15)
	Declare @VNroLinea numeric(10,0)
	Declare @VEstMerc varchar(15)
	Declare @VClienteID varchar(15)
	Declare @VCatLog varchar(50)
	Declare @VItemOk varchar(5)
	Declare @VTransId varchar(15)
	Declare @VCant numeric(10,0)
	Declare @VStation varchar(15)
	Declare @VNroLineaTrans numeric(10,0)
	Declare @VSeq	numeric(20,0)
	Declare @xSQL	varchar(200)
	Declare @Ctrl	numeric(20,0)


	SELECT @xStatus = STATUS, @VTipoDoc = TIPO_COMPROBANTE_ID,@VTipoOp = TIPO_OPERACION_ID 
	FROM DOCUMENTO WHERE DOCUMENTO_ID= @P_DOC_ID

	If @xStatus <> 'D20'
		Begin	
			Raiserror ('El documento no esta en D20.',16,1)
			Return    
		End
		
	SELECT	@Ctrl=COUNT(DISTINCT X.TRANSACCION_ID)
	FROM	(
			SELECT  ISNULL((   SELECT TRANSACCION_ID 
								From RL_PRODUCTO_TRATAMIENTO 
								Where cliente_id = dd.cliente_id 
									AND TIPO_OPERACION_ID=D.TIPO_OPERACION_ID 
									AND TIPO_COMPROBANTE_ID=D.TIPO_COMPROBANTE_ID 
									AND PRODUCTO_ID=DD.PRODUCTO_ID)
					,P.EGRESO 
					) AS TRANSACCION_ID
			From DET_DOCUMENTO DD INNER JOIN PRODUCTO P 
									On DD.CLIENTE_ID= P.CLIENTE_ID AND 
									DD.PRODUCTO_ID = P.PRODUCTO_ID INNER JOIN DOCUMENTO D 
									On DD.DOCUMENTO_ID=D.DOCUMENTO_ID 
			Where dd.documento_id = @P_DOC_ID)X
	if @Ctrl>1 begin
		DECLARE	PCUR CURSOR FOR
			--Obtiene el tratamiento de cada producto, a nivel particular o el default
			SELECT  DD.NRO_LINEA,
					DD.EST_MERC_ID,
					DD.CLIENTE_ID,
					DD.CAT_LOG_ID,
					DD.ITEM_OK,
					ISNULL((   SELECT TRANSACCION_ID 
								From RL_PRODUCTO_TRATAMIENTO 
								Where cliente_id = dd.cliente_id 
									AND TIPO_OPERACION_ID=D.TIPO_OPERACION_ID 
									AND TIPO_COMPROBANTE_ID=D.TIPO_COMPROBANTE_ID 
									AND PRODUCTO_ID=DD.PRODUCTO_ID)
					,P.EGRESO 
					) AS TRANSACCION_ID
			From DET_DOCUMENTO DD INNER JOIN PRODUCTO P 
									On DD.CLIENTE_ID= P.CLIENTE_ID AND 
									DD.PRODUCTO_ID = P.PRODUCTO_ID INNER JOIN DOCUMENTO D 
									On DD.DOCUMENTO_ID=D.DOCUMENTO_ID 
			Where dd.documento_id = @P_Doc_Id
			ORDER BY TRANSACCION_ID,DD.NRO_LINEA 
			
		Open PCUR
		Fetch Next From PCUR Into @VNroLinea, @VEstMerc, @VClienteID
								, @VCatLog, @VItemOk, @VTransId

		While @@Fetch_Status = 0
		Begin
			If @VTransId = ''
				Begin            
					Raiserror ('NO PUEDE CONTINUAR CON ESTE DOCUMENTO HASTA QUE CARGUE LOS TRATAMIENTO EN EL MAESTRO DE PRODUCTO.',16,1)
					Return    
				End

			Select	@VCant = Count(DT.doc_trans_id)
			From	DET_DOCUMENTO_TRANSACCION DDT INNER JOIN DOCUMENTO_TRANSACCION DT
					On DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID
			Where	DDT.DOCUMENTO_ID = @P_Doc_Id AND DT.TRANSACCION_ID = @VTransId
	        
			If @VCant = 0 
				Begin

					Select @VStation = ESTACION_ID
					From RL_TRANSACCION_ESTACION
					Where TRANSACCION_ID = @VTransId AND ORDEN = 1
	            
					Set @xSql = @VTRANSID + '-' + cast(@P_DOC_ID as varchar(20))
					Exec dbo.DOCUMENTO_TRANSACCION_API#InsertRecord 
							0, @xSql, @VTransId, @VStation , 'T10'
							, 'A', Null, '0', 1, @VTipoOp , Null, '0' 
							, Null, Null, Null, Null, Null, Null, Null, @VSeq Output

					Set @VNroLineaTrans = 1
	            
					Exec Det_Documento_Transaccion_Api#InsertRecord 
							@VSeq, @VNroLineaTrans 
							, @P_Doc_Id, @VNroLinea , null, @VEstMerc  
							, @vClienteID, @VCatLog , '0', '0'
				End
			Else
				Begin
					Select  @VNroLineaTrans = Max(NRO_LINEA_TRANS)+1
					From DET_DOCUMENTO_TRANSACCION 
					Where DOCUMENTO_ID = @P_Doc_Id AND DOC_TRANS_ID = @VSeq

					If @VNroLineaTrans is null
						Begin
							Set @VNroLineaTrans = 1
						End

					Exec Det_Documento_Transaccion_Api#InsertRecord @VSeq, @VNroLineaTrans , @P_Doc_Id, @VNroLinea , NULL, @VEstMerc  
																	, @vClienteID, @VCatLog , '0', '0'
				End

			Fetch Next From PCUR Into @VNroLinea, @VEstMerc, @VClienteID
									, @VCatLog, @VItemOk, @VTransId
		End

		Close PCUR
		DEALLOCATE PCUR
	end else begin
		---------------------------------------------------------------------
		--	Version Optimizada.
		---------------------------------------------------------------------				

		SELECT  @VTransId=ISNULL((  SELECT	TRANSACCION_ID 
									From	RL_PRODUCTO_TRATAMIENTO 
									Where	cliente_id = dd.cliente_id 
											AND TIPO_OPERACION_ID=D.TIPO_OPERACION_ID 
											AND TIPO_COMPROBANTE_ID=D.TIPO_COMPROBANTE_ID 
											AND PRODUCTO_ID=DD.PRODUCTO_ID),P.EGRESO 
				) 
		From DET_DOCUMENTO DD INNER JOIN PRODUCTO P 
								On DD.CLIENTE_ID= P.CLIENTE_ID AND 
								DD.PRODUCTO_ID = P.PRODUCTO_ID INNER JOIN DOCUMENTO D 
								On DD.DOCUMENTO_ID=D.DOCUMENTO_ID 
		Where dd.documento_id = @P_Doc_Id
				
		If @VTransId = ''
			Begin            
				Raiserror ('NO PUEDE CONTINUAR CON ESTE DOCUMENTO HASTA QUE CARGUE LOS TRATAMIENTO EN EL MAESTRO DE PRODUCTO.',16,1)
				Return    
			End

		Select	@VCant = Count(DT.doc_trans_id)
		From	DET_DOCUMENTO_TRANSACCION DDT INNER JOIN DOCUMENTO_TRANSACCION DT
				On DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID
		Where	DDT.DOCUMENTO_ID = @P_Doc_Id AND DT.TRANSACCION_ID = @VTransId

		Select	@VStation = ESTACION_ID
		From	RL_TRANSACCION_ESTACION
		Where	TRANSACCION_ID = @VTransId AND ORDEN = 1
	            
		Set @xSql = @VTRANSID + '-' + cast(@P_DOC_ID as varchar(20))
		Exec dbo.DOCUMENTO_TRANSACCION_API#InsertRecord 
				0, @xSql, @VTransId, @VStation , 'T10'
				, 'A', Null, '0', 1, @VTipoOp , Null, '0' 
				, Null, Null, Null, Null, Null, Null, Null, @VSeq Output
	
		INSERT INTO DET_DOCUMENTO_TRANSACCION
		SELECT	@VSeq, DD.NRO_LINEA,DD.DOCUMENTO_ID,DD.NRO_LINEA, NULL, DD.EST_MERC_ID,DD.CLIENTE_ID,DD.CAT_LOG_ID,'0','0',NULL,NULL
		FROM	DET_DOCUMENTO DD
		WHERE	DD.DOCUMENTO_ID=@P_Doc_Id
											
	end
	
	Exec Am_Funciones_Estacion_Api#UpdateStatusDoc @P_Doc_Id, 'D30'
	--Exec Am_Funciones_Estacion_Api#DocID_A_DocTrID @P_Doc_Id

End


GO


