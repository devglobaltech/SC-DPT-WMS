USE [WMS_ELECTRO_906_MATCH]
GO

/*
Script created by Quest Change Director for SQL Server at 16/04/2013 05:01 p.m.
Please back up your database before running this script
*/

PRINT N'Synchronizing objects from DESARROLLO_906 to WMS_ELECTRO_906_MATCH'
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

/*
Esta funcion esta cambiada respecto a la de warp para poder saber si hubo o no diferencia 
e inventario a raiz del proceso de picking.
*/

ALTER                   Procedure [dbo].[Funciones_Estacion_api#BorrarDocTREgreso]
@Doc_Trans_Id 	as numeric(20,0)
As
Begin

	Declare @Cant_Sol	as Float
	Declare @Cant_Conf	as Float
	Declare @NewRlId	as numeric(20,0)
	Declare @Dif		as numeric
	Declare @FEABDE		Cursor
	Declare @RlId		as numeric(20,0)
	Declare @DocTransO	as numeric(20,0)
	Declare @NroLinTrO	as numeric(10,0)
	Declare @NroLinTr	as numeric(10,0)
	Declare @DocId		as numeric(20,0)
	Declare @NroLinDoc	as numeric(10,0)
	Declare @Control	as int
	
	UPDATE historico_pos_ocupadas2 SET fecha=getdate()
	DELETE FROM rl_det_doc_trans_posicion WHERE doc_trans_id_egr=@Doc_Trans_Id
	/*
	Set @FEABDE=Cursor For
		SELECT   RL.RL_ID
				,RL.DOC_TRANS_ID
				,RL.NRO_LINEA_TRANS
				,DDT.NRO_LINEA_TRANS
				,DDT.DOCUMENTO_ID
				,DDT.NRO_LINEA_DOC
		FROM	DET_DOCUMENTO_TRANSACCION DDT 
				INNER JOIN RL_DET_DOC_TRANS_POSICION RL
				ON(DDT.DOC_TRANS_ID=RL.DOC_TRANS_ID_EGR AND DDT.NRO_LINEA_TRANS=RL.NRO_LINEA_TRANS_EGR)
		WHERE	DDT.DOC_TRANS_ID=@Doc_Trans_Id

	Open @FEABDE

	Fetch Next From @FEABDE into @RlId,@DocTransO,@NroLinTrO,@NroLinTr,@DocId,@NroLinDoc
	While @@Fetch_Status=0
	Begin
	
		select 	@Cant_Sol=dd.Cantidad	
		from 	rl_det_doc_trans_posicion rl 
				inner join det_documento_transaccion ddt 
				on(rl.doc_trans_id_egr=ddt.doc_trans_id and rl.nro_linea_trans_egr=ddt.nro_linea_trans)
				inner join det_documento dd
				on(ddt.documento_id=dd.documento_id and ddt.nro_linea_doc=dd.nro_linea)
		where	ddt.doc_trans_id=@Doc_Trans_Id and dd.documento_id=@docId and dd.nro_linea=@NroLinDoc

		select 	@Cant_Conf=p.cant_confirmada	
		from 	rl_det_doc_trans_posicion rl 
				inner join det_documento_transaccion ddt 
				on(rl.doc_trans_id_egr=ddt.doc_trans_id and rl.nro_linea_trans_egr=ddt.nro_linea_trans)
				inner join det_documento dd
				on(ddt.documento_id=dd.documento_id and ddt.nro_linea_doc=dd.nro_linea)
				inner join Picking p
				on(dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
		where	ddt.doc_trans_id=@Doc_Trans_Id and dd.documento_id=@docId and dd.nro_linea=@NroLinDoc

		If @Cant_sol=@Cant_Conf
		Begin
			DELETE FROM rl_det_doc_trans_posicion WHERE doc_trans_id_egr=@Doc_Trans_Id and nro_linea_trans_egr=@NroLinTr
		End
		Else
		Begin
			Set @Dif=0
			Set @Dif= @Cant_sol - @Cant_Conf

			Select	@Control=Count(Rl_Id)
			from	rl_det_doc_trans_posicion
			where	Doc_trans_id=@DocTransO and nro_linea_trans=@NroLinTrO
					and doc_trans_id_egr is null --and cat_log_id<>'DIF_INV'

			if @Control > 0
			Begin
				Insert into rl_det_doc_trans_posicion
					select 	 doc_trans_id
							,nro_linea_trans
							,posicion_anterior
							,posicion_actual
							,@dif as cantidad
							,tipo_movimiento_id
							,Ultima_estacion
							,Ultima_Secuencia
							,nave_anterior
							,nave_actual
							,documento_id
							,nro_linea
							,disponible
							,null as doc_trans_id_egr
							,null as nro_linea_trans_egr
							,doc_trans_id_tr
							,nro_linea_trans_tr
							,cliente_id
							,'DIF_INV' as cat_log_id
							,cat_log_id_final
							,est_merc_id
					from	rl_det_doc_trans_posicion
					where	Doc_trans_id=@DocTransO and nro_linea_trans=@NroLinTrO
							and doc_trans_id_egr is null and cat_log_id<>'DIF_INV'

					DELETE FROM rl_det_doc_trans_posicion WHERE doc_trans_id_egr=@Doc_Trans_Id and nro_linea_trans_egr=@NroLinTr

			End
			Else
			Begin
				Insert into rl_det_doc_trans_posicion
					select 	 doc_trans_id
							,nro_linea_trans
							,posicion_anterior
							,posicion_actual
							,@dif as cantidad
							,tipo_movimiento_id
							,Ultima_estacion
							,Ultima_Secuencia
							,nave_anterior
							,nave_actual
							,documento_id
							,nro_linea
							,disponible
							,null as doc_trans_id_egr
							,null as nro_linea_trans_egr
							,doc_trans_id_tr
							,nro_linea_trans_tr
							,cliente_id
							,'DIF_INV' as cat_log_id
							,cat_log_id_final
							,est_merc_id
					from	rl_det_doc_tr_pos_hist
					where	Doc_trans_id=@DocTransO and nro_linea_trans=@NroLinTrO
							and nave_actual<>2 and cat_log_id<>'DIF_INV'

				DELETE FROM rl_det_doc_trans_posicion WHERE doc_trans_id_egr=@Doc_Trans_Id and nro_linea_trans_egr=@NroLinTr
			End
		End
		Fetch Next From @FEABDE into @RlId,@DocTransO,@NroLinTrO,@NroLinTr,@DocId,@NroLinDoc
	End
	close @FEABDE
	Deallocate @FEABDE
	*/
End--Fin Procedure
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER          Procedure [dbo].[Funciones_Estacion_Api#Obtener_Rl_Id_Documento] 
	-------------------------------------------------
	--TABLAS rl_det_doc_trans_posicion, posicion, nave
	--TABLAS det_documento, det_documento_transaccion, categoria_logica
	-------------------------------------------------
	@P_Cliente_Id 		as varChar (15), 
	@P_Producto_Id 		as varChar (30), 
    @P_Nro_Serie 		as varChar (50), 
	@P_Nro_Lote 		as varChar (50), 
	@P_Fec_Vto 			as datetime, 
	@P_Nro_Partida		as varChar (50), 
	@P_Nro_Despacho 	as varChar (50), 
	@P_Nro_Bulto 		as varChar (50), 
	@P_Nave 			as numeric (20,0), 
	@P_Posicion 		as numeric (20,0), 
	@P_Cat_Log_Id 		as varChar (50), 
	@P_Opcion 			as int,
	@P_Est_Merc_Id 		as varChar (15), 
	@P_Prop1 			as varChar (100), 
	@P_Prop2 			as varChar (100), 
	@P_Prop3 			as varChar (100), 
	@P_Peso 			as numeric(20,5), 
	@P_Volumen 			as numeric(20,5), 
	@P_Unidad_Id 		as varChar (5), 
	@P_Unidad_Peso 		as varChar (5), 
	@P_Unidad_Volumen 	as varChar (5), 
	@P_Moneda_Id 		as varChar (20), 
	@P_Costo 			as numeric(10,3),
	-------------------------------------------------
	--CURSORES.
	-------------------------------------------------
	@Pcur				cursor varying output
 
As
Begin
	-------------------------------------------------
	--GENERICAS
	-------------------------------------------------
	Declare @Pfec_Vto_Hta 	datetime
	Declare @PPosicion_Nave numeric (20,0)
	Declare @CurSqlString 	nvarChar (4000)
	Declare @ParmDefinition nvarChar(500)
	-------------------------------------------------
	--Query String
	-------------------------------------------------
	Declare @StrSql 		nvarChar(4000) 
	Declare @Aux_Where 		nvarChar(4000) 
	Declare @Aux_Where1 	nvarChar(4000) 
	Declare @Msg 			nvarChar(4000) 
	Declare @Msg1 			nvarChar(4000) 
	-------------------------------------------------

	Set @StrSql = ''
	Set @Aux_Where = ''
	Set @Aux_Where1 = ''
	Set @Msg = ''
	Set @Msg1 = ''

	If (@P_Cat_Log_Id Is not null) and (@P_Opcion <> 3)
		Begin		
			Set @Aux_Where = @Aux_Where + 'and rl.cat_log_id =' + Char(39) + @P_Cat_Log_Id + Char(39) + Char(13)
		End

	If (@P_Fec_Vto Is not null)
		Begin
			Set @Pfec_Vto_Hta = @P_Fec_Vto
			Set @Aux_Where = @Aux_Where + ' and CONVERT(VARCHAR, dt.fecha_vencimiento ,101) between (CONVERT(VARCHAR, Cast(' + Char(39) + Cast(@P_Fec_Vto as Varchar(17)) + Char(39) +' as Datetime) ,101)) and (CONVERT(VARCHAR,Cast(' + CHar(39) + Cast(@P_Fec_Vto as varchar(17)) + Char(39) + 'as DateTime) ,101))' + Char(13)
		End

	If (@P_Nro_Lote Is not null) and (Ltrim(Rtrim(@P_Nro_Lote)) <> '')
		Begin		
			Set @Aux_Where = @Aux_Where + 'and dt.nro_lote =' + Char(39) + @P_Nro_Lote + Char(39) + Char(13)
		End

	If (@P_Nro_Serie Is not null) and (Ltrim(Rtrim(@P_Nro_Serie)) <> '')
		Begin
			Set @Aux_Where = @Aux_Where + 'and dt.nro_serie =' + Char(39) + @P_Nro_Serie + Char(39) + Char(13)
		End

	If (@P_Nro_Partida Is not null) and (Ltrim(Rtrim(@P_Nro_Partida)) <> '')
		Begin
			Set @Aux_Where = @Aux_Where + 'and dt.nro_partida =' + Char(39) + @P_Nro_Partida + Char(39) + Char(13)
		End

	If (@P_Nro_Despacho Is not null) and (Ltrim(Rtrim(@P_Nro_Despacho)) <> '')
		Begin
			Set @Aux_Where = @Aux_Where + 'and dt.nro_despacho =' + Char(39) + @P_Nro_Despacho + Char(39) + Char(13)
		End


	If (@P_Nro_Bulto Is not null) and (Ltrim(Rtrim(@P_Nro_Bulto)) <> '')
		Begin
			Set @Aux_Where = @Aux_Where + 'and dt.nro_bulto =' + Char(39) + @P_Nro_Bulto + Char(39) + Char(13)
		End

	If (@P_Posicion Is not null)
		Begin
			Set @PPosicion_Nave = @P_Posicion
			Set @Aux_Where = @Aux_Where + ' and rl.posicion_actual =' +  cast(@PPosicion_Nave as varChar(20)) + Char(13)
		End
	Else
		Begin
			Set @PPosicion_Nave = @P_Nave
			Set @Aux_Where = @Aux_Where + ' and rl.nave_actual = ' +  cast(@PPosicion_Nave as varChar(20)) + Char(13)
		End 

	If (@P_Est_Merc_Id Is not null) and (Ltrim(Rtrim(@P_Est_Merc_Id)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and rl.est_merc_id =' + Char(39) + @P_Est_Merc_Id + Char(39) + Char(13)
		End

	If (@P_Prop1 Is not null) and (Ltrim(Rtrim(@P_Prop1)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.prop1 =' + Char(39) + @P_Prop1 + Char(39) + Char(13)
		End

	If (@P_Prop2 Is not null) and (Ltrim(Rtrim(@P_Prop2)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.prop2 =' + Char(39) + @P_Prop2 + Char(39) + Char(13)
		End

	If (@P_Prop3 Is not null) and (Ltrim(Rtrim(@P_Prop3)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.prop3 =' + Char(39) + @P_Prop3 + Char(39) + Char(13)
		End

	If (Ltrim(Rtrim(@p_peso)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.peso =' + cast(@p_peso as varChar(25)) + Char(13)
		End

	If (Ltrim(Rtrim(@P_Volumen)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.volumen =' + cast(@P_Volumen as varChar(25)) + Char(13)
		End

	If (@P_Unidad_Id Is not null) and (Ltrim(Rtrim(@P_Unidad_Id)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.unidad_id =' + Char(39) + @P_Unidad_Id + Char(39) + Char(13)
		End

	If (@P_Unidad_Peso Is not null) and (Ltrim(Rtrim(@P_Unidad_Peso)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.unidad_peso =' + Char(39) + @P_Unidad_Peso + Char(39) + Char(13)
		End

	If (@P_Unidad_Volumen Is not null) and (Ltrim(Rtrim(@P_Unidad_Volumen)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.unidad_volumen =' + Char(39) + @P_Unidad_Volumen + Char(39) + Char(13)
		End

	If (@P_Moneda_Id Is not null) and (Ltrim(Rtrim(@P_Moneda_Id)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.moneda_id =' + Char(39) + @P_Moneda_Id + Char(39) + Char(13)
		End

	If (@P_Costo Is not null) and (Ltrim(Rtrim(@P_Costo)) <> '')
		Begin
			Set @Aux_Where1 = @Aux_Where1 + 'and dt.costo =' + @P_Costo + Char(13)
		End

	Set @Aux_Where = @Aux_Where + 'and dt.cliente_id = ' + Char(39) + upper(Ltrim(Rtrim(@P_Cliente_Id))) + Char(39) + Char(13) 
	Set @Aux_Where = @Aux_Where + 'and dt.producto_id= ' + Char(39) + upper(Ltrim(Rtrim(@P_Producto_Id))) + Char(39) + Char(13)

	If (@P_Opcion = 1 or @P_Opcion = 3 or @P_Opcion = 4 or @P_Opcion = 5)
		Begin
			Set @Msg = @Msg + 'select rl_id ' + Char(13)
		End
	Else
		Begin
			Set @Msg = @Msg + 'select sum(rl.cantidad) ' + Char(13)
		End 

	Set @Msg = @Msg + 'from  rl_det_doc_trans_posicion rl ' + Char(13)
	Set @Msg = @Msg + '		left outer join posicion p on rl.posicion_actual = p.posicion_id ' + Char(13)
	Set @Msg = @Msg + '		left outer join nave n on rl.nave_actual = n.nave_id ' + Char(13)
	Set @Msg = @Msg + '		left outer join nave n2 on p.nave_id = n2.nave_id, ' + Char(13)
	Set @Msg = @Msg + '		det_documento dt, ' + Char(13)
	Set @Msg = @Msg + '		det_documento_transaccion ddt , ' + Char(13)
	Set @Msg = @Msg + '		categoria_logica cl ' + Char(13)
	Set @Msg = @Msg + 'where rl.doc_trans_id = ddt.doc_trans_id ' + Char(13)
	Set @Msg = @Msg + '		and rl.nro_linea_trans =  ddt.nro_linea_trans ' + Char(13)
	Set @Msg = @Msg + '		and ddt.documento_id = dt.documento_id ' + Char(13)
	Set @Msg = @Msg + '		and ddt.nro_linea_doc =  dt.nro_linea ' + Char(13)
	Set @Msg = @Msg + '		and rl.disponible = 1 ' + Char(13)
	Set @Msg = @Msg + '		and cl.cliente_id = rl.cliente_id ' + Char(13)
	Set @Msg = @Msg + '		and cl.cat_log_id = rl.cat_log_id ' + Char(13)

	Set @Msg1 = @Msg1 + '	and ((rl.posicion_actual is not null and p.pos_lockeada = ' + Char(39) + '0' + Char(39) + ') or (rl.nave_actual is not null)) ' + Char(13)
   
	If (@P_Opcion = 2 or @P_Opcion = 3)
		Begin
			Set @Msg1 = @Msg1 + '	and ((rl.posicion_actual is not null) or (rl.nave_actual is not null)) ' + Char(13)
			Set @Msg1 = @Msg1 + '	and ((rl.nave_actual is not null) or (rl.posicion_actual is not null)) ' + Char(13)
		End
	Else
		Begin
			If @P_Opcion = 4
				Begin
					Set @Msg1 = @Msg1 + '	and ((rl.posicion_actual is not null and n2.disp_transf = ' + Char(39) + '1' + Char(39) + ') or (rl.nave_actual is not null)) ' + Char(13)
					Set @Msg1 = @Msg1 + '	and ((rl.nave_actual is not null and n.disp_transf = ' + Char(39) + '1' + Char(39) + ') or (rl.posicion_actual is not null)) ' + Char(13)
					Set @Msg1 = @Msg1 + '	and cl.disp_transf = ' + Char(39) + '1' + Char(39) + Char(13)
				End 
       		Else
				Begin
					If @P_Opcion = 5 
						Set @Msg1 = @Msg1 + '	and ((rl.posicion_actual is not null and n2.disp_egreso = ' + Char(39) + '1' + Char(39) + ') or (rl.nave_actual is not null)) ' + Char(13)
						Set @Msg1 = @Msg1 + '	and ((rl.nave_actual is not null  and n.disp_egreso= ' + Char(39) + '1' + Char(39) + ') or (rl.posicion_actual is not null)) ' + Char(13)
						Set @Msg1 = @Msg1 + '	and cl.disp_egreso = ' + Char(39) + '1' + Char(39) + Char(13)

				End 
		End

	Set @Msg1 = @Msg1 + '	and not exists ' + Char(13)
	Set @Msg1 = @Msg1 + '	( ' + Char(13)
	Set @Msg1 = @Msg1 + '		select posicion_id ' + Char(13)
	Set @Msg1 = @Msg1 + '		from rl_posicion_prohibida_cliente rlppc ' + Char(13)
	Set @Msg1 = @Msg1 + '		where rlppc.posicion_id = rl.posicion_actual ' + Char(13)
	Set @Msg1 = @Msg1 + '		and rlppc.cliente_id = dt.cliente_id ' + Char(13)
	Set @Msg1 = @Msg1 + '	) ' + Char(13)
    
	If (@P_Opcion = 1 or @P_Opcion = 2 or @P_Opcion = 3 or @P_Opcion = 4 or @P_Opcion = 5)
		Begin
			Set @Msg1 = @Msg1 + 'order by rl.cantidad ' + Char(13)
		End 

	Set @StrSql =N'set @pcur=cursor for '+ @Msg + @Aux_Where + @Aux_Where1 + @Msg1 + '; open @pcur'

	Set @ParmDefinition =  N'@pcur cursor output '

	Execute Sp_ExecuteSql @StrSql, @ParmDefinition,
    	                  @Pcur=@Pcur Output
	
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER      Procedure [dbo].[Funciones_Estacion_Api#GrabarDocTrEgreso]
	-------------------------------------------------
	--PROCEDURE Funciones_Estacion_Api#Obtener_Rl_Id_Documento
	-------------------------------------------------	
	@P_Doc_Trans_Id_Egr 		numeric (20,0), 
	@P_Nro_Linea_Trans_Egr		numeric (10,0), 
	@P_Cliente_Id 				varchar (15), 
	@P_Producto_Id 				varchar (30), 
	@P_Fecha_Vto 				datetime, 
	@P_Nro_Serie 				varchar (50), 
	@P_Nro_Partida 				varchar (50), 
	@P_Nro_Despacho 			varchar (50), 
	@P_Nro_Bulto 				varchar (50), 
	@P_Nro_Lote 				varchar (50), 
	@P_Cantidad 				numeric (20,5), 
	@P_Nave_Origen 				numeric (20,0), 
	@P_Posicion_Origen 			numeric (20,0), 
	@P_Nave_Destino 			numeric (20,0), 
	@P_Posicion_Destino 		numeric (20,0), 
	@P_Cat_Log_Id 				varchar (50), 
	@P_Est_Merc_Id 				varchar (15), 
	@P_Prop1 					varchar (100), 
	@P_Prop2 					varchar (100), 
	@P_Prop3 					varchar (100), 
	@P_Peso 					numeric(20,5), 
	@P_Volumen 					numeric(20,5), 
	@P_Unidad_Id 				varchar (5), 
	@P_Unidad_Peso 				varchar (5), 
	@P_Unidad_Volumen 			varchar (5), 
	@P_Moneda_Id 				varchar (20), 
	@P_Costo 					numeric(10,3)		
As
Begin
	-------------------------------------------------
	--GENERICAS
	-------------------------------------------------
	Declare @Msg 				nvarchar(4000) 
	Declare @Aux_Where 			nvarchar (4000) 
	Declare @Xsql 				nvarchar(4000)
	Declare @Cant_A_Ubicar 		numeric (20,5)
	Declare @Saldo_Ubic 		numeric (20,5)
	Declare @Total_A_Ubicar 	numeric (20,5)
	Declare @Cant_Total_Ubic 	numeric (10,0)
	Declare @P_Rl_Id 			numeric (10,0)
	Declare @New_Rl_Id 			numeric (10,0)
	-------------------------------------------------
	--CURSORES.
	-------------------------------------------------
	Declare	@Pcur 				cursor
  
	Set @Msg = ''
	Set @Aux_Where = ''
	Set @Xsql = ''

	Exec dbo.Funciones_Estacion_Api#Obtener_Rl_Id_Documento @P_Cliente_Id, @P_Producto_Id, @P_Nro_Serie, @P_Nro_Lote, @P_Fecha_Vto, @P_Nro_Partida, @P_Nro_Despacho, @P_Nro_Bulto, @P_Nave_Origen, @P_Posicion_Origen, @P_Cat_Log_Id, '5', @P_Est_Merc_Id, @P_Prop1, @P_Prop2, @P_Prop3, @P_Peso, @P_Volumen, @P_Unidad_Id, @P_Unidad_Peso, @P_Unidad_Volumen, @P_Moneda_Id, @P_Costo, @Pcur OUTPUT
	
	Set @Total_A_Ubicar = @P_Cantidad

	Update historico_pos_ocupadas2 Set fecha = Getdate()

		Fetch Next From @Pcur Into @P_Rl_Id
		While @@Fetch_Status = 0
		Begin
	        
			Select @Cant_Total_Ubic = cantidad From rl_det_doc_trans_posicion Where rl_id = @P_Rl_Id
	        	        
			IF (@Cant_Total_Ubic <= @Total_A_Ubicar)
				Begin
					Set @Saldo_Ubic = 0
					Set @Cant_A_Ubicar = @Cant_Total_Ubic
				End
			Else
				Begin
					Set @Saldo_Ubic = @Cant_Total_Ubic - @Total_A_Ubicar
	            	Set @Cant_A_Ubicar = @Total_A_Ubicar
				End

			Insert Into rl_det_doc_trans_posicion(
				doc_trans_id, 
				nro_linea_trans, 
				posicion_anterior, 
				posicion_actual, 
				cantidad, 
				tipo_movimiento_id, 
				ultima_secuencia, 
				nave_anterior, 
				nave_actual, 
				documento_id, 
				nro_linea, 
				disponible, 
				doc_trans_id_egr, 
				nro_linea_trans_egr, 
				cliente_id, 
				cat_log_id, 
				cat_log_id_final, 
				est_merc_id) 
			(Select doc_trans_id, 
				nro_linea_trans, 
				posicion_actual, 
				@p_posicion_destino, 
				@cant_a_ubicar, 
				null,
				null, 
				nave_actual, 
				@p_nave_destino, 
				null, 
				null, 
				0, 
				@p_doc_trans_id_egr, 	
				@p_nro_linea_trans_egr, 
				@p_cliente_id,
				(	Select 	cat_log_id 
					From 	categoria_logica cl 
					Where 	cl.cliente_id = @P_Cliente_Id AND cl.categ_stock_id= 'TRAN_EGR'
				), 
				cat_log_id,EST_MERC_ID From rl_det_doc_trans_posicion Where rl_id = @P_RL_ID)
	
	        Select @New_Rl_Id = Scope_Identity()
	        
	        IF @Saldo_Ubic = 0
				Begin
					Delete rl_det_doc_trans_posicion Where rl_id = @P_Rl_Id
	            End
			Else
				Begin
					Update rl_det_doc_trans_posicion 
					Set cantidad = @Saldo_Ubic
					Where rl_id = @P_Rl_Id
				End
	
			Set @Total_A_Ubicar = @Total_A_Ubicar - @Cant_A_Ubicar
	        IF @Total_A_Ubicar = 0 
				Begin
					Break
				End 

			Fetch Next From @Pcur Into @P_Rl_Id
		End
	
	Close @Pcur
	Deallocate @Pcur


End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER       Procedure [dbo].[Funciones_Frontera_Api#VerificaExistencias]
@xCliente_id		varchar(15)		Output,
@xProducto_id		varchar(30)		Output,
@xCantidad			numeric(20,0)	Output
As
Begin
	--------------------------------------------------------------------------------------------
	--Declaraciones.
	--------------------------------------------------------------------------------------------
	Declare @vRegistros			as int
	Declare @xSQL				as nvarchar(4000)
	
	--------------------------------------------------------------------------------------------
	-- Cursor @Pc1	
	--------------------------------------------------------------------------------------------	
	Declare @Pc1					Cursor
	--Variables para elCursor.
	Declare @Cliente_id			as Varchar(15)
	Declare @Producto_id		as Varchar(30)
	Declare @Cantidad			as Float
	Declare @Nro_Serie			as Varchar(50)
	Declare @Nro_lote			as varchar(50)
	Declare @Nro_Partida		as Varchar(50)
	Declare @Nro_Despacho		as Varchar(50)
	Declare @Nro_Bulto			as Varchar(50)
	Declare @Fecha_Vto			as Datetime
	Declare @Peso				as Float
	Declare @Volumen			as Float
	Declare @Tie_in				as Char(1)
	Declare @Cat_Log_Id_Final	as Varchar(50)
	Declare @Prop1				as Varchar(100)
	Declare @Prop2				as Varchar(100)
	Declare @Prop3				as Varchar(100)
	Declare @Unidad_id			as Varchar(20)
	Declare @Est_Merc_id		as Varchar(15)
	--------------------------------------------------------------------------------------------	
	-- Cursor @pcur
	--------------------------------------------------------------------------------------------
	Declare @Pcur				Cursor
	Declare @ClienteIDp			as Varchar(15)
	Declare @ProductoIDp		as Varchar(30)
	Declare @cantidadp			as Float
	Declare @Nro_Seriep 		as Varchar(50)
	Declare @Nro_Lotep			as varchar(50)
	Declare @Fecha_Vtop		as DateTime
	Declare @Nro_Despachop	as Varchar(50)
	Declare @Nro_Bultop			as Varchar(50)
	Declare @Nro_Partidap		as Varchar(50)
	Declare @Pesop				as Float
	Declare @Volumenp			as Float
	Declare @tie_inp				as Char(1)
	Declare @cat_log_idp		as Varchar(50)
	Declare @prop1p 			as Varchar(100)
	Declare @prop2p 			as Varchar(100)
	Declare @prop3p 			as Varchar(100)
	Declare @unidad_idp			as Varchar(20)
	Declare @vNULL1			as varchar(1)
	Declare @vNULL2			as varchar(1)
	Declare @est_merc_idp		as varchar(15)
	Declare @vNULL3			as varchar(1)
	Declare @vNULL4			as varchar(1)
	--------------------------------------------------------------------------------------------

	truncate table #temp_existencia
	
	truncate table #temp_existencia_doc
	
	truncate table #temp_rl_existencia_doc

	-----------------------------------------------------------------------------------------------------------------------------
	-- INSERT EN LA TABLA DE TEMP_EXISTENCIA, DE TODOS LOS ITEMS QUE EXISTAN DEL CLIENTE Y PRODUCTO.
	-----------------------------------------------------------------------------------------------------------------------------
	INSERT INTO #temp_existencia
	SELECT 	 t2.ClienteID
				 ,t2.ProductoID
				 ,Sum(IsNull(t2.cantidad, 0)) AS Cantidad
				 ,t2.Nro_Serie, t2.Nro_Lote
				 ,t2.Fecha_Vencimiento
				 ,t2.Nro_Despacho, t2.Nro_bulto, t2.Nro_Partida
				 ,NULL, NULL, NULL
				 ,NULL, NULL, NULL, NULL
				 ,NULL, NULL, NULL, NULL
				 ,t2.CategLogID, t2.prop1, t2.prop2, t2.prop3
				 ,t2.unidad_id, NULL, NULL, t2.est_merc_id, NULL, NULL
		 From	 (
				SELECT 	  rl.cat_log_id as CategLogID						 ,dd.cliente_id AS ClienteID
						 ,dd.producto_id AS ProductoID					 ,Sum(IsNull(rl.cantidad, 0)) AS Cantidad
						 ,dd.nro_serie									 ,dd.Nro_lote
						 ,dd.Fecha_vencimiento							 ,dd.Nro_despacho
						 ,dd.Nro_Bulto									 ,dd.Nro_Partida
						 ,dd.Peso AS Peso, dd.Volumen AS Volumen		 ,IsNull(n.nave_cod,n2.nave_cod) AS Storage
						 ,IsNull(rl.nave_actual,p.nave_id) as NaveID		 ,dd.tie_in
						 ,caln.calle_cod AS CalleCod						 ,caln.calle_id AS CalleID
						 ,coln.columna_cod AS ColumnaCod				 ,coln.columna_id AS ColumnaID
						 ,nn.nivel_cod AS NivelCod						 ,nn.nivel_id AS NivelID
						 ,dd.prop1										 ,dd.prop2
						 ,dd.prop3										 ,dd.unidad_id
						 ,rl.est_merc_id
				 FROM  	 rl_det_doc_trans_posicion rl						 LEFT OUTER JOIN  nave n
						 ON  rl.nave_actual  = n.nave_id					 LEFT OUTER JOIN  posicion p
						 ON  rl.posicion_actual  = p.posicion_id				 LEFT OUTER JOIN  ESTADO_MERCADERIA_RL EMRL
						 ON  EMRL.CLIENTE_ID  = RL.CLIENTE_ID			 AND    EMRL.EST_MERC_ID  = RL.EST_MERC_ID
						 LEFT OUTER JOIN  nave n2						 ON  p.nave_id  = n2.nave_id
						 LEFT OUTER JOIN  calle_nave caln					 ON  p.calle_id  = caln.calle_id
						 LEFT OUTER JOIN  columna_nave coln				 ON  p.columna_id  = coln.columna_id
						 LEFT OUTER JOIN  nivel_nave nn					 ON  p.nivel_id  = nn.nivel_id ,
						 det_documento_transaccion ddt,					 det_documento dd,
						 categoria_logica cl
				 Where 	RL.doc_trans_id = ddt.doc_trans_id				 AND rl.nro_linea_trans = ddt.nro_linea_trans
						 AND ddt.documento_id = dd.documento_id		 AND ddt.nro_linea_doc = dd.nro_linea
						 AND rl.cat_log_id = cl.cat_log_id					 AND rl.cliente_id = cl.cliente_id
						 AND rl.disponible = '1'							 AND cl.disp_egreso = '1'
						 and IsNull(n.disp_egreso, IsNull(n2.disp_egreso, '1')) = '1'
						 AND IsNull(p.pos_lockeada, '0') = '0'
						 AND IsNull(n.disp_egreso, IsNull(n2.disp_egreso, '1')) = '1'
						 AND (
								 SELECT (CASE WHEN (Count(posicion_id)) > 0 THEN 1 ELSE 0 END) AS VALOR
								 From rl_posicion_prohibida_cliente
								 Where Posicion_ID = IsNull(p.nave_id, 0)
								 AND cliente_id = dd.cliente_id
						 ) = 0
						 AND IsNull(EMRL.DISP_EGRESO, '1') = '1'
				 GROUP BY 
						 rl.cat_log_id, dd.cliente_id, dd.producto_id, dd.nro_serie, dd.Nro_lote, dd.Fecha_vencimiento, dd.Nro_Despacho
						 ,dd.nro_bulto, dd.Nro_Partida, dd.Peso, dd.Volumen, rl.nave_actual, p.nave_id, n.nave_cod, n2.nave_cod, caln.calle_cod
						 ,caln.calle_id, coln.columna_cod, coln.columna_id, nn.nivel_cod, nn.nivel_id, dd.tie_in, dd.prop1, dd.prop2, dd.prop3
						 ,dd.unidad_id
						 ,RL.est_merc_id
				 ) T2
	Where 	1 <> 0	AND t2.ClienteID =@xCliente_id AND t2.ProductoID =@xProducto_id
	GROUP BY 
			t2.ClienteID,t2.ProductoID,t2.Nro_Serie,t2.Nro_Lote,t2.Fecha_Vencimiento,t2.Nro_Despacho, t2.Nro_bulto, t2.Nro_Partida
			,t2.CategLogID,t2.prop1,t2.prop2,t2.prop3,t2.unidad_id	,t2.est_merc_id

	Select @vRegistros=Count(*) from #Temp_Existencia

	If @vRegistros>0
	Begin

		Set @Pc1= Cursor For
		SELECT  t2.* 
		FROM (
				SELECT 	dd.cliente_id ClienteID 	 ,dd.producto_id ProductoID  	,sum(IsNull(dd.cantidad,0)) AS cantidad 
						 ,dd.nro_serie 			 ,dd.nro_lote 				 ,dd.nro_partida 
						 ,dd.nro_despacho 		 ,dd.nro_bulto 				 ,dd.Fecha_vencimiento 
						 ,dd.peso,dd.volumen 	 ,dd.tie_in 					 ,dd.cat_log_id_final 
						 ,dd.prop1 				 ,dd.prop2 					 ,dd.prop3 
						 ,dd.unidad_id 			 ,dd.est_merc_id 
				FROM 	documento d, det_documento dd , categoria_logica cl 
				WHERE 	d.documento_id = dd.documento_id 		AND dd.cat_log_id = cl.cat_log_id 
						AND d.status = 'D20'						AND cl.categ_stock_id = 'TRAN_EGR'
						AND dd.cliente_id = cl.cliente_id 			AND dd.cliente_id=@xCliente_id
						AND dd.Producto_id=@xProducto_id
				GROUP BY
						  dd.cliente_id					, dd.producto_id 						 ,dd.nro_serie 
						 ,dd.nro_lote 					 ,dd.nro_partida 						 ,dd.nro_despacho 
						 ,dd.nro_bulto 					 ,dd.Fecha_vencimiento 				 ,dd.peso,dd.volumen 
						 ,dd.tie_in 						 ,dd.cat_log_id_final 					 ,dd.prop1 
						 ,dd.prop2 						 ,dd.prop3 							 ,dd.unidad_id 
						 ,dd.est_merc_id 
				UNION ALL 
				SELECT 	dd.cliente_id ClienteID	,dd.producto_id ProductoID	,sum(IsNull(dd.cantidad,0)) AS cantidad 
						,dd.nro_serie 			,dd.nro_lote 					,dd.nro_partida 
						,dd.nro_despacho 		,dd.nro_bulto 				,dd.Fecha_vencimiento 
						,dd.peso,dd.volumen 		,dd.tie_in 					,dd.cat_log_id_final 
						,dd.prop1 				,dd.prop2 					,dd.prop3 
						,dd.unidad_id 			,dd.est_merc_id 
				FROM 	det_documento dd , categoria_logica cl, det_documento_transaccion ddt 	,documento_transaccion dt 
				WHERE 
						ddt.cliente_id = cl.cliente_id 		AND ddt.cat_log_id = cl.cat_log_id 	AND cl.categ_stock_id = 'TRAN_EGR'
						AND dd.cliente_id = cl.cliente_id 	AND ddt.documento_id = dd.documento_id AND ddt.nro_linea_doc = dd.nro_linea 
						AND dt.doc_trans_id = ddt.doc_trans_id 	AND dt.status = 'T10'
						AND not exists (SELECT 	rl_id 
										FROM 	rl_det_doc_trans_posicion rl 
										WHERE 	rl.doc_trans_id_egr = ddt.doc_trans_id 
												AND rl.nro_linea_trans_egr = ddt.nro_linea_trans
										) 
						AND dd.cliente_id=@xCliente_id	AND dd.Producto_id=@xProducto_id
				GROUP BY 
						dd.cliente_id ,dd.producto_id 				,dd.nro_serie 					,dd.nro_lote 
						,dd.nro_partida 							,dd.nro_despacho 				,dd.nro_bulto 
						,dd.Fecha_vencimiento 					,dd.peso,dd.volumen 				,dd.tie_in 
						,dd.cat_log_id_final 						,dd.prop1 						,dd.prop2 
						,dd.prop3 								,dd.unidad_id 					,dd.est_merc_id 
				
				) T2 
		where 1 <> 0 

		Open @Pc1

		Fetch Next from @Pc1 into  	 @Cliente_id						,@Producto_id					,@Cantidad						,@Nro_Serie
									,@Nro_lote						,@Nro_Partida					,@Nro_Despacho					,@Nro_Bulto
									,@Fecha_Vto					,@Peso							,@Volumen						,@Tie_in
									,@Cat_Log_Id_Final				,@Prop1							,@Prop2							,@Prop3
									,@Unidad_id						,@Est_Merc_id
			
		While @@Fetch_Status= 0
		Begin

			Set @xSQL=N' UPDATE #temp_existencia SET cantidad = cantidad - ' + Cast(@Cantidad as varchar(20)) + Char(13)
			Set @xSQL = @xSQL + N' WHERE 1 = 1 ' + Char(13)
			if @Cliente_id is not null		
			Begin	
				Set @xSQL = @xSQL + N'	AND CLIENTEID = ' + Char(39) + @Cliente_id + Char(39) + Char(13)		
			End
			Else	
			Begin	
				Set @xSQL = @xSQL + N' AND CLIENTEID IS NULL' + Char(13)		
			End
			If @Producto_id is not null
			Begin
				Set @xSQL = @xSQL + N' AND PRODUCTOID = '	+ Char(39) + @Producto_id + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND PRODUCTOID IS NULL' + Char(13)
			End	
			if @nro_serie is not null
			Begin
				Set @xSQL = @xSQL + N' AND NRO_SERIE = ' + Char(39) + @nro_serie + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND NRO_SERIE IS NULL' + Char(13)
			End
			if @Nro_lote is not null
			Begin
				Set @xSQL = @xSQL + N' AND NRO_LOTE = ' + Char(39) + @Nro_Lote + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND NRO_LOTE IS NULL' + Char(13)
			End
			if @Nro_Partida is not null
			Begin
				Set @xSQL = @xSQL + N' AND NRO_PARTIDA = ' + Char(39) + @Nro_Partida + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND NRO_PARTIDA IS NULL' + Char(13)
			End
			if @Nro_despacho is not null
			Begin
				Set @xSQL = @xSQL + N' AND NRO_DESPACHO = ' + Char(39) + @Nro_despacho + Char(39)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND NRO_DESPACHO IS NULL' + Char(13)
			End
			if @Nro_bulto is not null
			Begin
				Set @xSQL = @xSQL + N' AND NRO_BULTO = ' + Char(39) + @Nro_Bulto + Char(39)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND NRO_BULTO IS NULL' + Char(13)
			End
			if @Fecha_Vto is not null
			Begin
				Set @xSQL = @xSQL + N' AND CONVERT(DATETIME, CONVERT(VARCHAR, FECHA_VENCIMIENTO, 112)) = CONVERT(DATETIME, CONVERT(VARCHAR, ' + Char(39) + Cast(@Fecha_Vto as Varchar(30)) + Char(39)  + ',112)) ' + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND FECHA_VENCIMIENTO IS NULL' + Char(13)
			End
			if @Cat_Log_Id_Final is not null
			Begin
				Set @xSQL = @xSQL + N' AND CATEGLOGID = ' + Char(39) + @Cat_Log_Id_Final + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND CATEGLOGID IS NULL' + Char(13)
			End
			if @prop1 is not null
			Begin
				Set @xSQL = @xSQL + N' AND PROP1 = ' + Char(39) + @Prop1 + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND PROP1 IS NULL' + Char(13)
			End
			if @prop2 is not null
			Begin
				Set @xSQL = @xSQL + N' AND PROP2 =' + Char(39) + @Prop2 + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND PROP2 IS NULL'
			End
			if @prop3 is not null
			Begin
				Set @xSQL = @xSQL + N' AND PROP3 = ' + Char(39) + @Prop3 + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND PROP3 IS NULL' + Char(13)
			End
			if @Unidad_id is not null
			Begin
				Set @xSQL = @xSQL + N' AND UNIDAD_ID = ' + Char(39) + @Unidad_id + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND UNIDAD_ID IS NULL' + Char(13)
			End
			if @est_merc_id is not null
			Begin
				Set @xSQL = @xSQL + N' AND EST_MERC_ID = ' + Char(39) + @est_merc_id + Char(39) + Char(13)
			End
			Else
			Begin
				Set @xSQL = @xSQL + N' AND EST_MERC_ID IS NULL' + Char(13)
			End

			EXECUTE SP_EXECUTESQL @xSQL

			DELETE FROM #temp_existencia WHERE cantidad = 0

			Fetch Next from @Pc1 into  	 @Cliente_id						,@Producto_id				,@Cantidad						,@Nro_Serie
										,@Nro_lote						,@Nro_Partida				,@Nro_Despacho					,@Nro_Bulto
										,@Fecha_Vto					,@Peso						,@Volumen						,@Tie_in
										,@Cat_Log_Id_Final				,@Prop1						,@Prop2							,@Prop3
										,@Unidad_id						,@Est_Merc_id
		End --Fin del While.
		Close @Pc1
		Deallocate @Pc1
	End -- Fin @vRegistros

		-----------------------------------------------------------------------------------------------------------
		-- LEVANTA LOS REGISTROS DE TEMP_EXISTENCIA.
		-----------------------------------------------------------------------------------------------------------
	Insert into #temp_existencia_doc
		SELECT  ClienteID 				,ProductoID 			      	,sum(cantidad)			     
				,Nro_Serie 			      	,Nro_Lote 				,Fecha_Vencimiento 
			      	,Nro_Despacho 			,Nro_Bulto 			      	,Nro_Partida				      
				,Peso 				      	,Volumen 				,null
				,null						,null						,null
			      	,categlogid				,prop1 				      	,prop2 					      
				,prop3 				      	,unidad_id				,NULL 
			      	,NULL 					,est_merc_id 		      	,NULL 					      
				,NULL 					,NULL
		 From 	#temp_existencia
		 GROUP BY 
				ClienteID					,ProductoID			,Nro_Serie					,Nro_Lote
				,Fecha_Vencimiento			,Nro_Despacho		,Nro_Bulto					,Nro_Partida
				,Peso						,Volumen			,tie_in						,categlogid
				,prop1						,prop2				,prop3						,unidad_id
				,est_merc_id

		Select 	@xCantidad= isnull(SUM(cantidad),0) 
		From 	#temp_existencia_doc
		Group by	Clienteid, productoid
	
End	--Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  procedure [dbo].[Funciones_Saldo_Api#Insertar_Lista_ProdCatLogING]
@Cliente_Id 	as varchar(15),
@Producto_id	as Varchar(30)
As
Begin

	truncate table #temp_saldos_catlog

	INSERT INTO #TEMP_SALDOS_CATLOG (CLIENTE_ID, PRODUCTO_ID, CAT_LOG_ID, CATEG_STOCK_ID, CANTIDAD, EST_MERC_ID)
		(	SELECT DISTINCT 	
					DD.CLIENTE_ID,	DD.PRODUCTO_ID,	dd.CAT_LOG_ID_FINAL AS CATEGORIA_LOGICA,
					CL.CATEG_STOCK_ID,	SUM(RL.CANTIDAD),	RL.EST_MERC_ID
			FROM 	RL_DET_DOC_TRANS_POSICION RL,
					DET_DOCUMENTO_TRANSACCION DDT,
					DET_DOCUMENTO DD,
					CATEGORIA_LOGICA CL
			WHERE 	RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID
					AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS
					AND DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID
					AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC
					AND RL.CLIENTE_ID = CL.CLIENTE_ID
					AND RL.CAT_LOG_ID = CL.CAT_LOG_ID
					AND CL.CATEG_STOCK_ID <> 'TRAN_EGR'
					AND DD.CLIENTE_ID = @Cliente_Id
					AND DD.PRODUCTO_ID = @Producto_id
			GROUP BY 	dd.cliente_id,
						dd.producto_id,
						DD.cat_log_id_final,
						cl.categ_stock_id,
						rl.est_merc_id
		)

	INSERT INTO #TEMP_SALDOS_CATLOG(CLIENTE_ID,PRODUCTO_ID,CAT_LOG_ID,CATEG_STOCK_ID,CANTIDAD,EST_MERC_ID)
	(	SELECT 	DISTINCT 
				DD.CLIENTE_ID,
				DD.PRODUCTO_ID,
				DD.CAT_LOG_ID_FINAL CATEGORIA_LOGICA,
				CL.CATEG_STOCK_ID,
				SUM(RL.CANTIDAD),
				DD.EST_MERC_ID
		FROM 	RL_DET_DOC_TRANS_POSICION RL,
				DET_DOCUMENTO DD,
				CATEGORIA_LOGICA CL
		WHERE 	RL.DOCUMENTO_ID = DD.DOCUMENTO_ID
				AND RL.NRO_LINEA = DD.NRO_LINEA
				AND RL.CLIENTE_ID = CL.CLIENTE_ID
				AND RL.CAT_LOG_ID = CL.CAT_LOG_ID
				AND DD.CLIENTE_ID = @Cliente_id
				AND DD.PRODUCTO_ID = @Producto_id
				AND NOT EXISTS (
								SELECT 	*
								FROM	#TEMP_SALDOS_CATLOG T
								WHERE 	T.CLIENTE_ID = DD.CLIENTE_ID
										AND T.PRODUCTO_ID = DD.PRODUCTO_ID
										AND T.CAT_LOG_ID = DD.CAT_LOG_ID
										AND T.CATEG_STOCK_ID = CL.CATEG_STOCK_ID
										AND T.EST_MERC_ID = DD.EST_MERC_ID
								)
		GROUP BY DD.CLIENTE_ID,
				DD.PRODUCTO_ID,
				DD.CAT_LOG_ID_FINAL,
				CL.CATEG_STOCK_ID,
				DD.EST_MERC_ID
	)

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  procedure [dbo].[Funciones_Saldo_api#Insertar_Lista_ProdCatLogEGR]
@Cliente_id		as varchar(15),
@Producto_id	as Varchar(30)
As
Begin
	
	Declare @StrSql1	as nvarchar(3000)
	Declare @StrSql2	as nvarchar(1000)
	Declare @StrSql3	as nvarchar(1000)
	Declare @StrSql3b	as nvarchar(1000)
	Declare @StrSql4	as nvarchar(1000)
	Declare @strSQLW	as nvarchar(500)
	Declare @xSQL		as nvarchar(4000)

	
	
	Set @StrSql1 = 'INSERT INTO #TEMP_SALDOS_CATLOG (cliente_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      producto_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      cat_log_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      categ_stock_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      cantidad,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '                      est_merc_id)'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + ' (SELECT cliente_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '         producto_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '         cat_log_id_final,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '         categ_stock_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '         Sum(cantidad) AS CANTIDAD,+ CHAR(13)'
	Set @StrSql1 = @StrSql1 + '         est_merc_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '    FROM'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '  (SELECT dd.cliente_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          Sum(dd.cantidad) AS cantidad,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          dd.est_merc_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '     FROM documento d,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          det_documento dd,'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '          categoria_logica cl'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '    WHERE dd.documento_id = d.documento_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '      AND dd.cliente_id = cl.cliente_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '      AND dd.cat_log_id = cl.cat_log_id'+ CHAR(13)
	Set @StrSql1 = @StrSql1 + '      AND d.status = ' + CHAR(39) + 'D20' + CHAR(39) + CHAR(13)
	Set @StrSql1 = @StrSql1 + '      AND cl.categ_stock_id = ' + CHAR(39) + 'TRAN_EGR' + CHAR(39) + CHAR(13)
	
	     	 Set   @StrSql2 = ' GROUP BY dd.cliente_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.est_merc_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + ' UNION ALL'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '   SELECT dd.cliente_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          Sum(dd.cantidad),'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          dd.est_merc_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '     FROM documento d,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          det_documento dd,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          categoria_logica cl,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          det_documento_transaccion ddt,'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '          documento_Transaccion dt'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '    WHERE ddt.documento_id = dd.documento_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND ddt.nro_linea_doc = dd.nro_linea'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND dd.documento_id = d.documento_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND dd.cliente_id = cl.cliente_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND dd.cat_log_id = cl.cat_log_id'+ CHAR(13)
	Set @StrSql2 = @StrSql2 + '      AND dt.doc_trans_id = ddt.doc_trans_id'+ CHAR(13)
	
	      	   Set @StrSql3 = '      AND d.status = ' + CHAR(39) + 'D30' + CHAR(39) + CHAR(13)
	Set @StrSql3 = @StrSql3 + '      AND dt.status = ' + CHAR(39) + 'T10' + CHAR(39) + CHAR(13)
	Set @StrSql3 = @StrSql3 + '      AND cl.categ_stock_id = ' + CHAR(39) + 'TRAN_EGR' + CHAR(39) + CHAR(13)
	Set @StrSql3 = @StrSql3 + '      and not EXISTS (SELECT rl_id' + CHAR(13)
	Set @StrSql3 = @StrSql3 + '                        FROM rl_det_doc_trans_posicion rl'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                       WHERE rl.doc_trans_id_egr = ddt.doc_trans_id'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                         AND rl.nro_linea_trans_egr = ddt.nro_linea_trans)'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                    GROUP BY dd.cliente_id,'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                             dd.producto_id,'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                             dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                             cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql3 = @StrSql3 + '                             dd.est_merc_id'+ CHAR(13)
	
	            Set @StrSql3b = ' UNION ALL' + CHAR(13)
	Set @StrSql3b = @StrSql3b + '    SELECT dd.cliente_id,' + CHAR(13)
	Set @StrSql3b = @StrSql3b + '           dd.producto_id,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           Sum(RL.cantidad),'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           rl.est_merc_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      FROM documento d,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           det_documento dd,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           categoria_logica cl,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           det_documento_transaccion ddt,'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '           rl_det_doc_trans_posicion rl'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '    WHERE rl.cliente_id = cl.cliente_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND rl.cat_log_id = cl.cat_log_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND rl.doc_trans_id_egr = ddt.doc_trans_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND rl.nro_linea_trans_egr = ddt.nro_linea_trans'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND ddt.documento_id = dd.documento_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND ddt.nro_linea_doc = dd.nro_linea'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND dd.documento_id = d.documento_id'+ CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND d.status = ' + CHAR(39) + 'D30' + CHAR(39) + CHAR(13)
	Set @StrSql3b = @StrSql3b + '      AND cl.categ_stock_id = ' + CHAR(39) + 'TRAN_EGR' + CHAR(39) + CHAR(13)
	
	           Set @StrSql4 = ' GROUP BY dd.cliente_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          dd.producto_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          dd.cat_log_id_final,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          cl.categ_stock_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          rl.est_merc_id'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + ' ) T1'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + ' GROUP BY cliente_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          producto_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          cat_log_id_final,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          categ_stock_id,'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + '          est_merc_id'+ CHAR(13)
	Set @StrSql4 = @StrSql4 + ' )'+ CHAR(13)
	
	If @Cliente_id IS NOT NULL 
	Begin
		Set @strSQLW = @strSQLW + '      AND dd.cliente_id = ltrim(rtrim(Upper(' + CHAR(39) + @CLiente_id + CHAR(39) +')))' + CHAR(13)
	End
	/*
	Else
	Begin
		'ORIGINAL @StrSql1 = @StrSql1 + '      AND funciones_generales_api.ClienteEnUsuario(dd.cliente_id, ' + fg.GetUsuarioActivo + ') = 1'
		@StrSql1 = @StrSql1 + '     AND '
		@StrSql1 = @StrSql1 + '        (SELECT CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END'
		@StrSql1 = @StrSql1 + '         FROM   rl_sys_cliente_usuario'
		@StrSql1 = @StrSql1 + '         WHERE  cliente_id = dd.cliente_id'
		@StrSql1 = @StrSql1 + '         And    usuario_id = '' + UCase(Trim(fg.GetUsuarioActivo)) + '') = 1'
		
		'ORIGINAL @StrSql2 = @StrSql2 + '      AND funciones_generales_api.ClienteEnUsuario(dd.cliente_id, ' + fg.GetUsuarioActivo + ') = 1'
		@StrSql2 = @StrSql2 + '     AND '
		@StrSql2 = @StrSql2 + '        (SELECT CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END'
		@StrSql2 = @StrSql2 + '         FROM   rl_sys_cliente_usuario'
		@StrSql2 = @StrSql2 + '         WHERE  cliente_id = dd.cliente_id'
		@StrSql2 = @StrSql2 + '         And    usuario_id = '' + UCase(Trim(fg.GetUsuarioActivo)) + '') = 1'
		
		'ORIGINAL @StrSql3b = @StrSql3b + '      AND funciones_generales_api.ClienteEnUsuario(dd.cliente_id, ' + fg.GetUsuarioActivo + ') = 1'
		@strSQLW = @strSQLW + '     AND '
		@strSQLW = @strSQLW + '        (SELECT CASE WHEN (Count(cliente_id)) > 0 THEN 1 ELSE 0 END'
		@strSQLW = @strSQLW + '         FROM   rl_sys_cliente_usuario'
		@strSQLW = @strSQLW + '         WHERE  cliente_id = dd.cliente_id'
		@strSQLW = @strSQLW + '         And    usuario_id = '' + UCase(Trim(fg.GetUsuarioActivo)) + '') = '1''
		
		P_CLIENTE_ID = '1'
		@strSQLW = @strSQLW + '      AND '1' = '' + UCase(Trim(P_CLIENTE_ID)) + '''
	End*/
	
	If @Producto_id is not null
	Begin
		Set @strSQLW = @strSQLW + '      AND dd.producto_id= ltrim(rtrim(upper(' + CHAR(39) + @Producto_id + CHAR(39) +')))'
	End
	Else
	Begin
		set @Producto_id = '1'
		Set @strSQLW = @strSQLW + '      AND ' + CHAR(39) + '1' + CHAR(39) +' = ' + CHAR(39) + @Producto_id + CHAR(39)
	End 
	set @xSQL=@StrSql1 + @strSQLW + @StrSql2 + @strSQLW + @StrSql3 + @StrSql3b + @strSQLW + @StrSql4

	Exec sp_Executesql @xSQL

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  procedure [dbo].[Funciones_Saldo_Api#Actualizar_Saldos_CatLog1]
As
Begin

	Declare @FSAASC 	Cursor
	Declare @Cliente	as varchar(15)
	Declare @Producto	as varchar(30)
	Declare @Saldo		as Float
	Declare @CatLogID	as varchar(50)
	Declare @EstMercId	as varchar(15)

	Set @FSAASC= Cursor for
		select 	t2.cliente_id,
				t2.producto_id,
				(t2.cantidad - isnull(t1.cantidad, 0)) as saldo,
				t2.cat_log_id,
				t2.est_merc_id
		from 	#temp_saldos_catlog t1,
				#temp_saldos_catlog t2
		where 	t1.categ_stock_id = 'TRAN_EGR'
				and t2.categ_stock_id = 'STOCK'
				and t2.cat_log_id = t1.cat_log_id
				and t2.est_merc_id = t1.est_merc_id
				and t2.cliente_id = t1.cliente_id
				and t2.producto_id = t1.producto_id

	Open @FSAASC
	Fetch Next From @FSAASC into @Cliente,@Producto,@Saldo,@CatLogId,@EstMercId
	While @@Fetch_Status=0
	Begin

		UPDATE 	#TEMP_SALDOS_CATLOG SET cantidad = @Saldo
		WHERE 	cliente_id = @Cliente
				And producto_id =@Producto
				And categ_stock_id = 'STOCK'
				And cat_log_id = @CatLogId
				And est_merc_id =@EstMercId
		
		Fetch Next From @FSAASC into @Cliente,@Producto,@Saldo,@CatLogId,@EstMercId
	End
	Close 		@FSAASC
	Deallocate 	@FSAASC
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  procedure [dbo].[Funciones_Saldo_api#Actualizar_Saldos_CatLog2]
@Cliente_id		as varchar(15),
@Producto_id	as varchar(30)
As
Begin

	declare @Cliente	as varchar(15)
	declare @producto	as varchar(30)
	declare @Qty		as float
	declare @EstMercId	as varchar(15)
	declare @FSAASC2	Cursor

	Set @FSAASC2= Cursor For
		SELECT 	
				dd.cliente_id,					dd.producto_id,
				Sum(rl.cantidad) AS Cantidad,	rl.est_merc_id
		FROM 	det_documento_transaccion ddt,	det_documento dd,
				documento d, categoria_logica cl,rl_det_doc_trans_posicion rl
		WHERE 	dd.documento_id  = d.documento_id
				And d.status = 'D30'						And rl.cliente_id = cl.cliente_id
				And rl.cat_log_id = cl.cat_log_id			And cl.categ_stock_id = 'TRAN_EGR'
				And ddt.documento_id = dd.documento_id		And ddt.nro_linea_doc = dd.nro_linea
				And rl.doc_trans_id_egr = ddt.doc_trans_id	And rl.nro_linea_trans_egr = ddt.nro_linea_trans
				And dd.cliente_id = @Cliente_id				And dd.producto_id = @Producto_id
		GROUP BY 	dd.cliente_id,				dd.producto_id,
					cl.cat_log_id,				cl.categ_stock_id,
					rl.est_merc_id

	Open @FSAASC2
	Fetch Next From @FSAASC2 into @Cliente,@Producto,@Qty,@EstMercId
	While @@Fetch_Status=0
	Begin
		UPDATE 	#TEMP_SALDOS_CATLOG SET cantidad = cantidad + @Qty
		WHERE 	cliente_id 	= @Cliente
				And producto_id 	= @Producto
				And categ_stock_id 	= 'STOCK'
				And cat_log_id 		= 'DISPONIBLE'

		Fetch Next From @FSAASC2 into @Cliente,@Producto,@Qty,@EstMercId
	End

	Close		@FSAASC2
	Deallocate	@FSAASC2
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  Procedure [dbo].[Funciones_Saldo_Api#Generar_Saldos_CategLog]
@Cliente_id		as varchar(15),
@Producto_id	as varchar(30)
As
Begin

	Declare @ErrCod	as Int

	exec @ErrCod=Funciones_Saldo_Api#Insertar_Lista_ProdCatLogING @Cliente_id, @Producto_id
	if @ErrCod > 1
	Begin
		RaisError('Error al ejecutar Funciones_Saldo_Api#Insertar_Lista_ProdCatLogING',16,1)
		Return
	End
	Exec @ErrCod=Funciones_Saldo_Api#Insertar_Lista_ProdCatLogEGR @Cliente_id, @Producto_id
	if @ErrCod > 1
	Begin
		RaisError('Error al ejecutar Funciones_Saldo_Api#Insertar_Lista_ProdCatLogEGR',16,1)
		Return
	End
	Exec @ErrCod=Funciones_Saldo_Api#Actualizar_Saldos_CatLog1
	if @ErrCod > 1
	Begin
		RaisError('Error al ejecutar Funciones_Saldo_Api#Actualizar_Saldos_CatLog1',16,1)
		Return
	End
	Exec @ErrCod=Funciones_Saldo_Api#Actualizar_Saldos_CatLog2 @Cliente_id, @Producto_id
	if @ErrCod > 1
	Begin
		RaisError('Error al ejecutar Funciones_Saldo_Api#Actualizar_Saldos_CatLog2',16,1)
		Return
	End
    
	delete from #temp_saldos_catlog where cantidad=0

End	--fin procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER    Procedure [dbo].[Funciones_Historico_Api#Actualizar_HistSaldos_CatLog]
@documento_id 	as numeric(20,0), 
@doc_trans_id 	as numeric(20,0), 
@Rl_id 			as numeric(20,0)
As
Begin
	Declare @FHAAHC 	Cursor
	Declare @Cliente	as varchar(15)
	Declare @DocId		as numeric(20,0)
	Declare @producto	as varchar(30)
	Declare @NroLinea	as numeric(10,0)
	Declare @Status		as varchar(3)
	Declare @CatLogId	as varchar(50)
	Declare @Cantidad	as Float
	Declare @catStock	as varchar(15)
	Declare @estmercid	as varchar(15)
	Declare @Control	as int

    If @documento_id is not null
	Begin
		Set @FHAAHC= Cursor for
			SELECT 	dd.cliente_id,	dd.producto_id,
					d.documento_id, dd.nro_linea,
					d.status
			FROM 	det_documento dd,
					documento d
			WHERE 	dd.documento_id = d.documento_id
					And dd.cliente_id = d.cliente_id
					And d.documento_id = @documento_id
		open @FHAAHC
	End

    If @Doc_trans_id is not null
	Begin
		Set @FHAAHC= Cursor for
			SELECT 	dd.cliente_id,	dd.producto_id,
					d.documento_id,	dd.nro_linea,
					d.status
			FROM 	det_documento_transaccion ddt,
					det_documento dd,
					documento d
			WHERE 	dd.documento_id = ddt.documento_id
					And ddt.documento_id = d.documento_id
					And dd.nro_linea = ddt.nro_linea_doc
					and ddt.doc_trans_id = @doc_trans_id
		Open @FHAAHC
	End

    If @RL_ID is not null
	Begin
		Set @FHAAHC=Cursor For
			SELECT 	dd.cliente_id,	dd.producto_id,
					d.documento_id,	dd.nro_linea,
					d.status
			FROM 	rl_det_doc_trans_posicion rl,
					det_documento_transaccion ddt,
					det_documento dd,
					documento d
			WHERE 	dd.documento_id = ddt.documento_id
					And ddt.documento_id = d.documento_id
					And dd.nro_linea = ddt.nro_linea_doc
					And ddt.doc_trans_id = rl.doc_trans_id
					And ddt.nro_linea_trans = rl.nro_linea_trans
					And rl.rl_id = @RL_ID
		Open @FHAAHC
    End

	Fetch Next From @FHAAHC Into @Cliente,@Producto,@DocId,@NroLinea,@Status
	While @@Fetch_Status=0
	Begin
		Exec Funciones_Saldo_Api#Generar_Saldos_CategLog @Cliente,@Producto
		
		select 	@Cliente=cliente_id, @Producto=producto_id, @CatLogId=cat_log_id, 
				@cantidad=isnull(cantidad,0), @catStock=categ_stock_id, @estmercid=est_merc_id
		from 	#temp_saldos_catlog 
		where 	cantidad <> 0
	
		if @@RowCount > 0
		Begin
			Insert Into HISTORICO_SALDOS_CATLOG (
			         FECHA
					,CLIENTE_ID
					,PRODUCTO_ID
					,CAT_LOG_ID
					,CANTIDAD
					,CATEG_STOCK_ID
					,DOCUMENTO_ID
					,NRO_LINEA
					,DOC_STATUS
					,EST_MERC_ID
					)
			 Values (
					getdate()
					,@Cliente
					,@Producto
					,@CatLogId
					,@cantidad
					,@catStock
					,@DocId
					,@NroLinea
					,@Status
					,@estmercid
					)
		End
		Fetch Next From @FHAAHC Into @Cliente,@Producto,@DocId,@NroLinea,@Status
	End

	Close 		@FHAAHC
	Deallocate 	@FHAAHC
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   Procedure [dbo].[Funciones_Saldo_Api#Insertar_Lista_Productos]
@ClienteId		as varchar(15),
@ProductoId		as varchar(30)
As
Begin
	
	declare @xSQL as Nvarchar(2000)	

	Truncate Table #temp_saldos_stock

	set @xSQL=' INSERT INTO #TEMP_SALDOS_STOCK ( ' + CHAR(13)
	set @xSQL= @xSQL +'        cliente_id,' + CHAR(13)
	set @xSQL= @xSQL +'        producto_id,' + CHAR(13)
	set @xSQL= @xSQL +'        cant_tr_ing,' + CHAR(13)
	set @xSQL= @xSQL +'        cant_stock,' + CHAR(13)
	set @xSQL= @xSQL +'        cant_tr_egr)' + CHAR(13)
	set @xSQL= @xSQL +' (SELECT DISTINCT p.cliente_id As Cliente,' + CHAR(13)
	set @xSQL= @xSQL +'        p.producto_id As Producto,' + CHAR(13)
	set @xSQL= @xSQL +'        0,' + CHAR(13)
	set @xSQL= @xSQL +'        0,' + CHAR(13)
	set @xSQL= @xSQL +'        0' + CHAR(13)
	set @xSQL= @xSQL +'        FROM producto As p ' + CHAR(13)
	set @xSQL= @xSQL +' WHERE 1 <> 0 ' + CHAR(13)

	if @clienteId is not null
	Begin
	    set @xSQL= @xSQL +'        AND p.cliente_id = ltrim(rtrim(upper(' + CHAR(39) + @Clienteid + CHAR(39) + ')))' + CHAR(13)
	End
    Else
	Begin
		set @ClienteId='1'
	    set @xSQL= @xSQL +'        AND ' + CHAR(39) + '1' + CHAR(39)+'=' + CHAR(39) + @Clienteid + CHAR(39) + CHAR(13)
	End

	if @ProductoId is not null
	Begin
	    set @xSQL= @xSQL +'        AND p.producto_id =rtrim(ltrim(upper(' + CHAR(39) + @Productoid + CHAR(39) +')))' + CHAR(13)
	End
	Else
	Begin
		set @ProductoId='1'
	    set @xSQL= @xSQL +'        AND ' + CHAR(39) + '1' +CHAR(39) +'=' + CHAR(39) + @Productoid + CHAR(39) + CHAR(13)
    End
	set @xSQL= @xSQL + ')' + CHAR(13)

	EXEC sp_executesql @xSQL

End--Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER      Procedure [dbo].[Funciones_Saldo_api#Obtener_Saldo_TR_ING_Act_Cantidad]
@Cliente_id		as Varchar(15),
@Producto_id	as Varchar(30),
@Caso			as varchar(20)

As
Begin

	declare @xSQL 			as nvarchar(400)
	declare @Acumulador		as Float
	declare @Campo			as nvarchar(50)
	declare @cliente 		as varchar(50)
	declare @productoid 	as varchar(50)
	declare @cantidad 		as float

	if @Cliente_id is null or ltrim(rtrim(@Cliente_id))=''
	Begin
		raiserror('El argumento Cliente no puede ser nulo',16,1)
	End
	if @Producto_id is null or ltrim(rtrim(@Producto_id))=''
	Begin
		raiserror('El argumento Producto no puede ser nulo',16,1)
	End

	Declare pCursorFS Cursor for
		SELECT 	dd.cliente_id,
				dd.producto_id,
				Sum(IsNull(rl.cantidad, 0)) As cantidad
		from 	rl_det_doc_trans_posicion rl, det_documento dd, categoria_logica cl
		where	rl.documento_id = dd.documento_id
				and rl.nro_linea = dd.nro_linea
				and rl.cliente_id=cl.cliente_id
				and rl.cat_log_id=cl.cat_log_id
				and cl.categ_stock_id = 'TRAN_ING'
				and dd.cliente_id = @cliente_id
				and dd.producto_id = @producto_id
		GROUP BY dd.cliente_id ,dd.producto_id , cl.categ_stock_id
		UNION all
		SELECT 	dd.cliente_id,
				dd.producto_id,
				Sum(IsNull(rl.cantidad, 0)) AS cantidad
		FROM 	rl_det_doc_trans_posicion rl,det_documento_transaccion  ddt , det_documento dd, categoria_logica cl
		WHERE 	rl.doc_trans_id = ddt.doc_trans_id AND rl.nro_linea_trans = ddt.nro_linea_trans
				and ddt.documento_id=dd.documento_id  and ddt.nro_linea_doc = dd.nro_linea
				and rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id
				and cl.categ_stock_id = 'TRAN_ING'
				And dd.cliente_id = @Cliente_id
				And dd.producto_id = @Producto_id
		group by dd.cliente_id, dd.producto_id, cl.categ_stock_id

	open pCursorFS

	If @Caso='TR_ING'
	Begin
		Set @Campo='cant_tr_ing'
	End
	Else
	Begin
		if @Caso='TR_EGR'
		Begin
			Set @Campo='cant_tr_egr'
		End
		Else
		Begin
			Set @Campo='cant_stock'
		End
	End

	Set @Acumulador=0

	FETCH NEXT FROM pCursorFS into @cliente,@producto_id,@cantidad
	While @@Fetch_Status=0
	Begin
		Set @Acumulador= @Acumulador + @cantidad
		FETCH NEXT FROM pCursorFS into @cliente,@producto_id,@cantidad
	End
	If @Acumulador > 0 
	Begin
		set @xSQL=N' Update 	#TEMP_SALDOS_STOCK Set ' + @campo + '= ' + @campo + ' + ' + cast(@Acumulador as varchar) +	' Where 	Cliente_id =' + char(39) + @cliente + char(39) +' And producto_id = ' + char(39) + @producto_id + char(39)
		EXEC sp_executesql @xSQL
	End



	Close pCursorFS
	Deallocate pCursorFS
End --Fin Picking
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER    Procedure [dbo].[Funciones_Saldo_api#Obtener_Saldo_TR_EGR_Act_Cant]
@Cliente_Id 	as varchar(15),
@Producto_id	as varchar(30),
@Caso			as varchar(10)
As
Begin
	
	declare @FSATREGR cursor
	declare @xSQL 			as nvarchar(400)
	declare @Acumulador		as Float
	declare @Campo			as nvarchar(50)
	declare @cliente 		as varchar(50)
	declare @productoid 	as varchar(50)
	declare @cantidad 		as float
	
	if @Cliente_id is null or ltrim(rtrim(@Cliente_id))=''
	Begin
		raiserror('El argumento Cliente no puede ser nulo',16,1)
		return
	End
	if @Producto_id is null or ltrim(rtrim(@Producto_id))=''
	Begin
		raiserror('El argumento Producto no puede ser nulo',16,1)
		return
	End

	Set	@FSATREGR = Cursor Forward_Only Static for
		select 	dd.cliente_id,
				dd.producto_id,
				sum(isnull(dd.cantidad, 0)) as cantidad
		from 	det_documento dd,
				documento d,
				categoria_logica cl
		where 	dd.documento_id  = d.documento_id
				and d.status = 'D20'
				and dd.cliente_id = cl.cliente_id
				and cl.cliente_id = dd.cliente_id
				and dd.cat_log_id = cl.cat_log_id
				and cl.categ_stock_id = 'TRAN_EGR'
				and dd.cliente_id = @Cliente_Id
				and dd.producto_id = @Producto_id
		group by dd.cliente_id, dd.producto_id, cl.categ_stock_id
		union all
		select 	dd.cliente_id,
				dd.producto_id,
				sum(isnull(dd.cantidad, 0)) as cantidad
		from  	det_documento_transaccion ddt,
				det_documento dd,
				documento_transaccion dt,
				categoria_logica cl
		where 	ddt.cliente_id = cl.cliente_id
				and ddt.cat_log_id = cl.cat_log_id
				and cl.cliente_id = dd.cliente_id
				and cl.categ_stock_id = 'TRAN_EGR'
				and ddt.documento_id = dd.documento_id
				and ddt.nro_linea_doc = dd.nro_linea
				and ddt.doc_trans_id = dt.doc_trans_id
				and dt.status = 'T10'
				and not exists (select 	rl_id 
								from 	rl_det_doc_trans_posicion rl
								where 	rl.doc_trans_id_egr = ddt.doc_trans_id
										and rl.nro_linea_trans_egr = ddt.nro_linea_trans)
				and dd.cliente_id = @Cliente_id
				and dd.producto_id = @Producto_id
		group by dd.cliente_id, dd.producto_id, cl.categ_stock_id
		union all
		select 	dd.cliente_id,
				dd.producto_id,
				sum(isnull(dd.cantidad, 0)) as cantidad
		from  	det_documento_transaccion ddt,
				det_documento dd,
				rl_det_doc_trans_posicion rl,
				documento d,
				categoria_logica cl
		where 	dd.documento_id = d.documento_id
				and rl.cat_log_id = cl.cat_log_id
				and d.status = 'D30'
				and rl.cliente_id = cl.cliente_id
				and cl.cliente_id = dd.cliente_id
				and cl.categ_stock_id = 'TRAN_EGR'
				and ddt.documento_id = dd.documento_id
				and ddt.nro_linea_doc = dd.nro_linea
				and rl.doc_trans_id_egr = ddt.doc_trans_id
				and rl.nro_linea_trans_egr = ddt.nro_linea_trans
				and dd.cliente_id = @Cliente_id
				and dd.producto_id = @Producto_id
		group by dd.cliente_id, dd.producto_id, cl.categ_stock_id

	If @Caso='TR_ING'
	Begin
		Set @Campo='cant_tr_ing'
	End
	Else
	Begin
		if @Caso='TR_EGR'
		Begin
			Set @Campo='cant_tr_egr'
		End
		Else
		Begin
			Set @Campo='cant_stock'
		End
	End

	open @FSATREGR

	Set @Acumulador=0

	FETCH NEXT FROM @FSATREGR into @cliente,@producto_id,@cantidad
	While @@Fetch_Status=0
	Begin
		Set @Acumulador= @Acumulador + @cantidad
		FETCH NEXT FROM @FSATREGR into @cliente,@producto_id,@cantidad
	End
	If @Acumulador > 0 
	Begin
		set @xSQL=N' Update 	#TEMP_SALDOS_STOCK Set ' + @campo + '= ' + @campo + ' + ' + Cast(@Acumulador as varchar) +	' Where 	Cliente_id =' + char(39) + @cliente + char(39) +' And producto_id = ' + char(39) + @producto_id + char(39)

		EXEC sp_executesql @xSQL

	End



	Close @FSATREGR
	Deallocate @FSATREGR

End --Fin Del Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER     procedure [dbo].[Funciones_Saldo_Api#Obtener_Saldo_Stock_Actualizar_Cantidades]
@Cliente_Id		as varchar(15),
@Producto_Id	as varchar(30),
@Caso			as varchar(10)
As
Begin
	
	Declare @FSAOST 		cursor
	declare @xSQL 			as nvarchar(400)
	declare @Acumulador		as Float
	declare @Campo			as nvarchar(50)
	declare @cliente 		as varchar(50)
	declare @productoid 	as varchar(50)
	declare @cantidad 		as float
	
	if @Cliente_id is null or ltrim(rtrim(@Cliente_id))=''
	Begin
		raiserror('El argumento Cliente no puede ser nulo',16,1)
		return
	End
	if @Producto_id is null or ltrim(rtrim(@Producto_id))=''
	Begin
		raiserror('El argumento Producto no puede ser nulo',16,1)
		return
	End

	Set @FSAOST= Cursor Forward_Only For
		select 	dd.cliente_id,
				dd.producto_id,
				sum(isnull(rl.cantidad, 0)) as cantidad
		from 	rl_det_doc_trans_posicion rl
				inner join det_documento_transaccion ddt
				on (ddt.doc_trans_id = rl.doc_trans_id	and ddt.nro_linea_trans =rl.nro_linea_trans)
				inner join det_documento dd
				on (dd.documento_id = ddt.documento_id	and dd.nro_linea = ddt.nro_linea_doc)
				inner join categoria_logica cl
				on (cl.cliente_id = rl.cliente_id and cl.cat_log_id = rl.cat_log_id	and cl.categ_stock_id = 'stock')
		where
				dd.cliente_id = @Cliente_Id
				and dd.producto_id = @Producto_Id
		group by dd.cliente_id, dd.producto_id
		order by dd.cliente_id, dd.producto_id

	If @Caso='TR_ING'
	Begin
		Set @Campo='cant_tr_ing'
	End
	Else
	Begin
		if @Caso='TR_EGR'
		Begin
			Set @Campo='cant_tr_egr'
		End
		Else
		Begin
			Set @Campo='cant_stock'
		End
	End
	
	Open @FSAOST

	Set @Acumulador=0

	FETCH NEXT FROM @FSAOST into @cliente,@producto_id,@cantidad
	While @@Fetch_Status=0
	Begin
		Set @Acumulador= @Acumulador + @cantidad
		FETCH NEXT FROM @FSAOST into @cliente,@producto_id,@cantidad
	End
	If @Acumulador > 0 
	Begin
		set @xSQL=N' Update 	#TEMP_SALDOS_STOCK Set ' + @campo + '= ' + @campo + ' + ' + cast(@Acumulador as varchar) +	' Where 	Cliente_id =' + char(39) + @cliente + char(39) +' And producto_id = ' + char(39) + @producto_id + char(39)
		EXEC sp_executesql @xSQL
	End



	Close @FSAOST
	Deallocate @FSAOST

End--Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  procedure [dbo].[Funciones_Saldo_Api#Actualizar_Saldos_STOCK1]
As
Begin
	Update #TEMP_SALDOS_STOCK Set CANT_STOCK = CANT_STOCK  - CANT_TR_EGR
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER    Procedure [dbo].[Funciones_Saldo_Api#Actualizar_Saldos_STOCK2]
@Cliente_id 	as varchar(15),
@Producto_Id	as varchar(30)
As
Begin
	
	declare @FSAASS2	Cursor
	declare @ClienteID	as varchar(15)
	declare @productoid as varchar(30)
	declare @acumulador	as float
	declare @cantidad	as float
	
	
	Set @FSAASS2= Cursor Forward_only for
		SELECT 	dd.cliente_id,
				dd.producto_id,
				Sum(IsNull(dd.cantidad, 0)) AS cantidad
		FROM 	det_documento_transaccion ddt,
				det_documento dd,
				documento d,
				categoria_logica cl
		WHERE 	dd.documento_id = d.documento_id
				And d.status = 'D30'
				And dd.cliente_id = cl.cliente_id
				And dd.cat_log_id = cl.cat_log_id
				AND cl.categ_stock_id = 'TRAN_EGR'
				AND ddt.documento_id = dd.documento_id
				AND ddt.nro_linea_doc = dd.nro_linea
				And dd.cliente_id =@Cliente_id
				And dd.producto_id = @Producto_Id
				And Exists (SELECT rl_id FROM rl_det_doc_trans_posicion rl
		WHERE 	rl.doc_trans_id_egr=ddt.doc_trans_id
				And rl.nro_linea_trans_egr=ddt.nro_linea_trans)
		GROUP BY dd.cliente_id, dd.producto_id, cl.categ_stock_id

	set @Acumulador=0

	Open @FSAASS2
	Fetch Next From @FSAASS2 into @ClienteId,@ProductoId,@Cantidad
	While @@Fetch_Status=0
	Begin
		set @Acumulador= @Acumulador + @Cantidad
		Fetch Next From @FSAASS2 into @ClienteId,@ProductoId,@Cantidad		
	End

	Update 	#TEMP_SALDOS_STOCK Set CANT_STOCK = CANT_STOCK - @Acumulador
	WHERE 	cliente_id = @ClienteId
			And producto_id = @productoid
	
	Close 		@FSAASS2
	Deallocate 	@FSAASS2
End	--Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  Procedure [dbo].[Funciones_Saldo_Api#Generar_Saldos_Stock]
@Cliente_Id 	as varchar(15),
@Producto_Id	as varchar(30)
As
Begin
	
	Declare @ErrorCod	as int

	Exec @ErrorCod=Funciones_Saldo_Api#Insertar_Lista_Productos @Cliente_ID,@Producto_ID
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_Api#Insertar_Lista_Productos.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_api#Obtener_Saldo_TR_ING_Act_Cantidad @Cliente_ID,@Producto_ID,'TR_ING'
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_api#Obtener_Saldo_TR_ING_Act_Cantidad.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_api#Obtener_Saldo_TR_EGR_Act_Cant @Cliente_ID,@Producto_ID,'TR_EGR'
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_api#Obtener_Saldo_TR_EGR_Act_Cant.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_Api#Obtener_Saldo_Stock_Actualizar_Cantidades @Cliente_ID,@Producto_ID,'STOCK'
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_Api#Obtener_Saldo_Stock_Actualizar_Cantidades.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_Api#Actualizar_Saldos_STOCK1
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_Api#Actualizar_Saldos_STOCK1.',16,1)
		Return
	End

	Exec @ErrorCod=Funciones_Saldo_Api#Actualizar_Saldos_STOCK2 @Cliente_Id, @Producto_ID
	if @ErrorCod>0
	Begin
		Raiserror('Ocurrio un error inesperado al ejecutar Funciones_Saldo_Api#Actualizar_Saldos_STOCK2.',16,1)
		Return
	End
	
End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER      procedure [dbo].[Funciones_Historico_Api#Actualizar_HistSaldos_STOCK]
@Documento_id 	as numeric(20,0), 
@Doc_Trans_id	as numeric(20,0),
@Rl_id 			as numeric(20,0)
As
Begin

	declare @FHAAHS 	Cursor
	declare @ClienteId	as varchar(15)
	declare @ProductoId as varchar(30)
	declare @QtyTrIng	as float
	declare @CantStock  as Float
	declare @QtyTrEgr   as float
	declare @Docid 		as numeric(20,0)
	declare @NroLinea	as numeric(10,0)
	declare @Status		as varchar(3)
	
	If @Documento_Id is not null
	Begin
		Set @FHAAHS=Cursor Forward_Only for
			SELECT 	dd.cliente_id,
					dd.producto_id,
					d.documento_id,
					dd.nro_linea,
					d.status
			FROM 	det_documento dd,
					documento d
			WHERE 	dd.documento_id = d.documento_id
					And dd.cliente_id = d.cliente_id
					And d.documento_id = @Documento_id

		Open @FHAAHS
	End
	If @Doc_Trans_id is not null
	Begin
		Set @FHAAHS=Cursor Forward_Only for
			SELECT 	dd.cliente_id,
					dd.producto_id,
					d.documento_id,
					dd.nro_linea,
					d.status
			FROM 	det_documento_transaccion ddt,
					det_documento dd,
					documento d
			WHERE	dd.documento_id = ddt.documento_id
					And ddt.documento_id = d.documento_id
					And dd.nro_linea = ddt.nro_linea_doc
					And ddt.doc_trans_id = @Doc_Trans_id

		Open @FHAAHS

	End
	If @Rl_Id is not null
	Begin
		Set @FHAAHS= Cursor Forward_Only For
			SELECT 	dd.cliente_id,
					dd.producto_id,
					d.documento_id,
					dd.nro_linea,
					d.status
			FROM 	rl_det_doc_trans_posicion rl,
					det_documento_transaccion ddt,
					det_documento dd,
					documento d
			WHERE 	dd.documento_id = ddt.documento_id
					And ddt.documento_id = d.documento_id
					And dd.nro_linea = ddt.nro_linea_doc
					And ddt.doc_trans_id = rl.doc_trans_id
					And ddt.nro_linea_trans = rl.nro_linea_trans
					And rl.rl_id = @Rl_Id
		
		Open @FHAAHS
	End

	Fetch Next From @FHAAHS into @ClienteId,@ProductoId,@DocId,@NroLinea,@Status
	While @@Fetch_Status=0
	Begin
		Exec Funciones_Saldo_Api#Generar_Saldos_Stock @ClienteId,@ProductoId

		Select 	@QtyTrIng=cant_tr_ing,@CantStock=cant_stock,@QtyTrEgr=cant_tr_egr
		From 	#temp_saldos_stock

		Insert Into HISTORICO_SALDOS_STOCK(Fecha, CLIENTE_ID, PRODUCTO_ID, CANT_TR_ING, CANT_STOCK,CANT_TR_EGR,DOCUMENTO_ID,NRO_LINEA,DOC_STATUS)
		Values (getdate(),@ClienteId,@ProductoId,@QtyTrIng,@CantStock,@QtyTrEgr,@DocId,@NroLinea,@Status)

	End --Fin Primer While
	Close		@FHAAHS
	Deallocate 	@FHAAHS
End --Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER  Procedure [dbo].[Funciones_Historico_Api#Enviar_RL_a_Historico]
@Doc_trans_id	as numeric(20,0),
@tipo_operacion	as varchar(5)
As
Begin
	declare @rl_hist_id 		as numeric(20,0)
	declare @doctransid 		as numeric(20,0)	
	declare @nro_linea_tr		as numeric(10,0)
	declare @pos_anterior		as numeric(20,0)
	declare @pos_actual			as numeric(20,0)
	declare @cantidad			as numeric(20,5)
	declare @tipo_mov_id		as varchar(5)
	declare @ultima_est			as varchar(5)
	declare @ultima_sec			as numeric(3,0)
	declare @nave_ant			as numeric(20,0)
	declare @nave_act			as numeric(20,0)
	declare @doc_id				as numeric(20,0)
	declare @nro_linea			as numeric(10,0)
	declare @disponible			as varchar(1)
	declare @doc_trans_id_egr	as numeric(20,0)
	declare @nro_lin_trans_egr	as numeric(10,0)
	declare @doc_trans_id_tr	as numeric(20,0)
	declare @nro_lin_tran_id_tr	as numeric(10,0)
	declare	@cliente_id			as varchar(15)
	declare @cat_log_id			as varchar(50)	
	declare @cat_log_id_final 	as varchar(50)
	declare	@est_merc_id		as varchar(15)
	Declare @FHERH 	Cursor

	if @tipo_operacion='ING'
	Begin
		Set @FHERH= Cursor Forward_Only Static for
		select * from rl_det_doc_trans_posicion where DOC_TRANS_ID =@Doc_trans_id
	End
	If @Tipo_Operacion='EGR'
	Begin
		Set @FHERH= Cursor Forward_Only Static for
		select * from rl_det_doc_trans_posicion where DOC_TRANS_ID_EGR=@Doc_trans_id
	End
	If @Tipo_Operacion='TR'
	Begin
		Set @FHERH= Cursor Forward_Only Static for
		select * from rl_det_doc_trans_posicion where DOC_TRANS_ID_TR =@Doc_trans_id
	End

	Open @FHERH
	
	fetch next from @fherh into   @rl_hist_id	, @doc_trans_id	, @nro_linea_tr	, @pos_anterior	, @pos_actual
								, @cantidad	, @tipo_mov_id	, @ultima_est	, @ultima_sec	, @nave_ant
								, @nave_act	, @doc_id	, @nro_linea	, @disponible	, @doc_trans_id_egr
								, @nro_lin_trans_egr	, @doc_trans_id_tr	, @nro_lin_tran_id_tr	, @cliente_id	
								, @cat_log_id	, @cat_log_id_final 	, @est_merc_id
	While @@Fetch_Status=0
	Begin

		insert into rl_det_doc_tr_pos_hist values(	  @doc_trans_id	, @nro_linea_tr	, @pos_anterior	, @pos_actual
												, @cantidad	, @tipo_mov_id	, @ultima_est	, @ultima_sec	, @nave_ant
												, @nave_act	, @doc_id	, @nro_linea	, @disponible	, @doc_trans_id_egr
												, @nro_lin_trans_egr	, @doc_trans_id_tr	, @nro_lin_tran_id_tr	, @cliente_id	
												, @cat_log_id	, @cat_log_id_final 	, @est_merc_id )

		fetch next from @fherh into   @rl_hist_id	, @doc_trans_id	, @nro_linea_tr	, @pos_anterior	, @pos_actual
									, @cantidad	, @tipo_mov_id	, @ultima_est	, @ultima_sec	, @nave_ant
									, @nave_act	, @doc_id	, @nro_linea	, @disponible	, @doc_trans_id_egr
									, @nro_lin_trans_egr	, @doc_trans_id_tr	, @nro_lin_tran_id_tr	, @cliente_id	
									, @cat_log_id	, @cat_log_id_final 	, @est_merc_id
	End
	Close @FHERH
	Deallocate @FHERH
End --Fin procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   Procedure [dbo].[Funciones_Historicos_api#Actualizar_Historicos_X_Mov]
@RlId 	as Numeric(20,0)
As
Begin
	declare @posicion_actual	as numeric(20,0)
	declare @nave_actual		as numeric(20,0)
	declare @posicion_anterior	as numeric(20,0)
	declare @nave_anterior		as numeric(20,0)
	declare @cantidad			as numeric(20,5)
	declare @documento_id		as numeric(20,0)
	declare @nro_linea_doc		as numeric(10,0)
	declare @doc_trans_id_egr	as numeric(20,0)
	declare @cliente_id			as varchar(15)
	declare @producto_id		as varchar(30)
	declare @nro_serie			as varchar(50)
	declare @nro_lote			as varchar(50)
	declare @fecha_vencimiento 	as datetime
	declare @nro_partida		as varchar(50)
	declare @nro_despacho		as varchar(50)
	declare @codigo				as varchar(10)
	declare @codigo2			as varchar(10)
	declare @es_pre_egreso		as varchar(1)
	declare @Usuario			as varchar(30)

	declare fhahxm cursor for
		select rl.posicion_actual, rl.nave_actual, rl.posicion_anterior,
		       rl.nave_anterior, rl.cantidad, ddt.documento_id, ddt.nro_linea_doc,
		       rl.doc_trans_id_egr, dd.cliente_id, dd.producto_id,dd.nro_serie,
		       dd.nro_lote, dd.fecha_vencimiento, dd.nro_partida,dd.nro_despacho
		from  rl_det_doc_trans_posicion rl,
		       det_documento_transaccion ddt,
		       det_documento_transaccion ddt2,
		       det_documento dd
		where ddt2.documento_id = dd.documento_id
		       and ddt2.nro_linea_doc = dd.nro_linea
		       and rl.doc_trans_id = ddt2.doc_trans_id
		       and rl.nro_linea_trans = ddt2.nro_linea_trans
		       and isnull(rl.doc_trans_id_tr, isnull(rl.doc_trans_id_egr, rl.doc_trans_id)) = ddt.doc_trans_id
		       and isnull(rl.nro_linea_trans_tr, isnull(rl.nro_linea_trans_egr, rl.nro_linea_trans)) = ddt.nro_linea_trans
		       and rl.rl_id = @RlId

	open fhahxm

	SELECT 	@Usuario=usuario_id FROM #temp_usuario_loggin

	fetch next from fhahxm into  @posicion_actual 	,@nave_actual
								,@posicion_anterior ,@nave_anterior
								,@cantidad			,@documento_id
								,@nro_linea_doc		,@doc_trans_id_egr
								,@cliente_id		,@producto_id
								,@nro_serie			,@nro_lote
								,@fecha_vencimiento	,@nro_partida
								,@nro_despacho
	while @@fetch_status=0
	Begin
		if @doc_trans_id_egr is null
		Begin
			set @codigo = 'TR'
            set @codigo2 = '+'
		End
		Else
		begin
			set @codigo = 'EGR'
			if @nave_actual is not null
			Begin
				select @es_pre_egreso=pre_egreso from nave	where nave_id =@nave_actual
			End
			else
			begin
				set @es_pre_egreso = '0'
			end

	        If @es_pre_egreso = '1'
			begin
                set @codigo2 = '0'
			end
            Else
			begin
                set @codigo2 = '+'
			end
		End

		insert into historico_producto 
		values(	 getdate(),@posicion_anterior,@cantidad,@codigo
				,null,@nave_anterior,@documento_id,@nro_linea_doc
				,@usuario,'-',@cliente_id,@producto_id
				,@nro_serie,@nro_lote,@fecha_vencimiento,@nro_partida
				,@nro_despacho)

		insert into historico_producto 
		values(	getdate(), @posicion_actual,@cantidad,@codigo,null,@nave_actual,
				@documento_id,@nro_linea_doc,@usuario,@codigo2,@cliente_id,	
				@producto_id,@nro_serie,@nro_lote,@fecha_vencimiento,@nro_partida,
				@nro_despacho)

		insert into historico_posicion
		values(	@posicion_anterior,'EGR',getdate(),null,@cantidad,@documento_id,@nro_linea_doc,@usuario,
				@nave_anterior,@cliente_id,@producto_id,@nro_serie,@nro_lote,@fecha_vencimiento,
				@nro_partida,@nro_despacho)
		
		insert into historico_posicion
		values(	@posicion_actual,'ING',getdate(),null,@cantidad,@documento_id,@nro_linea_doc,@usuario,
				@nave_actual,@cliente_id,@producto_id,@nro_serie,@nro_lote,@fecha_vencimiento,@nro_partida,
				@nro_despacho)

		fetch next from fhahxm into  @posicion_actual 	,@nave_actual
									,@posicion_anterior ,@nave_anterior
									,@cantidad			,@documento_id
									,@nro_linea_doc		,@doc_trans_id_egr
									,@cliente_id		,@producto_id
									,@nro_serie			,@nro_lote
									,@fecha_vencimiento	,@nro_partida
									,@nro_despacho
	end

	close fhahxm
	deallocate fhahxm
End --Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[FUNCIONES_HISTORICOS_API_ACTUALIZAR_POSICIONES_OCUPADAS]
AS
BEGIN

DECLARE @TOTAL NUMERIC,
	@ULTIMAFECHA VARCHAR (20),
	@FECHA_ACTUAL VARCHAR (20),
	@NUEVAFECHA varchar (20),
	@DIF_DIAS NUMERIC,
	@CONT NUMERIC,
	@HIST_ID NUMERIC,
	@FECHA DATETIME,
	@CLIENTE_ID VARCHAR (15),
	@NAVE_ID NUMERIC (20, 0),
	@POSICION_ID NUMERIC(20, 0),
	@CANTIDAD NUMERIC(10, 0),
	@PESO NUMERIC(20, 5),
	@UNIDAD_PESO VARCHAR (5),
	@VOLUMEN NUMERIC(20, 5),
	@UNIDAD_VOLUMEN VARCHAR (5)

	SET @FECHA_ACTUAL = GetDate()

	SELECT 	@total = COUNT(hist_id)
	FROM 	historico_pos_ocupadas
	WHERE 	fecha = cast(getdate() -1 as datetime)

	SELECT 	@UltimaFecha = Max(fecha)
	FROM 	historico_pos_ocupadas
 
	IF @TOTAL = 0
		BEGIN
			DECLARE	PCUR CURSOR FOR
				SELECT
						-- CONSULTA PARA SABER LOS TOTALES SOBRE LA TABLA DE HISTORICO_POS_OCUPADAS_DET
						X.CLIENTE_ID,
						X.NAVE_ID,
						X.POSICION_ID,
						COUNT(X.POSICION_ID) + (COUNT(X.NAVE_ID) - COUNT(X.POSICION_ID)) AS CANTIDAD,
						Round(Sum(X.PESO_TOTAL),3) AS PESO,
						X.UNIDAD_PESO,
						Round(Sum(X.VOLUMEN_TOTAL),3) AS VOLUMEN,
						X.UNIDAD_VOLUMEN
				FROM
		                (
						SELECT
								-- AGRUPO LOS DATOS POR ' DET.CLIENTE_ID, DET.NAVE, DET.POSICION '
								getdate() AS FECHA,
								DET.CLIENTE_ID,
								DET.NAVE_ID,
								DET.POSICION_ID,
								Round(Sum(DET.PESO_TOTAL),3) AS PESO_TOTAL,
								-- Por razones obvias se harcodea esto, ya que no puedo mezclar Peras con Bananas, tengo que forzar el Group By
								'KG' AS UNIDAD_PESO,
								Round(Sum(DET.VOLUMEN_TOTAL),3) AS VOLUMEN_TOTAL,
								-- Por razones obvias se harcodea esto, ya que no puedo mezclar Peras con Bananas, tengo que forzar el Group By
								'M3' AS UNIDAD_VOLUMEN
						FROM
								(
			                      -- OBTENGO TODOS LOS PRODUCTOS CON SU SALDO, PESO Y VOLUMEN ACTUAL.
								SELECT
										DD.CLIENTE_ID,
										DD.PRODUCTO_ID,
										rl.rl_id,
										RL.CANTIDAD,
										DD.UNIDAD_ID,
										isnull(N1.NAVE_ID,NAP.NAVE_ID) AS NAVE_ID,
										isnull(N1.NAVE_COD,NAP.NAVE_COD) AS NAVE_COD,
										P1.POSICION_ID AS POSICION_ID,
										P1.POSICION_COD AS POSICION_COD,
										CASE DD.PESO_UNITARIO
										WHEN 1 THEN DD.PESO * RL.CANTIDAD
										ELSE DD.PESO / DD.CANTIDAD * RL.CANTIDAD
										END As PESO_TOTAL,
										DD.UNIDAD_PESO,
										CASE DD.VOLUMEN_UNITARIO
										WHEN 1 THEN DD.VOLUMEN * RL.CANTIDAD
										ELSE DD.VOLUMEN / DD.CANTIDAD * RL.CANTIDAD
										END As VOLUMEN_TOTAL,
										DD.UNIDAD_VOLUMEN
								FROM 	DET_DOCUMENTO DD
										INNER JOIN DET_DOCUMENTO_TRANSACCION DDT 	ON (DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA)
										INNER JOIN RL_DET_DOC_TRANS_POSICION  RL 	ON (RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS)
										LEFT JOIN NAVE                       N1 	ON (NAVE_ID = RL.NAVE_ACTUAL AND N1.PRE_EGRESO ='0' AND N1.PRE_INGRESO = '0')
										LEFT JOIN POSICION                   P1 	ON (P1.POSICION_ID = RL.POSICION_ACTUAL)
										LEFT JOIN NAVE                      NAP 	ON (P1.NAVE_ID = NAP.NAVE_ID)
								) DET
					GROUP BY
								DET.CLIENTE_ID,
								DET.NAVE_ID,
								DET.NAVE_COD,
								DET.POSICION_ID,
								DET.POSICION_COD
					) X
					GROUP BY  X.FECHA,
					          X.CLIENTE_ID,
					          X.NAVE_ID,
							  X.POSICION_ID,
					          X.UNIDAD_PESO,
					          X.UNIDAD_VOLUMEN
		              -- PARA ASEGURARME QUE NO TRAIGA COSAS QUE NO EXISTEN
					HAVING 	(COUNT(X.POSICION_ID) > 0 OR COUNT(X.NAVE_ID) - COUNT(X.POSICION_ID) > 0 )
					ORDER BY 	X.FECHA,
						        X.CLIENTE_ID,
						        X.NAVE_ID
	
 				OPEN PCUR
				FETCH NEXT FROM PCUR INTO @CLIENTE_ID, @NAVE_ID, @POSICION_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN
				WHILE @@FETCH_STATUS = 0
				BEGIN
					INSERT INTO HISTORICO_POS_OCUPADAS ( FECHA, CLIENTE_ID, NAVE_ID, CANTIDAD, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN) VALUES(CONVERT(DateTime, datediff(d, 1, @FECHA_ACTUAL )), @CLIENTE_ID, @NAVE_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN)
					INSERT INTO HISTORICO_POSICION_OCUPADAS ( FECHA, CLIENTE_ID, NAVE_ID, POSICION_ID, CANTIDAD, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN) VALUES(CONVERT(DateTime, datediff(d, 1, @FECHA_ACTUAL )), @CLIENTE_ID, @NAVE_ID, @POSICION_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN)					
					FETCH NEXT FROM PCUR INTO @CLIENTE_ID, @NAVE_ID, @POSICION_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN
				END
				CLOSE PCUR
				DEALLOCATE PCUR

			SET @FECHA_ACTUAL=CAST(@FECHA_ACTUAL AS DATETIME)-2 
		    SET @DIF_DIAS =DATEDIFF(DAY,@ULTIMAFECHA,@FECHA_ACTUAL)
			IF @dif_dias = NULL
			BEGIN
				SET @dif_dias = 0
			END
			IF @DIF_DIAS > 0
			BEGIN
				DECLARE	PCUR1 CURSOR FOR
					SELECT * FROM HISTORICO_POSICION_OCUPADAS H1
					WHERE FECHA = CONVERT(DateTime, CONVERT(VarChar, GETDATE() - 1, 112))
					AND NAVE_ID IN (21,29)

				OPEN PCUR1
				FETCH NEXT FROM PCUR1 INTO @HIST_ID, @FECHA, @CLIENTE_ID, @NAVE_ID, @POSICION_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN
				WHILE @@FETCH_STATUS = 0
				BEGIN
					SET @CONT = 1
					WHILE @CONT <= @DIF_DIAS
					BEGIN
						--SET @NUEVAFECHA = CONVERT(DateTime, DateAdd(d, @CONT, @UltimaFecha ))
						--INSERT INTO HISTORICO_POS_OCUPADAS (FECHA, CLIENTE_ID, NAVE_ID, CANTIDAD, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN) VALUES( CAST(@NUEVAFECHA AS DATETIME ), @CLIENTE_ID, @NAVE_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN)
						--INSERT INTO HISTORICO_POSICION_OCUPADAS ( FECHA, CLIENTE_ID, NAVE_ID, POSICION_ID, CANTIDAD, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN) VALUES( CAST (@NUEVAFECHA AS DATETIME), @CLIENTE_ID, @NAVE_ID, @POSICION_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN)
		
						INSERT INTO HISTORICO_POS_OCUPADAS (FECHA, CLIENTE_ID, NAVE_ID, CANTIDAD, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN) VALUES( CONVERT(DateTime, datediff(d, @CONT, GETDATE() - 1 )), @CLIENTE_ID, @NAVE_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN)
						INSERT INTO HISTORICO_POSICION_OCUPADAS (FECHA, CLIENTE_ID, NAVE_ID, POSICION_ID, CANTIDAD, PESO, UNIDAD_PESO, VOLUMEN, UNIDAD_VOLUMEN) VALUES( CONVERT(DateTime, datediff(d, @CONT, GETDATE() - 1 )), @CLIENTE_ID, @NAVE_ID, @POSICION_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN)

						FETCH NEXT FROM PCUR1 INTO @HIST_ID, @FECHA, @CLIENTE_ID, @NAVE_ID, @POSICION_ID, @CANTIDAD, @PESO, @UNIDAD_PESO, @VOLUMEN, @UNIDAD_VOLUMEN
						SET @CONT = @CONT + 1
					END
			END
			CLOSE PCUR1
			DEALLOCATE PCUR1
			END
		END
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[FUNCIONES_INVENTARIO#VERIFICA_INV_CERRADO]
	@Doc_Trans_id AS NUMERIC(20) output
AS
BEGIN

	BEGIN TRY
		select isnull(cerrado,'0') from inventario where doc_trans_id = @Doc_Trans_id
	
	END TRY
	
	BEGIN CATCH

		EXEC usp_RethrowError
	END CATCH

END --PROCEDURE
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#ACT_FECHA_LOCK_GRABA]
@P_DOC_TRANS_ID AS SQL_VARIANT OUTPUT
AS
BEGIN
	DECLARE @V_DOC_TRANS_ID AS NUMERIC(20)

	SET @V_DOC_TRANS_ID = CONVERT(NUMERIC(20), @P_DOC_TRANS_ID)
	UPDATE INVENTARIO SET F_LOCKGRABA = GETDATE() WHERE DOC_TRANS_ID = @V_DOC_TRANS_ID
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#ACT_STOCK]
@P_INVENTARIO_ID AS NUMERIC(20) OUTPUT,
@P_MARBETE AS INT
AS
BEGIN
	BEGIN TRY
		DECLARE @V_CAT_LOG AS INT
		DECLARE @V_FOM_PROD AS INT
		DECLARE @V_NUM_STOCK AS TINYINT
		DECLARE @V_PRODUCTO_ID AS VARCHAR(100)
		DECLARE @V_CLIENTE_ID AS VARCHAR(100)
		DECLARE @V_POSICION_ID AS NUMERIC(20)
		DECLARE @V_NAVE_ID AS NUMERIC(20)
		DECLARE @V_CANT AS NUMERIC(20,5)
		DECLARE @V_NRO_LOTE AS VARCHAR(100)
		DECLARE @V_NRO_PARTIDA AS VARCHAR(100)


		SELECT @V_CAT_LOG=COUNT(*) FROM DET_INVENTARIO_CAT_LOG WHERE INVENTARIO_ID = @P_INVENTARIO_ID
		SELECT @V_FOM_PROD=COUNT(*) FROM DET_INVENTARIO_FAM_PROD WHERE INVENTARIO_ID = @P_INVENTARIO_ID
		SELECT @V_NUM_STOCK = NRO_CONTEO FROM INVENTARIO WHERE INVENTARIO_ID = @P_INVENTARIO_ID
		IF @P_MARBETE <> 0 
			BEGIN 
				SELECT @V_PRODUCTO_ID = PRODUCTO_ID,@V_CLIENTE_ID = CLIENTE_ID, @V_POSICION_ID = POSICION_ID, @V_NAVE_ID = NAVE_ID, @V_NRO_LOTE = NRO_LOTE, @V_NRO_PARTIDA = NRO_PARTIDA
				FROM DET_INVENTARIO 
				WHERE INVENTARIO_ID = @P_INVENTARIO_ID AND MARBETE = @P_MARBETE
			END



		UPDATE DET_INVENTARIO SET CANT_STOCK_CONT_1 = CASE WHEN @V_NUM_STOCK = 1 THEN X.CANTIDAD ELSE CANT_STOCK_CONT_1 END,
								  CANT_STOCK_CONT_2 = CASE WHEN @V_NUM_STOCK = 2 THEN X.CANTIDAD ELSE CANT_STOCK_CONT_2 END,
								  CANT_STOCK_CONT_3 = CASE WHEN @V_NUM_STOCK = 3 THEN X.CANTIDAD ELSE CANT_STOCK_CONT_3 END
		FROM DET_INVENTARIO DI INNER JOIN (
			SELECT  XX.nave_id,                      
					 XX.posicion_id,                   
					 XX.cliente_id,                    
					 XX.producto_id,                  
					 max(XX.pos_lockeada) as pos_lockeada,                  
					 sum(XX.cantidad) AS cantidad,
					 XX.nro_lote,
					 XX.nro_partida FROM ( 
						SELECT DISTINCT                         
							 nave.nave_id,                      
							 pos.posicion_id,                   
							 cli.cliente_id,                    
							 prod.producto_id,                  
							 pos.pos_lockeada,                  
							 sum(ex.cantidad) AS cantidad,
							 det_doc.nro_lote,
							 det_doc.nro_partida
						FROM                                                        
							 documento doc                                          
							 INNER JOIN det_documento det_doc ON (doc.documento_id=det_doc.documento_id) 
							 INNER JOIN cliente cli ON (det_doc.cliente_id=cli.cliente_id)               
							 INNER JOIN det_documento_transaccion det_doc_t ON (det_doc.documento_id=det_doc_t.documento_id AND det_doc.nro_linea=det_doc_t.nro_linea_doc) 
							 INNER JOIN documento_transaccion dt ON (det_doc_t.doc_trans_id=dt.doc_trans_id) 
							 INNER JOIN producto prod ON (det_doc.cliente_id=prod.cliente_id AND det_doc.producto_id=prod.producto_id) 
							 INNER JOIN familia_producto flia ON (prod.familia_id=flia.familia_id) 
							 INNER JOIN rl_det_doc_trans_posicion ex ON (det_doc_t.doc_trans_id=ex.doc_trans_id AND det_doc_t.nro_linea_trans=ex.nro_linea_trans) 
							 LEFT  JOIN categoria_logica cat ON ex.cliente_id=cat.cliente_id AND ex.cat_log_id=cat.cat_log_id 
							 LEFT  JOIN posicion pos ON  ex.posicion_actual=pos.posicion_id 
							 LEFT  JOIN nave nave on ex.nave_actual=nave.nave_id 

						 

						WHERE ((ex.nave_actual not in (select nave_id from nave where nave_cod = 'PRE-EGRESO')) or ( ex.nave_actual is null))
						AND (@V_CAT_LOG = 0 OR ex.cat_log_id IN (SELECT DCL.CAT_LOG_ID FROM DET_INVENTARIO_CAT_LOG DCL WHERE DCL.INVENTARIO_ID = @P_INVENTARIO_ID AND DCL.CLIENTE_ID = EX.CLIENTE_ID))
						AND (@V_FOM_PROD = 0  OR PROD.FAMILIA_ID IN (SELECT FAM.FAMILIA_ID FROM DET_INVENTARIO_FAM_PROD FAM WHERE FAM.INVENTARIO_ID = @P_INVENTARIO_ID))
						AND (@P_MARBETE = 0 OR 
							(PROD.PRODUCTO_ID = @V_PRODUCTO_ID AND EX.CLIENTE_ID = @V_CLIENTE_ID AND (NAVE.NAVE_ID = @V_NAVE_ID OR POS.POSICION_ID = @V_POSICION_ID)
								 AND (@V_NRO_LOTE IS NULL OR DET_DOC.NRO_LOTE = @V_NRO_LOTE)	
								 AND (@V_NRO_PARTIDA IS NULL OR DET_DOC.NRO_PARTIDA = @V_NRO_PARTIDA)))

						GROUP BY nave.nave_id, pos.posicion_id, cli.cliente_id, prod.producto_id, pos.pos_lockeada,det_doc.nro_lote,
							 det_doc.nro_partida

						--MERCADERRIA ASIGNADA NO PICKEADA 
						UNION ALL
						SELECT 					 rl.nave_anterior as nave,                                
												 rl.posicion_anterior as posicion,                            
												 dd.cliente_id,                                           
												 dd.producto_id, 0 as pos_lockeada,                    
												 sum(dd.cantidad - (ISNULL(P.CANT_CONFIRMADA,0))) AS CANTIDAD,
												 dd.nro_lote,
												 dd.nro_partida
										  FROM  det_documento_transaccion ddt                                
												 inner join det_documento dd on (ddt.documento_id = dd.documento_id And ddt.nro_linea_doc = dd.nro_linea)   
												 inner join rl_det_doc_trans_posicion rl on (rl.doc_trans_id_egr = ddt.doc_trans_id And rl.nro_linea_trans_egr = ddt.nro_linea_trans)  
												 inner join documento d on (d.documento_id  =dd.documento_id)                                                                          
												 inner join categoria_logica cl on (rl.cliente_id = cl.cliente_id And rl.cat_log_id = cl.cat_log_id)                                   
												 INNER JOIN (SELECT DOCUMENTO_ID, NRO_LINEA, SUM(CANT_CONFIRMADA) AS CANT_CONFIRMADA     
															 FROM PICKING GROUP BY DOCUMENTO_ID, NRO_LINEA) P                                  
													 ON (DD.DOCUMENTO_ID = P.DOCUMENTO_ID AND DD.NRO_LINEA = P.NRO_LINEA)                  
										  WHERE d.status = 'D30'                                                                     
												 and nave_actual in (select nave_id from nave where nave_cod = 'PRE-EGRESO')             
												 and cl.categ_stock_id = 'TRAN_EGR'                                                      
										 group by rl.nave_anterior,rl.posicion_anterior,dd.cliente_id,dd.producto_id, dd.nro_lote, dd.nro_partida
						) XX

					GROUP BY  XX.nave_id, XX.posicion_id, XX.cliente_id, XX.producto_id,  XX.nro_lote,	 XX.nro_partida							


				)
				X ON (DI.PRODUCTO_ID = X.PRODUCTO_ID AND DI.CLIENTE_ID = X.CLIENTE_ID
						AND (DI.NAVE_ID = X.NAVE_ID OR DI.POSICION_ID=X.POSICION_ID)
						AND (DI.NRO_LOTE IS NULL OR DI.NRO_LOTE = X.NRO_LOTE)
						AND (DI.NRO_PARTIDA IS NULL OR DI.NRO_PARTIDA = X.NRO_PARTIDA))
		WHERE (@P_MARBETE = 0 OR DI.MARBETE = @P_MARBETE)
				AND DI.INVENTARIO_ID = @P_INVENTARIO_ID
				AND ((@V_NUM_STOCK = 1 AND DI.CANT_STOCK_CONT_1 IS NULL)
					OR(@V_NUM_STOCK = 2 AND DI.CANT_STOCK_CONT_2 IS NULL)
					OR(@V_NUM_STOCK = 3 AND DI.CANT_STOCK_CONT_3 IS NULL))




		--ACA COLOCO O (CERO) SI LA CONSULTA ANTERIOR NO ENCONTRO NADA EN LA POSICION, PARA QUE NO QUEDE NULL
		UPDATE DET_INVENTARIO SET CANT_STOCK_CONT_1 = CASE WHEN @V_NUM_STOCK = 1 THEN 0 ELSE CANT_STOCK_CONT_1 END,
								  CANT_STOCK_CONT_2 = CASE WHEN @V_NUM_STOCK = 2 THEN 0 ELSE CANT_STOCK_CONT_2 END,
								  CANT_STOCK_CONT_3 = CASE WHEN @V_NUM_STOCK = 3 THEN 0 ELSE CANT_STOCK_CONT_3 END 
		WHERE (@P_MARBETE = 0 OR MARBETE = @P_MARBETE)
		AND INVENTARIO_ID = @P_INVENTARIO_ID
		AND MODO_INGRESO = 'S'
		AND ((@V_NUM_STOCK = 1 AND CANT_STOCK_CONT_1 IS NULL)
			OR(@V_NUM_STOCK = 2 AND CANT_STOCK_CONT_2 IS NULL)
			OR(@V_NUM_STOCK = 3 AND CANT_STOCK_CONT_3 IS NULL))



	
	END TRY
	BEGIN CATCH
		EXEC USP_RETHROWERROR
	END CATCH
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#ACT_STOCK_GRAL]
@P_DOC_TRANS_ID AS SQL_VARIANT OUTPUT
AS
BEGIN
	BEGIN TRY
		DECLARE @V_DOC_TRANS_ID AS NUMERIC(20)
		DECLARE @V_INVENTARIO_ID AS NUMERIC(20)

		SET @V_DOC_TRANS_ID = CONVERT(NUMERIC(20), @P_DOC_TRANS_ID)


		SET @V_INVENTARIO_ID = 0
		SELECT @V_INVENTARIO_ID = INVENTARIO_ID FROM INVENTARIO WHERE DOC_TRANS_ID = @V_DOC_TRANS_ID

		IF @V_INVENTARIO_ID = 0
			RAISERROR('No se pudo recuperar el nro INVENTARIO_ID.',15,1)

		EXEC FUNCIONES_INVENTARIO_API#ACT_STOCK @V_INVENTARIO_ID, 0
	
	END TRY
	BEGIN CATCH
		EXEC USP_RETHROWERROR
	END CATCH
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER    Procedure [dbo].[Funciones_Loggin_Api#Registra_Usuario_Loggin]
@Usuario as varchar(30)
As
Begin

	Declare @Session_Id				as varchar(100)
	Declare @Terminal				as varchar(100)
	Declare @emplazamiento_default	as varchar(15)
	Declare @deposito_default		as varchar(15)
	Declare	@RolId					as varchar(5)


	SELECT 	@Session_Id=USER_NAME(),
	       		@Terminal=HOST_NAME()

	SELECT 	@emplazamiento_default=emplazamiento_default,
	       	@deposito_default=deposito_default
	FROM   	sys_perfil_usuario
	WHERE  	usuario_id =@Usuario

	SELECT 	@RolId=rol_id
	FROM   	sys_usuario 
	WHERE  	usuario_id =@Usuario

	INSERT INTO #temp_usuario_loggin 
	            (usuario_id,
	             terminal,
	             fecha_loggin,
	             session_id,
	             rol_id,
	             emplazamiento_default,
	             deposito_default)
	Values
	            ( @Usuario
	             ,@Terminal
	             ,GETDATE()
	             ,@Session_Id 
	             ,@RolId
	             ,@emplazamiento_default
	             ,@deposito_default)
	set @usuario = rtrim(ltrim(upper(@Usuario)))

	Exec dbo.Registra_Sys_Session_Login @Usuario, @Terminal , 1

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER        Procedure [dbo].[Funciones_Inventario_Api#Ajustes_masivos]
@P_INVENTARIO_ID 		as Numeric(20,0) OUTPUT,
@P_RESULTADO			AS NUMERIC(20,0) OUTPUT
As
Begin

declare @cur cursor
declare @CUR_INT cursor
DECLARE @V_CLIENTE_ID VARCHAR(100)
DECLARE @V_PRODUCTO_ID VARCHAR(100)
DECLARE @V_CANTIDAD NUMERIC(20,5)
DECLARE @V_NAVE_ID NUMERIC(20)
DECLARE @V_POSICION_ID NUMERIC(20)
DECLARE @V_MARBETE NUMERIC(20)
DECLARE @V_NRO_LOTE AS VARCHAR(100)
DECLARE @V_NRO_PARTIDA AS VARCHAR(100)
--PARA DBO.FUNCIONES_INVENTARIO_API#REALIZAR_AJUSTE
DECLARE @V2_CAT_LOG_ID VARCHAR(50)
DECLARE @V2_FEC_VTO VARCHAR(50)
DECLARE @V2_NRO_LOTE VARCHAR(50)
DECLARE @V2_NRO_PARTIDA VARCHAR(50) 		
DECLARE @V2_NRO_DESPACHO VARCHAR(50) 		
DECLARE @V2_NRO_BULTO VARCHAR(50) 		
DECLARE @V2_NRO_SERIE VARCHAR(50) 		
DECLARE @V2_EST_MERC_ID VARCHAR(50)		
DECLARE @V2_PROP1 VARCHAR(100)		
DECLARE @V2_PROP2 VARCHAR(100) 	
DECLARE @V2_PROP3 VARCHAR(100)		
DECLARE @V2_PESO NUMERIC(20,5)	
DECLARE @V2_VOLUMEN NUMERIC(20,5)	
DECLARE @V2_UNIDAD_ID VARCHAR(5)		
DECLARE @V2_UNIDAD_PESO VARCHAR(5)		
DECLARE @V2_UNIDAD_VOLUMEN VARCHAR(5) 		
DECLARE @V2_MONEDA_ID VARCHAR(20)		
DECLARE @V2_COSTO NUMERIC(10,3)	
DECLARE @V2_CANTIDAD NUMERIC(20,5)	
DECLARE @V2_SIGNO VARCHAR(3)
DECLARE @V2_CANT_AJU_ACT NUMERIC(20,4)
--FIN PARA DBO.FUNCIONES_INVENTARIO_API#REALIZAR_AJUSTE		
DECLARE @V_CANT_AUX NUMERIC(20,4)




BEGIN TRY
	SET XACT_ABORT ON
	------primero hago los ajustes
	
	SELECT *  INTO #INV_T FROM DET_INVENTARIO WHERE INVENTARIO_ID=@P_INVENTARIO_ID;
	
	UPDATE	DET_INVENTARIO SET MODO_INGRESO=CASE WHEN P.SERIE_ING=1 THEN 'M' ELSE D.MODO_INGRESO END
	FROM	DET_INVENTARIO D INNER JOIN PRODUCTO P
			ON(D.CLIENTE_ID=P.CLIENTE_ID AND D.PRODUCTO_ID=P.PRODUCTO_ID)
	WHERE	INVENTARIO_ID=@P_INVENTARIO_ID;
	
	--BEGIN TRAN EXTERNA
	Set @cur = Cursor For
		Select	A.CLIENTE_ID, A.PRODUCTO_ID, A.MARBETE, A.CANT_AJU, A.NAVE_ID, A.POSICION_ID, I.NRO_LOTE, I.NRO_PARTIDA  
		FROM	DET_INVENTARIO_AJU A
				INNER JOIN DET_INVENTARIO I ON (I.INVENTARIO_ID = A.INVENTARIO_ID AND I.MARBETE = A.MARBETE)
		WHERE	(A.PROCESADO = 'N' OR A.PROCESADO IS NULL) 
				AND I.MODO_INGRESO = 'S'
				AND A.INVENTARIO_ID = @P_INVENTARIO_ID AND A.CANT_AJU <> 0 
		ORDER BY 
				A.CLIENTE_ID, A.POSICION_ID, I.NRO_LOTE DESC, I.NRO_PARTIDA DESC FOR UPDATE
			
	Open @cur
	Fetch Next From @cur into @V_CLIENTE_ID, @V_PRODUCTO_ID, @V_MARBETE, @V_CANTIDAD, @V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA


	IF OBJECT_ID('tempdb.dbo.#temp_usuario_loggin','U') IS NULL
		BEGIN
			--================================================================
			CREATE TABLE #temp_usuario_loggin (
				usuario_id            			VARCHAR(20)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				terminal              			VARCHAR(100)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				fecha_loggin          		DATETIME     ,
				session_id            			VARCHAR(60)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				rol_id                			VARCHAR(5)  COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
				emplazamiento_default 	VARCHAR(15)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
				deposito_default      		VARCHAR(15)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
			)
			exec FUNCIONES_LOGGIN_API#REGISTRA_USUARIO_LOGGIN 'USER'
			--================================================================
		END


	While @@Fetch_Status=0
	Begin
				
		--PRINT '----------------------------------------------------------'
		--print @V_MARBETE

		IF @V_CANTIDAD < 0 
			BEGIN
				SET @V2_SIGNO = '-' 
				SET @V_CANT_AUX = @V_CANTIDAD * (-1)
			END 
		ELSE 
			BEGIN
				SET @V2_SIGNO = '+'
				SET @V_CANT_AUX = @V_CANTIDAD
			END

		

		Set @CUR_INT = Cursor For SELECT rl.cat_log_id as CategLogID 
										  ,sum(ISNULL(rl.cantidad,0)) AS Cantidad 
										  ,dd.nro_serie 
										  ,dd.Nro_lote 
										  ,dd.Fecha_vencimiento 
										  ,dd.Nro_Despacho 
										  ,dd.Nro_bulto 
										  ,dd.Nro_Partida 
										  ,rl.est_merc_id 
										  ,dd.prop1 
										  ,dd.prop2 
										  ,dd.prop3 
										  ,DD.PESO
										  ,DD.VOLUMEN
										  ,dd.unidad_id 
										  ,DD.UNIDAD_PESO
										  ,DD.UNIDAD_VOLUMEN
										  ,dd.moneda_id 
										  ,dd.costo 
									FROM  rl_det_doc_trans_posicion rl (NoLock)
										  LEFT JOIN nave n (NoLock)            on rl.nave_actual = n.nave_id 
										  LEFT JOIN posicion p  (NoLock)       on rl.posicion_actual = p.posicion_id 
										  LEFT JOIN nave n2   (NoLock)         on p.nave_id = n2.nave_id 
										  LEFT JOIN calle_nave caln (NoLock)   on p.calle_id = caln.calle_id 
										  LEFT JOIN columna_nave coln (NoLock) on p.columna_id = coln.columna_id
										  LEFT JOIN nivel_nave nn  (NoLock)    on p.nivel_id = nn.nivel_id
										  ,det_documento dd (NoLock) 
										  inner join documento d (NoLock) on(dd.documento_id=d.documento_id) 
										  left join sucursal s on(s.sucursal_id=d.sucursal_origen and s.cliente_id=d.cliente_id)
										  ,det_documento_transaccion ddt (NoLock)
										  ,cliente c (NoLock)
										  ,producto prod (NoLock)
										  ,categoria_logica cl (NoLock)
										  ,documento_transaccion dt (NoLock)
									WHERE rl.doc_trans_id = ddt.doc_trans_id 
										  AND rl.nro_linea_trans = ddt.nro_linea_trans 
										  and ddt.documento_id = dd.documento_id 
										  and ddt.doc_trans_id = dt.doc_trans_id 
										  AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA 
										  AND DD.CLIENTE_ID = C.CLIENTE_ID 
										  AND DD.PRODUCTO_ID = PROD.PRODUCTO_ID 
										  AND DD.CLIENTE_ID = PROD.CLIENTE_ID 
										  AND RL.CAT_LOG_ID = CL.CAT_LOG_ID 
										  AND RL.CLIENTE_ID = CL.CLIENTE_ID 
										  AND RL.DISPONIBLE= '1'
										  AND ISNULL(p.pos_lockeada,'0')='0'
										  AND DD.CLIENTE_ID = @V_CLIENTE_ID
										  AND DD.PRODUCTO_ID = @V_PRODUCTO_ID
										  AND (@V_NAVE_ID IS NULL OR RL.NAVE_ACTUAL = @V_NAVE_ID)
										  AND (@V_POSICION_ID IS NULL OR RL.POSICION_ACTUAL = @V_POSICION_ID)
									 GROUP BY rl.cat_log_id
										  ,dd.nro_serie 
										  ,dd.Nro_lote 
										  ,dd.Fecha_vencimiento 
										  ,dd.Nro_Despacho 
										  ,dd.Nro_bulto 
										  ,dd.Nro_Partida 
										  ,rl.est_merc_id 
										  ,dd.prop1 
										  ,dd.prop2 
										  ,dd.prop3 
										  ,DD.PESO
										  ,DD.VOLUMEN
										  ,dd.unidad_id 
										  ,DD.UNIDAD_PESO
										  ,DD.UNIDAD_VOLUMEN
										  ,dd.moneda_id 
										  ,dd.costo 



		BEGIN TRAN 
	
		OPEN @CUR_INT
		FETCH NEXT FROM @CUR_INT INTO @V2_CAT_LOG_ID,@V2_CANTIDAD,@V2_NRO_SERIE,@V2_NRO_LOTE,@V2_FEC_VTO,@V2_NRO_DESPACHO,
										@V2_NRO_BULTO,@V2_NRO_PARTIDA,@V2_EST_MERC_ID,@V2_PROP1,@V2_PROP2,@V2_PROP3,@V2_PESO,
										@V2_VOLUMEN,@V2_UNIDAD_ID,@V2_UNIDAD_PESO,@V2_UNIDAD_VOLUMEN,@V2_MONEDA_ID,@V2_COSTO

		WHILE @@FETCH_STATUS =0
		BEGIN
			
			IF @V2_SIGNO = '-'
				BEGIN
					IF @V2_CANTIDAD >= @V_CANT_AUX
						BEGIN
							SET @V2_CANT_AJU_ACT = @V_CANT_AUX
							SET @V_CANT_AUX =  0
						END
					ELSE
						BEGIN
							SET @V2_CANT_AJU_ACT = @V2_CANTIDAD
							SET @V_CANT_AUX = @V_CANT_AUX - @V2_CANTIDAD 
						END
				END
			ELSE
				BEGIN
					SET @V2_CANT_AJU_ACT = @V_CANT_AUX
					SET @V_CANT_AUX =  0
				END

			BEGIN TRY

				EXEC DBO.FUNCIONES_INVENTARIO_API#REALIZAR_AJUSTE_INV @V_CLIENTE_ID, @V_PRODUCTO_ID, 
								@V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA,
								@V2_CANT_AJU_ACT, @V2_SIGNO

			END TRY
			BEGIN CATCH
				SET @V_CANT_AUX = 1
				BREAK
			END CATCH
			
			IF @V_CANT_AUX = 0
				BREAK

			FETCH NEXT FROM @CUR_INT INTO @V2_CAT_LOG_ID,@V2_CANTIDAD,@V2_NRO_SERIE,@V2_NRO_LOTE,@V2_FEC_VTO,@V2_NRO_DESPACHO,
										@V2_NRO_BULTO,@V2_NRO_PARTIDA,@V2_EST_MERC_ID,@V2_PROP1,@V2_PROP2,@V2_PROP3,@V2_PESO,
										@V2_VOLUMEN,@V2_UNIDAD_ID,@V2_UNIDAD_PESO,@V2_UNIDAD_VOLUMEN,@V2_MONEDA_ID,@V2_COSTO
			

		END --END WHILE @CUR_INT

		CLOSE @CUR_INT
		DEALLOCATE @CUR_INT
		
		IF @V_CANT_AUX = 0
			BEGIN

				--HACER UPDATE DEL REGISTRO DEL CURSOR EXTERNO EN UN CAMPO NUEVO , PARA MARCAR QUE SE COMPLETO EL AJUSTE
				UPDATE DET_INVENTARIO_AJU SET PROCESADO = 'S' WHERE CURRENT OF @cur

				
				
				COMMIT TRAN 
			END
		ELSE
			BEGIN

				ROLLBACK TRAN 
				UPDATE DET_INVENTARIO_AJU SET PROCESADO = 'E' WHERE CURRENT OF @cur

			END

		Fetch Next From @cur into @V_CLIENTE_ID, @V_PRODUCTO_ID, @V_MARBETE, @V_CANTIDAD, @V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA
		
		
	End	--End While @cur.

	CLOSE @cur
	DEALLOCATE @cur
	

	PRINT 'FINALIZADO LOS AJUSTES'	
   
	------ahora creo los documentos por la mercadería que no esta en sistema-----------------------------------------------------------




    DECLARE @V_Cliente_Id_EXT VARCHAR(15)
	DECLARE @CUR_EXTERNA AS CURSOR
	DECLARE @PROCESO AS VARCHAR(1) --1 = PRIMER PROCESO (SIN SERIES), 2 = CON SERIES

	--PARA DOCUMENTO ING
		DECLARE @P_Documento_Id numeric
		DECLARE @P_Cliente_Id varchar(15)
		DECLARE @P_Tipo_Comprobante_Id varchar(5)
		DECLARE @P_Tipo_Operacion_Id varchar(5)
		DECLARE @P_Det_Tipo_Operacion_Id varchar(5)
		DECLARE @P_Cpte_Prefijo varchar(6)
		DECLARE @P_Cpte_Numero varchar(20)
		DECLARE @P_Fecha_Cpte varchar(20)
		DECLARE @P_Fecha_Pedida_Ent varchar(20)
		DECLARE @P_Sucursal_Origen varchar(20)
		DECLARE @P_Sucursal_Destino varchar(20)
		DECLARE @P_Anulado varchar(1)
		DECLARE @P_Motivo_Anulacion varchar(15)
		DECLARE @P_Peso_Total numeric
		DECLARE @P_Unidad_Peso varchar(5)
		DECLARE @P_Volumen_Total numeric
		DECLARE @P_Unidad_Volumen varchar(5)
		DECLARE @P_Total_Bultos numeric
		DECLARE @P_Valor_Declarado numeric
		DECLARE @P_Orden_De_Compra varchar(20)
		DECLARE @P_Cant_Items numeric
		DECLARE @P_Observaciones varchar(200)
		DECLARE @P_Status varchar(3)
		DECLARE @P_NroRemito varchar(30)
		DECLARE @P_Fecha_Alta_Gtw varchar(20)
		DECLARE @P_Fecha_Fin_Gtw varchar(20)
		DECLARE @P_Personal_Id varchar(20)
		DECLARE @P_Transporte_Id varchar(20)
		DECLARE @P_Nro_Despacho_Importacion varchar(30)
		DECLARE @P_Alto numeric
		DECLARE @P_Ancho numeric
		DECLARE @P_Largo numeric
		DECLARE @P_Unidad_Medida varchar(5)
		DECLARE @P_Grupo_Picking varchar(50)
		DECLARE @P_Prioridad_Picking numeric
		
--PARA DOCUMENTO_TRANSACCION
		DECLARE @P_Completado varchar(1)
		DECLARE @P_Transaccion_Id varchar(15)
		DECLARE @P_Estacion_Actual varchar(15)
		DECLARE @P_Est_Mov_Actual varchar(20)
		DECLARE @P_Orden_Id numeric
		DECLARE @P_It_Mover varchar(1)
		DECLARE @P_Orden_Estacion numeric
		--DECLARE @P_Tipo_Operacion_Id varchar(5)
		DECLARE @P_Tr_Pos_Completa varchar(1)
		DECLARE @P_Tr_Activo varchar(1)
		DECLARE @P_Usuario_Id varchar(20)
		DECLARE @P_Terminal varchar(20)
		--DECLARE @P_Fecha_Alta_Gtw datetime
		DECLARE @P_Tr_Activo_Id varchar(10)
		DECLARE @P_Session_Id varchar(60)
		DECLARE @P_Fecha_Cambio_Tr datetime
		--DECLARE @P_Fecha_Fin_Gtw datetime
		DECLARE @P_Doc_Trans_Id numeric
		
--PARA DET_DOCUMENTO
		DECLARE @P_Nro_Linea numeric
		DECLARE @P_Cantidad numeric
		DECLARE @P_Nro_Serie varchar(50)
		DECLARE @P_Nro_Serie_Padre varchar(50)
		DECLARE @P_Est_Merc_Id varchar(15)
		DECLARE @P_Cat_Log_Id varchar(50)
		DECLARE @P_Nro_Bulto varchar(50)
		DECLARE @P_Descripcion varchar(200)
		DECLARE @P_Nro_Lote varchar(50)
		DECLARE @P_Fecha_Vencimiento datetime
		DECLARE @P_Nro_Despacho varchar(50)
		DECLARE @P_Nro_Partida varchar(50)
		DECLARE @P_Unidad_Id varchar(5)
		DECLARE @P_Peso numeric
		--DECLARE @P_Unidad_Peso varchar(5)
		DECLARE @P_Volumen numeric
		--DECLARE @P_Unidad_Volumen varchar(5)
		DECLARE @P_Busc_Individual varchar(1)
		DECLARE @P_Tie_In varchar(1)
		DECLARE @P_Nro_Tie_In_Padre varchar(100)
		DECLARE @P_Nro_Tie_In varchar(100)
		DECLARE @P_Item_Ok varchar(1)
		DECLARE @P_Moneda_Id varchar(20)
		DECLARE @P_Costo numeric
		DECLARE @P_Cat_Log_Id_Final varchar(50)
		DECLARE @P_Prop1 varchar(100)
		DECLARE @P_Prop2 varchar(100)
		DECLARE @P_Prop3 varchar(100)
		--DECLARE @P_Largo numeric
		--DECLARE @P_Alto numeric
		--DECLARE @P_Ancho numeric
		DECLARE @P_Volumen_Unitario varchar(1)
		DECLARE @P_Peso_Unitario varchar(1)
		DECLARE @P_Cant_Solicitada numeric
		--variables para el det_documento_transaccion
		--DECLARE @P_Doc_Trans_Id numeric
		DECLARE @P_Nro_Linea_Trans numeric
		--DECLARE @P_Documento_Id numeric
		DECLARE @P_Nro_Linea_Doc numeric
		DECLARE @P_Motivo_Id varchar(15)
		--DECLARE @P_Est_Merc_Id varchar(15)
		--DECLARE @P_Cliente_Id varchar(15)
		--DECLARE @P_Cat_Log_Id varchar(50)
		--DECLARE @P_Item_Ok varchar(1)
		DECLARE @P_Movimiento_Pendiente varchar(1)
		declare @RL_ID numeric(20)
		DECLARE @FL_CONTENEDORA VARCHAR(1)
		DECLARE @SEC_CONTENEDORA   int  


	
	SET @CUR_EXTERNA = CURSOR FOR
		SELECT distinct A.CLIENTE_ID
		FROM DET_INVENTARIO_AJU A
			INNER JOIN DET_INVENTARIO I ON (I.INVENTARIO_ID = A.INVENTARIO_ID AND I.MARBETE = A.MARBETE)
		WHERE (A.PROCESADO = 'N' OR A.PROCESADO IS NULL) 
			AND I.MODO_INGRESO = 'M'
			AND A.CANT_AJU > 0
			AND A.INVENTARIO_ID = @P_INVENTARIO_ID 
		order by 1


	BEGIN TRAN

	open @CUR_EXTERNA
	FETCH NEXT FROM @CUR_EXTERNA INTO @V_Cliente_Id_EXT
    WHILE @@FETCH_STATUS = 0 
	BEGIN
		

		SET @PROCESO = '1' --EMPIEZO A CREAR EL DOCUMENTO DE INGRESO PARA LOS PRODUCTOS SIN SERIES


		INICIO_DOC:
		
		set @P_Documento_Id = null
		
		IF @PROCESO = '1' --SIN SERIES
		BEGIN
		
			Set @cur = Cursor For
				Select A.CLIENTE_ID, A.PRODUCTO_ID, A.MARBETE, A.CANT_AJU, A.NAVE_ID, A.POSICION_ID, I.NRO_LOTE, I.NRO_PARTIDA
				FROM DET_INVENTARIO_AJU A
					INNER JOIN DET_INVENTARIO I ON (I.INVENTARIO_ID = A.INVENTARIO_ID AND I.MARBETE = A.MARBETE)
					INNER JOIN PRODUCTO P ON (P.CLIENTE_ID = I.CLIENTE_ID AND P.PRODUCTO_ID = I.PRODUCTO_ID)
				WHERE (A.PROCESADO = 'N' OR A.PROCESADO IS NULL) 
					AND I.MODO_INGRESO = 'M'
					AND A.CANT_AJU > 0
					AND A.INVENTARIO_ID = @P_INVENTARIO_ID 
					AND A.CLIENTE_ID = @V_Cliente_Id_EXT
					AND (P.SERIE_ING IS NULL OR P.SERIE_ING <> '1')
		END
		ELSE --CON SERIES
		BEGIN
		
			Set @cur = Cursor For
				Select A.CLIENTE_ID, A.PRODUCTO_ID, A.MARBETE, A.CANT_AJU, A.NAVE_ID, A.POSICION_ID, I.NRO_LOTE, I.NRO_PARTIDA
				FROM DET_INVENTARIO_AJU A
					INNER JOIN DET_INVENTARIO I ON (I.INVENTARIO_ID = A.INVENTARIO_ID AND I.MARBETE = A.MARBETE)
					INNER JOIN PRODUCTO P ON (P.CLIENTE_ID = I.CLIENTE_ID AND P.PRODUCTO_ID = I.PRODUCTO_ID)
				WHERE (A.PROCESADO = 'N' OR A.PROCESADO IS NULL) 
					AND I.MODO_INGRESO = 'M'
					AND A.CANT_AJU > 0
					AND A.INVENTARIO_ID = @P_INVENTARIO_ID 
					AND A.CLIENTE_ID = @V_Cliente_Id_EXT
					AND P.SERIE_ING = '1'
		END
		


				
		Open @cur
		Fetch Next From @cur into  @V_CLIENTE_ID, @V_PRODUCTO_ID, @V_MARBETE, @V_CANTIDAD, @V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA

		if @@fetch_status = 0 
			begin
				--CREAR DOS DOCUMENTO POR CLIENTE, UNO PARA LOS PRODUCTOS SIN SERIALIZACION Y OTRO PARA LOS PRODUCTOS SERIELIZADOS
				--EL DOCUMENTO POR LOS SERIALIZADOS QUEDARA EN D30 PARA QUE SE PUEDAN CARGAR LAS SERIES DE LA FORMA HABITUAL

				--CREA EL DOCUMENTO
				PRINT 'COMIENZA EL AJUSTE POR DOC INGRESO'
				

				SET @P_Cliente_Id = @V_CLIENTE_ID
				SET @P_Tipo_Comprobante_Id = 'IM'
				SET @P_Tipo_Operacion_Id = 'ING'
				SET @P_Det_Tipo_Operacion_Id = 'MAN'
				IF @PROCESO = '1'
					SET @P_Cpte_Prefijo='0001'
				ELSE
					SET @P_Cpte_Prefijo='0002'
					
				SET @P_Cpte_Numero=replicate('0',8 - len(convert(varchar(100), @P_INVENTARIO_ID, 1))) + convert(varchar(100), @P_INVENTARIO_ID)
				SET @P_Fecha_Cpte = CONVERT(datetime,CONVERT(VARCHAR,GETDATE(),101),101)--PARA DEVOLVER FECHA SIN HORAS Y MINUTOS
				SET @P_Observaciones = 'AJUSTE POR INVENTARIO NRO: ' + CAST(@P_INVENTARIO_ID AS VARCHAR)
				SET @P_Status = 'D40'
				SET @P_Unidad_Peso = NULL
		
				
				EXECUTE [dbo].[Documento_Api#InsertRecord] 
							   @P_Documento_Id OUTPUT
							  ,@P_Cliente_Id
							  ,@P_Tipo_Comprobante_Id
							  ,@P_Tipo_Operacion_Id
							  ,@P_Det_Tipo_Operacion_Id
							  ,@P_Cpte_Prefijo
							  ,@P_Cpte_Numero
							  ,@P_Fecha_Cpte
							  ,@P_Fecha_Pedida_Ent
							  ,@P_Sucursal_Origen
							  ,@P_Sucursal_Destino
							  ,@P_Anulado
							  ,@P_Motivo_Anulacion
							  ,@P_Peso_Total
							  ,@P_Unidad_Peso
							  ,@P_Volumen_Total
							  ,@P_Unidad_Volumen
							  ,@P_Total_Bultos
							  ,@P_Valor_Declarado
							  ,@P_Orden_De_Compra
							  ,@P_Cant_Items
							  ,@P_Observaciones
							  ,@P_Status
							  ,@P_NroRemito
							  ,@P_Fecha_Alta_Gtw
							  ,@P_Fecha_Fin_Gtw
							  ,@P_Personal_Id
							  ,@P_Transporte_Id
							  ,@P_Nro_Despacho_Importacion
							  ,@P_Alto
							  ,@P_Ancho
							  ,@P_Largo
							  ,@P_Unidad_Medida
							  ,@P_Grupo_Picking
							  ,@P_Prioridad_Picking
				

				--CREA EL DOCUMENTO_TRANSACCION

				


				IF @PROCESO = '1'  --PARA DOC_INGRESO SIN SERIES
				BEGIN
					SET @P_Completado = '0'
					SET @P_Transaccion_Id = 'ING_ABAST_F'
					SET @P_Status = 'T40'
					SET @P_Tipo_Operacion_Id = 'ING'
					SET @P_Tr_Activo = '0'
				end
				else
				begin -- PARA DOC_INGRESO CON SERIES
					SET @P_Completado = '0'
					SET @P_Transaccion_Id = 'ING_ABAST_F'
					SET @P_Status = 'T10'
					SET @P_Tipo_Operacion_Id = 'ING'
					SET @P_Tr_Activo = '0'
					SET @P_Estacion_Actual  = 'RECEP_ABAST'
					SET @P_Est_Mov_Actual = 'A'
					SET @P_It_Mover = '0'
					SET @P_Orden_Estacion = '1'
				end
				
				
				
				
				
				EXEC [dbo].[Documento_Transaccion_Api#InsertRecord] 
						@P_Completado
						,@P_Observaciones
						,@P_Transaccion_Id
						,@P_Estacion_Actual
						,@P_Status
						,@P_Est_Mov_Actual
						,@P_Orden_Id
						,@P_It_Mover
						,@P_Orden_Estacion
						,@P_Tipo_Operacion_Id
						,@P_Tr_Pos_Completa
						,@P_Tr_Activo
						,@P_Usuario_Id
						,@P_Terminal
						,@P_Fecha_Alta_Gtw
						,@P_Tr_Activo_Id
						,@P_Session_Id
						,@P_Fecha_Cambio_Tr
						,@P_Fecha_Fin_Gtw
						,@P_Doc_Trans_Id OUTPUT
				
				
			end


			
			
			
		While @@Fetch_Status=0
		Begin	

			--Creo el detalle de det_documento


			
			set @P_Cantidad = @V_CANTIDAD
			SET @P_Cant_Solicitada = @V_CANTIDAD
			SET @P_Cat_Log_Id = 'DISPONIBLE'
			SET @P_Cat_Log_Id_Final = 'DISPONIBLE'
			set @P_Volumen_Unitario = '1'
			set @P_Peso_Unitario = '1'
			SET @P_Tie_In = '0'
            SET @P_Nro_Lote = @V_NRO_LOTE
			SET @P_Nro_Partida = @V_NRO_PARTIDA
			select @P_Descripcion = descripcion, @P_Unidad_Id = unidad_id, @P_Unidad_Peso = unidad_peso, @P_Unidad_Volumen = unidad_volumen 
				from producto
				where producto_id =@V_PRODUCTO_ID
				
			
			
			SELECT @FL_CONTENEDORA = FLG_CONTENEDORA FROM PRODUCTO 
			WHERE CLIENTE_ID =@P_Cliente_Id AND PRODUCTO_ID = @V_PRODUCTO_ID
			
			
			IF @FL_CONTENEDORA = '1' 
			BEGIN
				EXEC GET_VALUE_FOR_SEQUENCE 'CONTENEDORA', @SEC_CONTENEDORA OUTPUT
				set @P_Nro_Bulto = @SEC_CONTENEDORA
				
			END
			
						
			
		
			EXECUTE [dbo].[Det_Documento_Api#InsertRecord] 
			   @P_Documento_Id
			  ,@P_Nro_Linea
			  ,@P_Cliente_Id
			  ,@V_PRODUCTO_ID
			  ,@P_Cantidad
			  ,@P_Nro_Serie
			  ,@P_Nro_Serie_Padre
			  ,@P_Est_Merc_Id
			  ,@P_Cat_Log_Id
			  ,@P_Nro_Bulto
			  ,@P_Descripcion
			  ,@P_Nro_Lote
			  ,@P_Fecha_Vencimiento
			  ,@P_Nro_Despacho
			  ,@P_Nro_Partida
			  ,@P_Unidad_Id
			  ,@P_Peso
			  ,@P_Unidad_Peso
			  ,@P_Volumen
			  ,@P_Unidad_Volumen
			  ,@P_Busc_Individual
			  ,@P_Tie_In
			  ,@P_Nro_Tie_In_Padre
			  ,@P_Nro_Tie_In
			  ,@P_Item_Ok
			  ,@P_Moneda_Id
			  ,@P_Costo
			  ,@P_Cat_Log_Id_Final
			  ,@P_Prop1
			  ,@P_Prop2
			  ,@P_Prop3
			  ,@P_Largo
			  ,@P_Alto
			  ,@P_Ancho
			  ,@P_Volumen_Unitario
			  ,@P_Peso_Unitario
			  ,@P_Cant_Solicitada		

			
		

			SELECT @P_Nro_Linea_Trans = max(NRO_LINEA) FROM DET_DOCUMENTO WHERE documento_id = @P_Documento_Id
			set @P_Nro_Linea_Doc = @P_Nro_Linea_Trans
			set @P_Item_Ok= '0'
			set @P_Movimiento_Pendiente = '0'
			
			exec [Det_Documento_Transaccion_Api#InsertRecord] 
				   @P_Doc_Trans_Id
				  ,@P_Nro_Linea_Trans
				  ,@P_Documento_Id
				  ,@P_Nro_Linea_Doc
				  ,@P_Motivo_Id
				  ,@P_Est_Merc_Id
				  ,@P_Cliente_Id
				  ,@P_Cat_Log_Id
				  ,@P_Item_Ok
				  ,@P_Movimiento_Pendiente
			
			Insert Into RL_DET_DOC_TRANS_POSICION (
						DOC_TRANS_ID,				NRO_LINEA_TRANS,
						POSICION_ANTERIOR,		POSICION_ACTUAL,
						CANTIDAD,					TIPO_MOVIMIENTO_ID, --ver TIPO_MOVIMIENTO_ID
						ULTIMA_ESTACION,			ULTIMA_SECUENCIA,
						NAVE_ANTERIOR,				NAVE_ACTUAL,
						DOCUMENTO_ID,				NRO_LINEA,
						DISPONIBLE,					DOC_TRANS_ID_EGR,
						NRO_LINEA_TRANS_EGR,		DOC_TRANS_ID_TR,
						NRO_LINEA_TRANS_TR,		CLIENTE_ID,
						CAT_LOG_ID,				CAT_LOG_ID_FINAL,
						EST_MERC_ID)
			Values (@P_Doc_Trans_Id, @P_Nro_Linea_Trans, NULL, @V_POSICION_ID, @P_Cantidad, NULL, NULL, NULL, null,@V_NAVE_ID, @P_Documento_Id, @P_Nro_Linea_Doc, '1', null, null, null, null, @V_CLIENTE_ID, @P_Cat_Log_Id,@P_Cat_Log_Id,null)

			
			set @RL_ID = scope_identity()

			EXEC Funciones_Historicos_api#Actualizar_Historicos_X_Mov @RL_ID

			UPDATE DET_INVENTARIO_AJU SET PROCESADO = 'S' WHERE CURRENT OF @cur

			Fetch Next From @cur into  @V_CLIENTE_ID, @V_PRODUCTO_ID, @V_MARBETE, @V_CANTIDAD, @V_NAVE_ID, @V_POSICION_ID, @V_NRO_LOTE, @V_NRO_PARTIDA

		End	--End While @cur.

		
		IF @P_Documento_Id IS NOT NULL
			BEGIN 
		
				UPDATE DOCUMENTO SET STATUS = 'D20' WHERE DOCUMENTO_ID = @P_Documento_Id

				/*NO HAY QUE ASIGNAR TRATAMIENTO, PORQUE YA ESTA CREADO EL DOCUMENTO TRANSACCION
				-----------------------------------------------------------------------------------------------------------------
				--ASIGNO TRATAMIENTO...
				-----------------------------------------------------------------------------------------------------------------
				exec asigna_tratamiento#asigna_tratamiento_ing @P_Documento_Id
				*/

				Exec Am_Funciones_Estacion_Api#UpdateStatusDoc @P_Documento_Id, 'D30'
                Exec Am_Funciones_Estacion_Api#DocID_A_DocTrID @P_Documento_Id
			


				------------------------------------------------------------------------------------------------------------------------------------
				--Guardo en la tabla de auditoria
				-----------------------------------------------------------------------------------------------------------------
				exec dbo.AUDITORIA_HIST_INSERT_ING_AJU_INV @P_Documento_Id	
				
				IF @PROCESO = '1'
				BEGIN
					UPDATE DOCUMENTO SET STATUS = 'D40' WHERE DOCUMENTO_ID = @P_Documento_Id
				END
				
				
				
			END
		
		
		IF @PROCESO = '1' 
		BEGIN
			SET @PROCESO = 2 --PARA PROCESAR LOS PRDUCTOS CON SERIES.
			GOTO INICIO_DOC
		END
		


		CLOSE @cur
		DEALLOCATE @cur
		
		

		FETCH NEXT FROM @CUR_EXTERNA INTO @V_Cliente_Id_EXT
	END --WHILE @@FECTH_STATUS = 0 DE @CUR_EXTERNA
	CLOSE @CUR_EXTERNA
	DEALLOCATE @CUR_EXTERNA

	COMMIT TRAN

	UPDATE	DET_INVENTARIO SET MODO_INGRESO=T.MODO_INGRESO
	FROM	DET_INVENTARIO DD INNER JOIN #INV_T T
			ON(DD.INVENTARIO_ID=T.INVENTARIO_ID AND DD.MARBETE=T.MARBETE)
	WHERE	DD.INVENTARIO_ID = @P_INVENTARIO_ID

	SELECT @P_RESULTADO=COUNT(*) FROM DET_INVENTARIO_AJU WHERE INVENTARIO_ID = @P_INVENTARIO_ID AND PROCESADO <> 'S' AND CANT_AJU <> 0


	update inventario set aju_realizado = '1' , fecha_aju=getdate() where inventario_id = @P_INVENTARIO_ID
	


END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 ROLLBACK TRAN 
    EXEC usp_RethrowError;
END CATCH

end
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#GET_SELECT_CONTEO]
@P_DOC_TRANS_ID AS NUMERIC(20) OUTPUT
AS 
BEGIN
		DECLARE @V_NUM_STOCK AS TINYINT
		DECLARE @V_INVENTARIO_ID AS BIGINT 
		
		
		SELECT @V_NUM_STOCK = NRO_CONTEO, @V_INVENTARIO_ID = INVENTARIO_ID FROM INVENTARIO WHERE DOC_TRANS_ID = @P_DOC_TRANS_ID
				


		SELECT i.inventario_id AS inventario_id,  
		 dc.marbete AS marbete,               
		 i.nro_conteo AS nro_conteo,          
		 CASE WHEN @V_NUM_STOCK = 1 THEN (dc.conteo1)
		 	  WHEN @V_NUM_STOCK = 2 THEN (dc.conteo2)
		      WHEN @V_NUM_STOCK = 3 THEN (dc.conteo3)
		 END as conteo,
		 CASE WHEN @V_NUM_STOCK = 1 THEN (di.CANT_STOCK_CONT_1)
		 	  WHEN @V_NUM_STOCK = 2 THEN (di.CANT_STOCK_CONT_2)
		      WHEN @V_NUM_STOCK = 3 THEN (di.CANT_STOCK_CONT_3)
		 END AS saldo,
		 CASE WHEN @V_NUM_STOCK = 1 THEN dc.obsConteo1
			  WHEN @V_NUM_STOCK = 2 THEN dc.obsConteo2
			  WHEN @V_NUM_STOCK = 3 THEN dc.obsConteo3
		 END as obsconteo,
		 cli.razon_social AS cli_razon_social,
		 prod.producto_id AS producto_id,     
		 prod.codigo_producto AS cod_producto,
		 prod.descripcion AS prod_descripcion,
		 di.nro_lote,
		 di.nro_partida,
		 dc.posicion_id AS posicion_id,       
		 dep.descripcion AS deposito_cod,     
		 nave.nave_cod AS nave_cod,           
		 calle.calle_cod AS calle_cod,        
		 col.columna_cod AS columna_cod,      
		 nivel.nivel_cod AS nivel_cod,        
		 CASE WHEN @V_NUM_STOCK = 1 THEN (dc.conteo1-di.CANT_STOCK_CONT_1)
		 	  WHEN @V_NUM_STOCK = 2 THEN (dc.conteo2-di.CANT_STOCK_CONT_2)
		      WHEN @V_NUM_STOCK = 3 THEN (dc.conteo3-di.CANT_STOCK_CONT_3)
		 END AS diffConteo
		 ,di.modo_ingreso as modo             
		 ,i.lockgraba   
		 FROM                                 
		  inventario i                        
		 ,det_inventario di                   
		 ,det_conteo dc                       
		 ,producto prod                       
		 ,cliente cli                         
		 ,posicion pos                        
		  LEFT OUTER JOIN unidad_contenedora uc on pos.posicion_id=uc.posicion_id
		 ,nave                                
		 ,deposito dep                        
		 ,calle_nave calle                    
		 ,columna_nave col                    
		 ,nivel_nave nivel                    
		 WHERE                                
		 i.inventario_id=di.inventario_id     
		 AND di.inventario_id=dc.inventario_id
		 AND di.marbete=dc.marbete            
		 AND dc.cliente_id=prod.cliente_id    
		 AND dc.producto_id=prod.producto_id  
		 AND dc.cliente_id=cli.cliente_id     
		 AND dc.posicion_id=pos.posicion_id   
		 AND pos.nave_id=nave.nave_id         
		 AND nave.deposito_id=dep.deposito_id 
		 AND pos.nave_id=calle.nave_id        
		 AND pos.calle_id=calle.calle_id      
		 AND pos.nave_id=col.nave_id          
		 AND pos.calle_id=col.calle_id        
		 AND pos.columna_id=col.columna_id    
		 AND pos.nave_id=nivel.nave_id        
		 AND pos.calle_id=nivel.calle_id      
		 AND pos.columna_id=nivel.columna_id  
		 AND pos.nivel_id=nivel.nivel_id      
		 AND di.pos_lockeada=0                
		 AND di.inventario_id=@V_INVENTARIO_ID
		 AND (@V_NUM_STOCK = 1 
			OR (@V_NUM_STOCK = 2 AND (((dc.conteo1-di.CANT_STOCK_CONT_1) <> 0) OR DC.CONTEO1 IS NULL))
			OR (@V_NUM_STOCK = 3 AND (((dc.conteo2-di.CANT_STOCK_CONT_2) <> 0) OR (DC.CONTEO2 IS NULL AND di.modo_ingreso ='M'))))
		UNION 
		 SELECT                                 
		 i.inventario_id AS inventario_id,      
		 dc.marbete AS marbete,                 
		 i.nro_conteo AS nro_conteo,            
		 CASE WHEN @V_NUM_STOCK = 1 THEN (dc.conteo1)
		 	  WHEN @V_NUM_STOCK = 2 THEN (dc.conteo2)
		      WHEN @V_NUM_STOCK = 3 THEN (dc.conteo3)
		 END as conteo,
		 CASE WHEN @V_NUM_STOCK = 1 THEN (di.CANT_STOCK_CONT_1)
		 	  WHEN @V_NUM_STOCK = 2 THEN (di.CANT_STOCK_CONT_2)
		      WHEN @V_NUM_STOCK = 3 THEN (di.CANT_STOCK_CONT_3)
		 END AS saldo,
		 CASE WHEN @V_NUM_STOCK = 1 THEN dc.obsConteo1
			  WHEN @V_NUM_STOCK = 2 THEN dc.obsConteo2
			  WHEN @V_NUM_STOCK = 3 THEN dc.obsConteo3
		 END as obsconteo,
		 cli.razon_social AS cli_razon_social,  
		 prod.producto_id AS producto_id,       
		 prod.codigo_producto AS cod_producto,  
		 prod.descripcion AS prod_descripcion,  
		 di.nro_lote,
		 di.nro_partida,
		 dc.posicion_id AS posicion_id,         
		 dep.descripcion AS deposito_cod,       
		 nave.nave_cod AS nave_cod,             
		 null AS calle_cod,                     
		 null AS columna_cod,                   
		 null AS nivel_cod,                     
		 CASE WHEN @V_NUM_STOCK = 1 THEN (dc.conteo1-di.CANT_STOCK_CONT_1)
		 	  WHEN @V_NUM_STOCK = 2 THEN (dc.conteo2-di.CANT_STOCK_CONT_2)
		      WHEN @V_NUM_STOCK = 3 THEN (dc.conteo3-di.CANT_STOCK_CONT_3)
		 END AS diffConteo
		 ,di.modo_ingreso as modo               
		 ,i.lockgraba
		 FROM                                   
		 inventario i                           
		 ,det_inventario di                     
		 ,det_conteo dc                         
		 ,producto prod                         
		 ,cliente cli                           
		 ,nave                                  
		 ,deposito dep                          
		 WHERE                                  
		 i.inventario_id=di.inventario_id       
		 AND di.inventario_id=dc.inventario_id  
		 AND di.marbete=dc.marbete              
		 AND dc.cliente_id=prod.cliente_id      
		 AND dc.producto_id=prod.producto_id    
		 AND dc.cliente_id=cli.cliente_id       
		 AND dc.nave_id=nave.nave_id            
		 AND nave.deposito_id=dep.deposito_id   
		 AND di.pos_lockeada=0                  
		 AND di.inventario_id=@V_INVENTARIO_ID
		 AND (@V_NUM_STOCK = 1 
			OR (@V_NUM_STOCK = 2 AND (((dc.conteo1-di.CANT_STOCK_CONT_1) <> 0) OR DC.CONTEO1 IS NULL))
			OR (@V_NUM_STOCK = 3 AND (((dc.conteo2-di.CANT_STOCK_CONT_2) <> 0) OR (DC.CONTEO2 IS NULL AND di.modo_ingreso ='M'))))
		 ORDER BY 2


END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[FUNCIONES_INVENTARIO_API#GetCabecera]
@p_doc_trans_id NUMERIC(20) OUTPUT
AS
BEGIN
select i.inventario_id as inventario_id                 
       ,CONVERT(datetime,i.fecha_inicio) as fecha_inicio
       ,CONVERT(datetime,i.fecha_final) as fecha_final  
       ,i.nro_conteo as nro_conteo                      
       ,i.lockear as poslock                            
       ,i.descripcion as descripcion                    
	   ,i.aju_realizado
	   ,i.fecha_aju
	   ,i.cerrado
	   ,i.aju_realizado_2
	   ,i.fecha_aju_2

 FROM inventario i                                      
 WHERE i.doc_trans_id=@p_doc_trans_id

END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#GETDIFINVENTARIO]
@INVENTARIO_ID AS NUMERIC(20,0) output
AS 
BEGIN

declare @usu_id as varchar(100)
declare @usu_nombre as varchar(100)
declare @terminal as varchar(100)

			select top 1 @usu_id = usuario_id, @terminal = terminal from #temp_usuario_loggin

			select @usu_nombre = nombre from  sys_usuario where usuario_id =@usu_id

		SELECT                                              
			prod.cliente_id AS cliente_id         
		  ,cli.razon_social AS cli_razon_social  
		  ,prod.producto_id AS producto_id       
		  ,prod.codigo_producto AS cod_producto  
		  ,prod.descripcion AS prod_descripcion  
		  ,dc.posicion_id AS posicion_id         
		  ,dep.deposito_id AS deposito_id        
		  ,dep.descripcion AS deposito_cod       
		  ,nave.nave_id AS nave_id               
		  ,nave.nave_cod AS nave_cod             
		  ,calle.calle_id AS calle_id            
		  ,calle.calle_cod AS calle_cod
		  ,col.columna_id AS columna_id           
		  ,col.columna_cod AS  columna_cod
		  ,nivel.nivel_id AS nivel_id             
		  ,nivel.nivel_cod AS nivel_cod 
		  ,uc.uc_id AS unidad_contenedora_id      
		  ,uc.nro_serie AS ucNro_Serie            
		  ,uc.descripcion AS ucDescripcion        
		  ,dc.inventario_id AS inventario_id      
		  ,dc.marbete AS marbete                  
		  ,dc.conteo1 AS Conteo1                  
		  ,dc.conteo2 AS Conteo2                  
		  ,dc.conteo3 AS Conteo3                  
   	      ,di.cant_stock_cont_1 as Stock_1                 
		  ,di.cant_stock_cont_2 as Stock_2                 
		  ,di.cant_stock_cont_3 as Stock_3                 
		  ,(dc.conteo1-di.cant_stock_cont_1) AS diffConteo1 
		  ,(dc.conteo2-di.cant_stock_cont_2) AS diffConteo2 
		  ,(dc.conteo3-di.cant_stock_cont_3) AS diffConteo3 
		  ,dc.obsconteo1 AS obsconteo1            
		  ,dc.obsconteo2 AS obsconteo2            
		  ,dc.obsconteo3 AS obsconteo3            
		  ,@usu_nombre as USOINTERNOUsuario         
		  ,@terminal AS USOINTERNOTerminal     
		 FROM                                     
			  det_inventario di                   
			  ,det_conteo dc                      
			  ,producto prod                      
			  ,cliente cli                        
			  ,posicion pos                       
			  LEFT OUTER JOIN unidad_contenedora uc on pos.posicion_id=uc.posicion_id
			  ,nave                               
			  ,deposito dep                       
			  ,calle_nave calle                   
			  ,columna_nave col                   
			  ,nivel_nave nivel                   
		WHERE                                     
		   di.inventario_id=dc.inventario_id      
		   AND di.marbete=dc.marbete              
		   AND dc.cliente_id=prod.cliente_id      
		   AND dc.producto_id=prod.producto_id    
		   AND dc.cliente_id=cli.cliente_id       
		   AND dc.posicion_id=pos.posicion_id     
		   AND pos.nave_id=nave.nave_id           
		   AND nave.deposito_id=dep.deposito_id   
		   AND pos.nave_id=calle.nave_id          
		   AND pos.calle_id=calle.calle_id        
		   AND pos.nave_id=col.nave_id            
		   AND pos.calle_id=col.calle_id          
		   AND pos.columna_id=col.columna_id      
		   AND pos.nave_id=nivel.nave_id          
		   AND pos.calle_id=nivel.calle_id        
		   AND pos.columna_id=nivel.columna_id    
		   AND pos.nivel_id=nivel.nivel_id        
		   AND di.pos_lockeada=0                  

		AND di.inventario_id = @INVENTARIO_ID
		UNION                                               
		SELECT 
		   prod.cliente_id AS cliente_id         
		   ,cli.razon_social AS cli_razon_social 
		   ,prod.producto_id AS producto_id      
		   ,prod.codigo_producto AS cod_producto 
		   ,prod.descripcion AS prod_descripcion 
		   ,NULL AS posicion_id                  
		   ,dep.deposito_id AS deposito_id       
		   ,dep.descripcion AS deposito_cod      
		   ,nave.nave_id AS nave_id              
		   ,nave.nave_cod AS nave_cod            
		   ,NULL AS calle_id                     
		   ,NULL AS calle_cod                    
		   ,NULL AS columna_id                   
		   ,NULL AS columna_cod                  
		   ,NULL AS nivel_id                     
		   ,NULL AS nivel_cod                    
		   ,NULL AS unidad_contenedora_id        
		   ,NULL AS ucNro_Serie                  
		   ,NULL AS ucDescripcion                
		   ,dc.inventario_id AS inventario_id    
		   ,dc.marbete AS marbete                
		   ,dc.conteo1 AS Conteo1                
		   ,dc.conteo2 AS Conteo2                
		   ,dc.conteo3 AS Conteo3                
		   ,di.cant_stock_cont_1 as Stock_1                 
		   ,di.cant_stock_cont_2 as Stock_2                 
		   ,di.cant_stock_cont_3 as Stock_3                 
		   ,(dc.conteo1-di.cant_stock_cont_1) AS diffConteo1 
		   ,(dc.conteo2-di.cant_stock_cont_2) AS diffConteo2 
		   ,(dc.conteo3-di.cant_stock_cont_3) AS diffConteo3 
		   ,dc.obsconteo1 AS obsconteo1          
		   ,dc.obsconteo2 AS obsconteo2          
		   ,dc.obsconteo3 AS obsconteo3          
		   ,@usu_nombre as USOINTERNOUsuario            
		   ,@terminal AS USOINTERNOTerminal           
		FROM                                     
		det_inventario di                        
		,det_conteo dc                           
		,producto prod                           
		,cliente cli                             
		,nave                                    
		,deposito dep                            
		WHERE                                    
		di.inventario_id=dc.inventario_id        
		AND di.marbete=dc.marbete                
		AND dc.cliente_id=prod.cliente_id        
		AND dc.producto_id=prod.producto_id      
		AND dc.cliente_id=cli.cliente_id         
		AND dc.nave_id=nave.nave_id              
		AND nave.deposito_id=dep.deposito_id     
		AND di.pos_lockeada=0                    
		AND di.inventario_id=@INVENTARIO_ID
		 ORDER BY 20,1,3,21




END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[FUNCIONES_INVENTARIO_API#GETSELECTCONTROL]
@P_DOC_TRANS_ID NUMERIC(20) output,
@P_LISTADO numeric(1) output
AS 
BEGIN


SELECT i.inventario_id AS inventario_id,     
   dc.marbete AS marbete,                    
   i.nro_conteo AS nro_conteo,               
   CASE WHEN  di.modo_ingreso in ('M') THEN
		0
   ELSE
		di.cant_stock_cont_1
   END AS ExistenciaConteo1,
   dc.conteo1 as Conteo1,                    
   (dc.conteo1 - di.CANT_STOCK_CONT_1) as DifConteo1, 
   CASE WHEN  di.modo_ingreso in ('M') THEN
		0
   ELSE
		di.cant_stock_cont_2
   END AS ExistenciaConteo2,
   dc.conteo2 as Conteo2,                    
   (dc.conteo2 - di.CANT_STOCK_CONT_2) as DifConteo2, 
   CASE WHEN  di.modo_ingreso in ('M') THEN
		0
   ELSE
		di.cant_stock_cont_3
   END AS ExistenciaConteo3,
   dc.conteo3 as Conteo3,                    
   (dc.conteo3 - di.CANT_STOCK_CONT_3) as DifConteo3, 
   CASE WHEN A.CANT_AJU IS NOT NULL THEN
		ISNULL(A.CANT_AJU, ISNULL(DC.CONTEO3, ISNULL(DC.CONTEO2, DC.CONTEO1 ))) 
   ELSE
		0
   END AS CANT_AJU,
   a.PROCESADO, 
   dc.obsconteo1 as obsconteo1,              
   dc.obsconteo2 as obsconteo2,              
   dc.obsconteo3 as obsconteo3,              
   cli.razon_social AS cli_razon_social,     
   prod.producto_id AS producto_id,          
   prod.codigo_producto AS cod_producto,     
   prod.descripcion AS prod_descripcion,     
   di.nro_lote,
   di.nro_partida,
   dc.posicion_id AS posicion_id,            
   dep.descripcion AS deposito_cod,          
   nave.nave_cod AS nave_cod,                
   calle.calle_cod AS calle_cod,             
   col.columna_cod AS columna_cod,           
   nivel.nivel_cod AS nivel_cod              
   ,di.modo_ingreso as modo_Ingreso          
   ,CASE WHEN di.pos_lockeada = 1 THEN 'S' ELSE 'N' end as lockeada
FROM                                         
   inventario i 
	inner join det_inventario di on (i.inventario_id=di.inventario_id)
	left join DET_INVENTARIO_AJU A on (A.INVENTARIO_ID = DI.INVENTARIO_ID AND A.MARBETE = DI.MARBETE)
	inner join det_conteo dc on (di.inventario_id=dc.inventario_id AND di.marbete=dc.marbete)
	inner join producto prod on (dc.cliente_id=prod.cliente_id AND dc.producto_id=prod.producto_id)
	inner join cliente cli on (dc.cliente_id=cli.cliente_id)
  	inner join posicion pos  on (dc.posicion_id=pos.posicion_id)
  	inner join nave  on (pos.nave_id=nave.nave_id)
  	inner join deposito dep  on (nave.deposito_id=dep.deposito_id)
  	inner join calle_nave calle  on (pos.nave_id=calle.nave_id AND pos.calle_id=calle.calle_id)
  	inner join columna_nave col  on (pos.nave_id=col.nave_id  AND pos.calle_id=col.calle_id AND pos.columna_id=col.columna_id)
  	inner join nivel_nave nivel  on (pos.nave_id=nivel.nave_id AND pos.calle_id=nivel.calle_id AND pos.columna_id=nivel.columna_id AND pos.nivel_id=nivel.nivel_id)
WHERE                                        
	   (	  @P_LISTADO = 1 
		 OR ( @P_LISTADO = 2 AND (isnull(dc.conteo3 - di.CANT_STOCK_CONT_3,isnull(dc.conteo2 - di.CANT_STOCK_CONT_2,dc.conteo1-di.CANT_STOCK_CONT_1)) <> 0))
		 OR ( @P_LISTADO = 3 AND (isnull(dc.conteo3 - di.cantidad,isnull(dc.conteo2 - di.CANT_STOCK_CONT_2,dc.conteo1-di.CANT_STOCK_CONT_1)) = 0))
		 OR ( @P_LISTADO = 4 AND di.modo_ingreso in ('M'))
		 OR ( @P_LISTADO = 5 AND di.pos_lockeada=1)
       )
	AND i.doc_trans_id=@P_DOC_TRANS_ID
 UNION                                       
SELECT                                       
   i.inventario_id AS inventario_id,         
   dc.marbete AS marbete,                    
   i.nro_conteo AS nro_conteo,               
   CASE WHEN  di.modo_ingreso in ('M') THEN
		0
   ELSE
		di.cant_stock_cont_1
   END AS ExistenciaConteo1,
   dc.conteo1 as Conteo1,                    
   (dc.conteo1 - di.CANT_STOCK_CONT_1) as DifConteo1, 
   CASE WHEN  di.modo_ingreso in ('M') THEN
		0
   ELSE
		di.cant_stock_cont_2
   END AS ExistenciaConteo2,
   dc.conteo2 as Conteo2,                    
   (dc.conteo2 - di.CANT_STOCK_CONT_2) as DifConteo2, 
   CASE WHEN  di.modo_ingreso in ('M') THEN
		0
   ELSE
		di.cant_stock_cont_3
   END AS ExistenciaConteo3,
   dc.conteo3 as Conteo3,                    
   (dc.conteo3 - di.CANT_STOCK_CONT_3) as DifConteo3,
   CASE WHEN A.CANT_AJU IS NOT NULL THEN
		ISNULL(A.CANT_AJU, ISNULL(DC.CONTEO3, ISNULL(DC.CONTEO2, DC.CONTEO1 ))) 
   ELSE
		0
   END AS CANT_AJU,
   a.PROCESADO,
   dc.obsconteo1 as obsconteo1,              
   dc.obsconteo2 as obsconteo2,              
   dc.obsconteo3 as obsconteo3,              
   cli.razon_social AS cli_razon_social,     
   prod.producto_id AS producto_id,          
   prod.codigo_producto AS cod_producto,     
   prod.descripcion AS prod_descripcion,  
   di.nro_lote,
   di.nro_partida,   
   dc.posicion_id AS posicion_id,            
   dep.descripcion AS deposito_cod,          
   nave.nave_cod AS nave_cod,                
   null AS calle_cod,                        
   null AS columna_cod,                      
   null AS nivel_cod                         
   ,di.modo_ingreso as modo_Ingreso          
   ,CASE WHEN di.pos_lockeada = 1 THEN 'S' ELSE 'N' end as lockeada
FROM 
    inventario i 
    inner join det_inventario di on (i.inventario_id=di.inventario_id)
	left join DET_INVENTARIO_AJU A on (A.INVENTARIO_ID = DI.INVENTARIO_ID AND A.MARBETE = DI.MARBETE)
	inner join det_conteo dc on (di.inventario_id=dc.inventario_id AND di.marbete=dc.marbete)
	inner join producto prod on (dc.cliente_id=prod.cliente_id AND dc.producto_id=prod.producto_id)
	inner join cliente cli on (dc.cliente_id=cli.cliente_id)
  	inner join nave  on (DC.nave_id=nave.nave_id)
  	inner join deposito dep  on (nave.deposito_id=dep.deposito_id)
WHERE                                        
      (	  @P_LISTADO = 1 
		 OR ( @P_LISTADO = 2 AND (isnull(dc.conteo3 - di.CANT_STOCK_CONT_3,isnull(dc.conteo2 - di.CANT_STOCK_CONT_2,dc.conteo1-di.CANT_STOCK_CONT_1)) <> 0))
		 OR ( @P_LISTADO = 3 AND (isnull(dc.conteo3 - di.CANT_STOCK_CONT_3,isnull(dc.conteo2 - di.CANT_STOCK_CONT_2,dc.conteo1-di.CANT_STOCK_CONT_1)) = 0))
		 OR ( @P_LISTADO = 4 AND di.modo_ingreso in ('M'))
		 OR ( @P_LISTADO = 5 AND di.pos_lockeada=1)
       )

   AND I.doc_trans_id=@P_DOC_TRANS_ID
ORDER BY 2                                   

END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER    Procedure [dbo].[Funciones_Inventario_Api#Realizar_Ajuste_Cat_log]
@P_CLIENTE_ID 		As varchar(15),
@P_PRODUCTO_ID 		As varchar(30),
@P_fec_vto 			As datetime,
@P_nro_lote 		As varchar(50),
@P_nro_partida 		As varchar(50),
@P_Nro_Despacho 	As varchar(50),
@P_Nro_Bulto 		As varchar(50),
@P_nro_serie 		As varchar(50),
@P_Nave 			As numeric(20,0),
@P_posicion 		As numeric(20,0),
@pvalor 			As numeric(20,0),
@pCatLogOld 		As varchar(50),
@pCatLogNew 		As varchar(50),
@P_Est_Merc_Id 		As varchar(15),
@P_PROP1 			As varchar(100),
@P_Prop2 			As varchar(100),
@P_Prop3 			As varchar(100),
@P_PESO 			As numeric(20,5),
@P_VOLUMEN 			As numeric(20,5),
@P_UNIDAD_ID 		As varchar(5),
@P_UNIDAD_PESO 		As varchar(5),
@P_UNIDAD_VOLUMEN 	As varchar(5),
@P_Moneda_Id 		As varchar(20),
@P_Costo 			As numeric(20,5),
@P_Rl_Id			As numeric(20,0)
As
Begin
	----------------------------------------------------
	--			Cursores.
	----------------------------------------------------
	Declare @pCur1					Cursor --Cursor para la rl
	----------------------------------------------------
	--			Para @pCur1
	----------------------------------------------------
	Declare @Rl_id					As numeric(20,0)
	----------------------------------------------------
	--			Genericas
	----------------------------------------------------
	Declare @Total_A_Cambiar		As numeric(20,5)
	Declare @Cant_Total_Ubic		As numeric(20,5)
	Declare @cant_a_ubicar			as numeric(20,5)
	----------------------------------------------------
	Declare @vUsuario				As varchar(20)
	Declare @vTerminal				As varchar(100)
	----------------------------------------------------
	--			Para Tabla Rl.
	----------------------------------------------------
	Declare @Rl_Id1					As  numeric(20, 0)
	Declare @Doc_Trans_Id 			As  numeric(20, 0)  
	Declare @Nro_Linea_Trans 		As  numeric(10, 0)  
	Declare @Posicion_Anterior 		As  numeric(20, 0)  
	Declare @Posicion_Actual 		As  numeric(20, 0)  
	Declare @Cantidad 				As  numeric(20, 5)  
	Declare @Tipo_Movimiento_Id 	As  varchar(5) 
	Declare @Ultima_Estacion 		As  varchar(5)   
	Declare @Ultima_Secuencia 		As  numeric(3, 0)  
	Declare @Nave_Anterior 			As  numeric(20, 0)  
	Declare @Nave_Actual 			As  numeric(20, 0)  
	Declare @Documento_id 			As  numeric(20, 0)  
	Declare @Nro_Linea 				As  numeric(10, 0)  
	Declare @Disponible 			As  varchar(1)
	Declare @Doc_Trans_Id_Egr 		As  numeric(20, 0)  
	Declare @Nro_Linea_Trans_Egr 	As  numeric(10, 0)  
	Declare @Doc_Trans_Id_Tr 		As  numeric(20, 0)  
	Declare @Nro_Linea_Trans_Tr 	As  numeric(10, 0)  
	Declare @Cliente_id 			As  varchar(15) 
	Declare @Cat_Log_Id 			As  varchar(50) 
	Declare @Cat_Log_Id_Final 		As  varchar(50)
	Declare @Est_Merc_Id 			As  varchar(15)
	----------------------------------------------------

	Set @Cant_A_Ubicar=0	

	Select @Total_A_Cambiar=Cantidad from Rl_Det_Doc_Trans_Posicion Where Rl_Id=@P_Rl_Id;

	Select @vUsuario=Usuario_id,@vTerminal=Terminal from #Temp_Usuario_Loggin;	

	Update Historico_Pos_Ocupadas2 SET fecha = getdate()

	Begin

		SELECT 	@Rl_Id1					=  rl_id,
				@Doc_Trans_Id 			=  Doc_Trans_Id,
				@Nro_Linea_Trans 		=  Nro_Linea_Trans,
				@Posicion_Anterior 		=  Posicion_Anterior,
				@Posicion_Actual 		=  Posicion_Actual,
				@Cantidad 				=  Cantidad,
				@Tipo_Movimiento_Id 	=  Tipo_Movimiento_Id,
				@Ultima_Estacion 		=  Ultima_Estacion,
				@Ultima_Secuencia 		=  Ultima_Secuencia,
				@Nave_Anterior 			=  Nave_Anterior,
				@Nave_Actual 			=  Nave_Actual,
				@Documento_id 			=  Documento_Id,
				@Nro_Linea 				=  Nro_Linea,
				@Disponible 			=  Disponible,
				@Doc_Trans_Id_Egr 		=  Doc_Trans_Id_Egr,
				@Nro_Linea_Trans_Egr 	=  Nro_Linea_Trans_Egr,
				@Doc_Trans_Id_Tr 		=  Doc_Trans_Id_Tr,
				@Nro_Linea_Trans_Tr 	=  Nro_Linea_Trans_Tr,
				@Cliente_id 			=  Cliente_id,
				@Cat_Log_Id 			=  Cat_Log_Id,
				@Cat_Log_Id_Final 		=  Cat_Log_Id_Final,
				@Est_Merc_Id 			=  Est_Merc_Id
		FROM 	RL_DET_DOC_TRANS_POSICION 
		WHERE 	RL_ID=@P_Rl_Id

		Set 	@Cant_Total_Ubic = @Cantidad

		Update 	rl_det_doc_trans_posicion
		Set  	cat_log_id =@pCatLogNew
		Where 	rl_id =@P_Rl_Id

		INSERT INTO HISTORICO_RL_CAT_LOG      
        Values ( GETDATE()             
                ,@Doc_Trans_Id
                ,@Nro_Linea_Trans
                ,@Nave_Actual
                ,@Nave_Actual
                ,@Posicion_Actual
                ,@posicion_actual
                ,@Cantidad
                ,@cat_log_id_final
                ,@cant_a_ubicar
                ,@pCatLogNew
                ,@vUsuario
                ,@vterminal
                );

	End

End --Fin Procedure
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[FUNCIONES_INVENTARIO_API#SAVE_CANT_AJU]
@P_INVENTARIO_ID NUMERIC(20) OUTPUT,
@P_MARBETE NUMERIC(20) OUTPUT,
@P_CANT_AJU NUMERIC(20,5) OUTPUT
AS
BEGIN
	UPDATE DET_INVENTARIO_AJU SET CANT_AJU = @P_CANT_AJU WHERE INVENTARIO_ID = @P_INVENTARIO_ID AND MARBETE = @P_MARBETE 
END
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER   Procedure [dbo].[Funciones_Movimiento_api#Actualizar_Reglas_Pendientes]
@Doc_Trans_Id 	as Numeric(20,0),
@Regla			as Varchar(5),
@Opcion			as Int
As
Begin
	Declare @vTotal as Int
	
	If ltrim(rtrim(Upper(@Regla)))='RM_1'
	Begin
		If @Opcion=1
		Begin
			delete 	pendiente_doc_trans
			where 	doc_trans_id =@doc_trans_id
					and regla_id = 'RM_1'
		End
		Else
		Begin
			select @vTotal=isnull(count(regla_id),0)
			from pendiente_doc_trans
			Where doc_trans_id =@Doc_Trans_id
			and regla_id = 'RM_1'
			
			if @vTotal=0
			Begin
				Insert Into PENDIENTE_DOC_TRANS(
				        DOC_TRANS_ID,
				        TIPO_REGLA,
				        REGLA_ID,
				        NRO_LINEA)
				 Values (
				        @Doc_trans_id
				        ,'MOV'
				        ,'RM_1'
				        ,1
				     )
			End
		End--Fin Else
	End
	Else
	Begin
		if @Opcion=1
		Begin
			delete 	pendiente_doc_trans
			where 	doc_trans_id =@doc_trans_id
					and regla_id = 'RU_1'
		End
	End --Fin Else
End --Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER    Procedure [dbo].[Funciones_Movimiento_Api#Realizar_Movimiento]
@RlId as numeric(20,0),
@Secuencia_Realizada as int
As
Begin

	Declare @DocTransID			as numeric(20,0)
	Declare @NroLineaT			as numeric(10,0)
	Declare @TipoOperacion		as varchar(5)
	Declare @Posicion_Actual	as numeric(20,0)
	Declare @Posicion_anterior 	as numeric(20,0)
	Declare @Tipo_Movimiento	as varchar(5)
	Declare @max_secuencia		as int
	Declare @vCountInt			as Int
	Declare @ppos_lock			as varchar(1)
	Declare @plock_tipo_op		as varchar(5)
	Declare @plock_doc_trans_id as numeric(20,0)
	Declare @PosVacia			as varchar(1)
	Declare @Flag				as Int


	select 	@DocTransID=dt.doc_trans_id,@NroLineaT=rl.nro_linea_trans,@TipoOperacion=dt.tipo_operacion_id
	from 	rl_det_doc_trans_posicion rl,documento_transaccion dt
	where 	rl_id = @rlid and isnull(isnull(rl.doc_trans_id_egr,rl.doc_trans_id_tr),rl.doc_trans_id)=dt.doc_trans_id

	Update rl_det_doc_trans_posicion set ultima_secuencia =1	where rl_id =@RlId
	Update det_documento_transaccion set movimiento_pendiente='1' where doc_trans_id =@doctransid     and nro_linea_trans=@nrolineat
	Update documento_transaccion Set it_mover = 1	WHERE doc_trans_id=@DocTransID

	select @Posicion_Actual=posicion_actual, @Posicion_anterior=posicion_anterior, @Tipo_Movimiento=tipo_movimiento_id
	from rl_det_doc_trans_posicion
	where rl_id =@RlId

	if @@RowCount>0
	Begin
		If @Tipo_Movimiento is not null
		Begin
			    SELECT 	@max_secuencia=Max(secuencia)  FROM det_tipo_movimiento
			    Where 	tipo_movimiento_id =1
		End
		Else
		Begin
			    SELECT 	@max_secuencia=Max(secuencia)  FROM det_tipo_movimiento
			    Where 	tipo_movimiento_id is null
		End
		
		if @Posicion_Actual is not null
		Begin
			SELECT 	@vCountInt=Count(rl_id) 
			FROM 	rl_det_doc_trans_posicion rl
			Where 	posicion_actual = @Posicion_Actual
					and ultima_secuencia IS NULL and rl.doc_trans_id_egr =@DocTransID
			
			if @vCountInt=0
			Begin
				SELECT 	@ppos_lock=pos_lockeada,@plock_tipo_op=lck_tipo_operacion,@plock_doc_trans_id=lck_doc_trans_id
				From 	posicion
				WHERE 	posicion_id = @Posicion_Actual
				
				If @plock_tipo_op = @tipooperacion  And @plock_doc_trans_id = @doctransid And @max_secuencia = @Secuencia_Realizada
				Begin
					Update posicion	SET pos_vacia=0, pos_lockeada=0, LCK_TIPO_OPERACION=NULL, LCK_USUARIO_ID=NULL, LCK_DOC_TRANS_ID=NULL,LCK_OBS = Null
					WHERE posicion_id = @posicion_actual
				End
			End
		End
		
		if @Posicion_Anterior is not null
		Begin
			SELECT	@vCountInt=Count(rl_id)
			FROM 	rl_det_doc_trans_posicion rl
			Where 	posicion_actual = @posicion_anterior
			      	and ultima_secuencia IS NULL and rl.doc_trans_id_egr =@DocTransID
			
			If @vCountInt=0
			Begin
				If @posicion_anterior is not null
				Begin
					SELECT 	@ppos_lock=pos_lockeada,@plock_tipo_op=lck_tipo_operacion,@plock_doc_trans_id=lck_doc_trans_id,@PosVacia=pos_vacia
					From posicion
					WHERE posicion_id =@posicion_anterior
				End
				Else
				Begin
					SELECT 	@ppos_lock=pos_lockeada,@plock_tipo_op=lck_tipo_operacion,@plock_doc_trans_id=lck_doc_trans_id,@PosVacia=pos_vacia
					From posicion
					WHERE posicion_id is null
				End
				
				If @plock_tipo_op = @tipooperacion  And @plock_doc_trans_id = @doctransid And @max_secuencia = @Secuencia_Realizada
				Begin
					Update posicion SET pos_lockeada=0, LCK_TIPO_OPERACION = NULL, LCK_USUARIO_ID = NULL, LCK_DOC_TRANS_ID=NULL, LCK_OBS = Null
	                WHERE posicion_id = @posicion_anterior
					
					select @vCountInt=sum(isnull(cantidad,0))
					from rl_det_doc_trans_posicion rl
					where posicion_actual=@posicion_anterior
					
					if @vCountInt>0
					Begin
						Set @Flag=0
					End
					Else
						Set @Flag=1				
					End

					if @posvacia=0 and @Flag=1
					Begin
						Update posicion Set pos_vacia = 1  WHERE posicion_id =@posicion_anterior
					End
				End				
			End
		End
	End 
--End --Fin Procedure.
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER    Procedure [dbo].[Funciones_movimiento_api#Tareas_Post_Movimiento]
@Doc_trans_id	as numeric(20,0)
As
Begin
	Declare @TipoOperacion 	as varchar(5)
	Declare @Estacion 		as varchar(20)

	Select @TipoOperacion=tipo_operacion_id from documento_transaccion  where doc_trans_id =@Doc_trans_Id

	if @TipoOperacion='ING'
	Begin
		SELECT 	@Estacion=min(estacion)
		FROM  	det_tipo_movimiento t1, RL_DET_DOC_TRANS_POSICION t2,
		      	det_documento_transaccion t3,det_documento t4
		WHERE 	t1.tipo_movimiento_id = t2.tipo_movimiento_id 
				and t3.nro_linea_trans = t2.nro_linea_trans 
				and t4.documento_id = t3.documento_id AND  t4.nro_linea = t3.nro_linea_doc 
				and t2.doc_trans_id =@Doc_trans_id 
				AND t2.ultima_estacion < t1.estacion AND
				((ultima_secuencia IS NOT NULL and ultima_secuencia < secuencia) OR
				(ultima_secuencia IS NULL)  ) AND t3.doc_trans_id =@Doc_trans_id
				and t2.doc_trans_id=t3.doc_Trans_id

		UPDATE RL_DET_DOC_TRANS_POSICION
		SET  ultima_estacion =  @Estacion
			,ultima_secuencia = NULL 
			,tipo_movimiento_ID=null 
		where doc_trans_id=@Doc_trans_id

	End
	
	if @TipoOperacion='EGR'
	Begin
		SELECT 	@Estacion=min(estacion)
		FROM  	det_tipo_movimiento t1, RL_DET_DOC_TRANS_POSICION t2,
		      	det_documento_transaccion t3,det_documento t4
		WHERE 	t1.tipo_movimiento_id = t2.tipo_movimiento_id 
				and t3.nro_linea_trans = t2.nro_linea_trans 
				and t4.documento_id = t3.documento_id AND  t4.nro_linea = t3.nro_linea_doc 
				and t2.doc_trans_id =@Doc_trans_id 
				AND t2.ultima_estacion < t1.estacion AND
				((ultima_secuencia IS NOT NULL and ultima_secuencia < secuencia) OR
				(ultima_secuencia IS NULL)  ) AND t3.doc_trans_id =@Doc_trans_id
				and t2.doc_trans_id=t3.doc_Trans_id	
				and t2.doc_trans_id_egr=t3.doc_Trans_id

		UPDATE RL_DET_DOC_TRANS_POSICION
		SET  ultima_estacion =  @Estacion
			,ultima_secuencia = NULL 
			,tipo_movimiento_ID=null 
		where doc_trans_id_egr=@Doc_trans_id

	End
		
	If @TipoOperacion='TR'
	Begin
		SELECT 	@Estacion=min(estacion)
		FROM  	det_tipo_movimiento t1, RL_DET_DOC_TRANS_POSICION t2,
		      	det_documento_transaccion t3,det_documento t4
		WHERE 	t1.tipo_movimiento_id = t2.tipo_movimiento_id 
				and t3.nro_linea_trans = t2.nro_linea_trans 
				and t4.documento_id = t3.documento_id AND  t4.nro_linea = t3.nro_linea_doc 
				and t2.doc_trans_id =@Doc_trans_id 
				AND t2.ultima_estacion < t1.estacion AND
				((ultima_secuencia IS NOT NULL and ultima_secuencia < secuencia) OR
				(ultima_secuencia IS NULL)  ) AND t3.doc_trans_id =@Doc_trans_id
				and t2.doc_trans_id=t3.doc_Trans_id	
				and t2.doc_trans_id_tr=t3.doc_Trans_id

		UPDATE RL_DET_DOC_TRANS_POSICION
		SET  ultima_estacion =  @Estacion
			,ultima_secuencia = NULL 
			,tipo_movimiento_ID=null 
		where doc_trans_id_tr=@Doc_trans_id

	End

	--Exec Funciones_Movimiento_api#Actualizar_Reglas_Pendientes @Doc_trans_id,'RM_1',1
	UPDATE documento_transaccion SET it_mover=0 Where doc_trans_id=@Doc_trans_id

End
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER    Procedure [dbo].[Funciones_Movimiento_api#Tareas_pre_movimiento]
	@Doc_trans_id as numeric(20,0)
As
Begin

	Declare @Rl_Id 		as Numeric(20,0)
	Declare @Nav_Ant 	as Numeric(20,0)
	Declare @Pos_Ant	as Numeric(20,0)
	Declare @Pos_Act	as Numeric(20,0)
	Declare @Nav_Act	as Numeric(20,0)
	
	Declare Tpm Cursor For
	SELECT 	 rl_id 
			--,nave_anterior
			--,posicion_anterior
			--,posicion_actual
			--,nave_actual
	FROM  	RL_DET_DOC_TRANS_POSICION rl 
	WHERE 	rl.doc_trans_id_egr = @Doc_Trans_id

	Open Tpm
	Fetch Next From Tpm into @Rl_Id
	While @@Fetch_Status=0
	Begin
		Update RL_DET_DOC_TRANS_POSICION SET tipo_movimiento_id = '1',ultima_estacion = 'A',ultima_secuencia = Null
		Where rl_id =@Rl_Id
	
		Fetch Next From Tpm into @Rl_Id
	End
	Close Tpm
	Deallocate Tpm

	update documento_transaccion set est_mov_actual = 'A' where doc_trans_id =@doc_trans_id

	--EXEC Funciones_Movimiento_api#Actualizar_Reglas_Pendientes @Doc_trans_id, 'RM_1', 0
    -- EXEC Funciones_Movimiento_api#Actualizar_Reglas_Pendientes @Doc_trans_id, 'RU_1', 1

End --Fin procedure
GO

IF @@ERROR <> 0
BEGIN
   IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
   INSERT INTO #tmpErrors (Error) SELECT 1
   BEGIN TRANSACTION
END
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

ALTER       Procedure [dbo].[Funciones_Stock_Api#Documento_A_Egreso]
				 @pDocumentoID numeric(20,0)
As
Begin

	Declare @vCategoriaStockID varchar(8)
	Declare @Fecha_Actual datetime
	Declare @pUsuarioID varchar(20)
	Declare @Nro_Linea numeric(10,0)
	Declare @Cliente_Id varchar(15)
	Declare @Producto_Id varchar(30)
	Declare @Cantidad numeric(20,5)
	Declare @Tipo_Operacion_Id varchar(5)
	Declare @Nro_Serie varchar(50)
	Declare @Nro_Lote varchar(50)
	Declare @Fecha_Vencimiento datetime
	Declare @Nro_Partida varchar(50)
	Declare @Nro_Despacho varchar(50)
	Declare @vNaveID numeric(20,0)
	Declare @vHPOS numeric(20,0)

	Set @vCategoriaStockID = 'TRAN_EGR'
	Set @Fecha_Actual = GetDate()

	Select @pUsuarioID = usuario_id	From #temp_usuario_loggin
	
	Declare PCUR Cursor For
		Select  dd.nro_linea,
				dd.cliente_id,
				dd.producto_id,
				dd.cantidad,
				d.tipo_operacion_id,
				dd.nro_serie,
				dd.nro_lote,
				dd.fecha_vencimiento,
				dd.nro_partida,
				dd.nro_despacho
		From det_documento dd,
		   documento d
		Where d.documento_id = dd.documento_id
		    And d.documento_id = @pDocumentoID

	Open PCUR
	Fetch Next From PCUR Into @Nro_Linea, @Cliente_Id, @Producto_Id
							, @Cantidad, @Tipo_Operacion_Id, @Nro_Serie
							, @Nro_Lote, @Fecha_Vencimiento, @Nro_Partida
							, @Nro_Despacho
	While @@Fetch_Status = 0
	Begin

		Exec Historico_Producto_Api#InsertRecord 
								@vHPOS 
                                ,@Fecha_Actual
                                ,Null
                                ,@Cantidad
                                ,@Tipo_Operacion_Id
                                ,Null 
                                ,@vNaveID
								,@pDocumentoID
								,@Nro_Linea 
								,@pUsuarioID
                                ,'+'
                                ,@Cliente_Id 
                                ,@Producto_Id
								,@Nro_Serie
								,@Nro_Lote
								,@Fecha_Vencimiento
								,@Nro_Partida
								,@Nro_Despacho

		Update det_documento
			Set cat_log_id = (Select cat_log_id
		                         From categoria_stock cs
		                         Inner Join categoria_logica cl
		                         On cl.categ_stock_id = cs.categ_stock_id
		                         Where cs.categ_stock_id = 'TRAN_EGR'
		                         And cliente_id = @Cliente_Id
							)
		Where documento_id = @pDocumentoID 
			And nro_linea = @Nro_Linea

		Update documento
		Set status = 'D20'
		Where documento_id = @pDocumentoID


		Fetch Next From PCUR Into @Nro_Linea, @Cliente_Id, @Producto_Id
								, @Cantidad, @Tipo_Operacion_Id, @Nro_Serie
								, @Nro_Lote, @Fecha_Vencimiento, @Nro_Partida
								, @Nro_Despacho
	End

	Close PCUR
	Deallocate PCUR



	Exec dbo.Funciones_Historico_Api#Actualizar_HistSaldos_STOCK @pDocumentoID, Null, Null
	Exec dbo.Funciones_Historico_Api#Actualizar_HistSaldos_CatLog @pDocumentoID, Null, Null

End
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