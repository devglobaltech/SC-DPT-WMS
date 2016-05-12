/****** Object:  StoredProcedure [dbo].[Funciones_Estacion_Api#Obtener_Rl_Id_Documento]    Script Date: 09/12/2014 12:38:12 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Funciones_Estacion_Api#Obtener_Rl_Id_Documento]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Funciones_Estacion_Api#Obtener_Rl_Id_Documento]
GO

CREATE          Procedure [dbo].[Funciones_Estacion_Api#Obtener_Rl_Id_Documento] 
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
	
	Set @Msg1 = @Msg1 + '	and not exists (select 1 from #tmp_consumo_locator_egr where rl_id = rl.rl_id)' + Char(13)
	
	Set @Msg1 = @Msg1 + '	and not exists (select 1 from consumo_locator_egr where rl_id = rl.rl_id)' + Char(13)
    Set @Msg1 = @Msg1 + 'order by rl.cantidad ' + Char(13)
    
	/*If (@P_Opcion = 1 or @P_Opcion = 2 or @P_Opcion = 3 or @P_Opcion = 4 or @P_Opcion = 5)
		Begin
			Set @Msg1 = @Msg1 + 'order by rl.cantidad ' + Char(13)
		End 
	*/
	Set @StrSql =N'set @pcur=cursor for '+ @Msg + @Aux_Where + @Aux_Where1 + @Msg1 + '; open @pcur'

	Set @ParmDefinition =  N'@pcur cursor output '
	
	Execute Sp_ExecuteSql @StrSql, @ParmDefinition,
    	                  @Pcur=@Pcur Output
	
End



GO


