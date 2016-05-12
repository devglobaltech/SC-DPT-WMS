
GO

/*
Script created by Quest Change Director for SQL Server at 14/12/2012 05:26 p.m.
Please back up your database before running this script
*/

PRINT N'Synchronizing objects from V9 to CORAL'
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

ALTER                     Procedure [dbo].[Locator_Api#Verifica_Existencia_Ubic_Mov]
	 @p_cliente_id 			as varchar(15)
	,@p_producto_id 		as varchar(30)
	,@p_cantidad 			As numeric(20,5)
	,@p_TieIN 				As varchar(1)
	,@p_nro_serie 			As varchar(50)
	,@p_nro_lote 			As varchar(50)
	,@p_fecha_vencimiento 	As varchar(20)
	,@p_Nro_Despacho 		As varchar(50)
	,@p_Nro_Bulto 			As varchar(50)
	,@p_nro_partida 		As varchar(50)
	,@p_CatLog_ID 			As varchar(50)
	,@p_peso 				As numeric(20,5)
	,@p_volumen 			As numeric(20,5)
	,@p_Nave 				As varchar(45)
	,@p_calle 				As varchar(45)
	,@p_columna 			As varchar(45)
	,@p_Nivel 				As varchar(45)
	,@P_PROP1 				As varchar(100)
	,@p_Prop2 				As varchar(100)
	,@p_Prop3 				As varchar(100)
	,@p_unidad_id 			As varchar(5)
	,@p_unidad_peso 		As varchar(5)
	,@p_unidad_volumen 		As varchar(5)
	,@p_Est_Merc_Id 		As varchar(15)
	,@p_Moneda_Id 			As varchar(20)
	,@p_Costo 				As numeric(10,3)
	,@p_documento_id 		As numeric(20,0)
	,@p_doc_trans_id 		As numeric(20,0)
	,@p_TipoOperacion_ID 	As varchar(5)
	,@p_Ilimitado 			As char(1)
	,@p_nave_id 			As numeric(20,0)
	,@p_posicion_id 		As numeric(20,0)
--	,@c_egr 				Cursor varying Output
As
Begin
	------------------------------------------------
	--			Declaraciones.
	------------------------------------------------
	Declare @vDepositoID		as varchar(15)
	Declare @intVarSuma 		as Int
	Declare @intVarReservados 	as Int
	Declare @intVarContador 	as Int
	Declare @p_fecha_vto_hta	as DateTime
	Declare @p_fecha_vto_dde	as Datetime
	Declare @vNro_Serie			as varchar(50)
	Declare @vNro_Partida		as varchar(50)
	Declare @vNro_Lote			as varchar(50)
	Declare @vNro_Despacho		as varchar(50)
	Declare @vNro_Bulto			as varchar(50)
	Declare @vCat_Log			as varchar(50)
	Declare @vProp1				as varchar(100)
	Declare @vProp2				as varchar(100)
	Declare @vProp3				as varchar(100)
	Declare @vFecha_Vencimiento as datetime
	Declare @vUnidad_Id			as varchar(5)
	Declare @vEst_Merc_Id		as varchar(15)
	Declare	@vPeso				as numeric(20,5)
	Declare @vVolumen			as numeric(20,5)
	Declare @vUnidad_Peso		as varchar(5)
	Declare @vMoneda_id			as varchar(50)
	Declare @vCosto				as numeric(20,5)
	Declare @vUnidad_Volumen	as varchar(5)
	Declare @tCliente_Id 		as varchar(15)
	Declare @tProducto_Id 		as varchar(30)
	Declare @tNro_Serie			as varchar(50)
	Declare @tNro_Lote 			as varchar(50)
	Declare @tNro_Despacho 		as varchar(50)
	Declare @tNro_Bulto			as varchar(50)
	Declare @tNro_Partida 		as varchar(50)
	Declare @tCat_Log 			as varchar(15)
	Declare @tProp1 			as varchar(100)
	Declare @tProp2 			as varchar(100)
	Declare @tProp3 			as varchar(100)
	Declare @tUnidad_Id 		as varchar(5)
	Declare @tEst_Merc_Id 		as varchar(15)
	Declare @varRowId			as integer
	Declare @Abierto			as Char(1)
	------------------------------------------------
	--			Query String
	------------------------------------------------
	Declare @StrSqlOrderBy		as varchar(8000)
	Declare @strsql				as varchar(8000)
	Declare @StrSql1			as varchar(8000)
	Declare @NUnion				as varchar(8000)
	Declare @NStrSql			as varchar(8000)
	Declare @NStrSql1			as varchar(8000)
	Declare @StrX				as varchar(8000)
	Declare @StrSql2			as varchar(8000)
	Declare @StrSql2a			as varchar(8000)
	Declare @StrSql3			as varchar(8000)
	Declare @StrSql5			as varchar(8000)
	Declare @xSQL				as varchar(8000)
	Declare @nSQL				as nvarchar(4000)
	------------------------------------------------
	--			Sys_Criterios_Locator
	------------------------------------------------
	Declare @Criterio_id		as varchar(30)
	Declare @Order_id			as varchar(5)
	Declare @Forma_id			as Varchar(30)
	------------------------------------------------
	--			Cursor Existencia
	------------------------------------------------
	Declare @rl_id 				as numeric(20,0)
	Declare @ClienteID			as varchar(15)
	Declare @Productoid 		as varchar(30)
	Declare @Cantidad 			as float
	Declare @nro_serie 			as varchar(50)
	Declare @nro_lote 			as varchar(50)
	Declare @fecha_vencimiento  as datetime
	Declare @nro_despacho 		as varchar(50)
	Declare @nro_bulto 			as varchar(50)
	Declare @nro_partida 		as varchar(50)
	Declare @peso 				as float
	Declare @volumen 			as numeric(20,5)
	Declare @cat_log_id 		as varchar(50)
	Declare @prop1 				as varchar(100)
	Declare @prop2 				as varchar(100)
	Declare @prop3 				as varchar(100)
	Declare @fecha_cpte 		as datetime
	Declare @fecha_alta_gtw 	as datetime
	Declare @unidad_id 			as varchar(5)
	Declare @unidad_peso 		as varchar(5)
	Declare @Unidad_volumen 	as varchar(5)
	Declare @est_merc_id 		as varchar(15)
	Declare @moneda_id 			as varchar(50)
	Declare @costo 				as numeric(10,3)
	------------------------------------------------
	--			Cursor Reservados
	------------------------------------------------
	Declare @cliente_idR			as varchar(15)
	Declare @producto_idR			as varchar(30)
	Declare @cantidadR 				as float
	Declare @nro_serieR				as varchar(50)
	Declare @nro_loteR				as varchar(50)
	Declare @fecha_vencimientoR		Datetime
	Declare @nro_despachoR			as varchar(50)
	Declare @nro_bultoR				as varchar(50)
	Declare @nro_partidaR			as varchar(50)
	Declare @pesoR					numeric(20,5)
	Declare @volumenR				numeric(20,5)
	Declare @tie_inR				as varchar(1)
	Declare	@cant_dispR				as numeric(20,5)
	Declare @codeR					as varchar(1)
	Declare @descriptionR			as varchar(100)
	Declare @cat_log_id_finalR		as varchar(50)
	Declare @prop1R					as varchar(100)
	Declare @prop2R					as varchar(100)
	Declare @prop3R					as varchar(100)
	Declare @unidad_idR				as varchar(5)
	Declare @unidad_pesoR			as varchar(5)
	Declare @unidad_volumenR		as varchar(5)
	Declare @est_merc_idR			as varchar(15)
	Declare @moneda_idR				as varchar(50)
	Declare @costoR					as float
	Declare @ordenR					as varchar(10)
	------------------------------------------------
	--			Cursores
	------------------------------------------------
	Declare @pAux					Cursor
	Declare @Cexistencia			Cursor
	Declare @Creservados			Cursor
	Declare @C_egr2					Cursor
	------------------------------------------------
	--	Definicion de parametros
	------------------------------------------------
	Declare @ParmDefinition 		nvarchar(500)

	------------------------------------------------


	Create table #tmp_Q1(
		 Rl_Id 				numeric(20,0)
		,Cliente_id 		varchar(15) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Producto_id		varchar(30) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Cantidad			numeric(20,5)
		,Nro_Serie			varchar(50) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Nro_Lote			varchar(50) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Fecha_Vencimiento	Datetime
		,Nro_Despacho		varchar(50) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Nro_Bulto			Varchar(50) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Nro_Partida		varchar(50) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Peso				numeric(20,5)
		,Volumen			numeric(20,5)
		,Cat_log_Id			varchar(50) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Prop1				varchar(100) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Prop2				varchar(100) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Prop3				varchar(100) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Fecha_Cpte			datetime
		,Fecha_Alta_Gtw		datetime	
		,Unidad_id			varchar(5)		COLLATE SQL_Latin1_General_CP1_CI_AS
		,Unidad_Peso		varchar(5) 		COLLATE SQL_Latin1_General_CP1_CI_AS
		,Unidad_Volumen		varchar(5) 		COLLATE SQL_Latin1_General_CP1_CI_AS
		,Est_Merc_id		varchar(15) 	COLLATE SQL_Latin1_General_CP1_CI_AS
		,Moneda_id			varchar(20)		COLLATE SQL_Latin1_General_CP1_CI_AS
		,Costo				numeric(10,3)
		);

	--Truncate Table #tmp_Q1

	Select 	@vDepositoId= Deposito_Default from #Temp_Usuario_loggin;

	Set @intVarSuma =0
	Set @intVarReservados =0
	Set @intVarContador =0



	Set @Abierto='0'
      
    Truncate table #Temp_Existencia_locator;
    Truncate table #TEmp_Existencia_locator_Rl;

	Set @pAux=Cursor For
		select 	criterio_id,order_id,forma_id
		from 	sys_criterio_locator
		where 	cliente_id =ltrim(rtrim(upper(@p_cliente_id))) 
				and producto_id =ltrim(rtrim(upper(@p_producto_id)))
				and criterio_id <> 'ORDEN_PICKING'
		order by posicion_id
	
	Open @pAux

	Set  @StrSqlOrderBy='ORDER BY '

	Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	While @@Fetch_Status=0
	Begin
		if @Forma_id='TO_NUMBER'
		Begin
			Set	@StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(NUMERIC(20, 5), CASE WHEN ISNUMERIC(' + @Criterio_id + ') = 1 THEN ' + @CRITERIO_ID + ' ELSE NULL END) ' + @ORDER_ID + ', '
		End
		Else
		Begin
			if @Forma_id='TO_CHAR'
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + ' ' + @Criterio_id + ' ' + @Order_id + ', '
			End
			Else
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(DATETIME, ' + ' (' + @CRITERIO_ID + ')) ' + @ORDER_ID + ', '
			End	
		End				
		Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	End --fin While @pAux

	Close @pAux
	Deallocate @pAux

	If @StrSqlOrderBy <> 'ORDER BY '
	Begin
		Set @StrSqlOrderBy = Substring(@StrSqlOrderBy, 1, Len(@StrSqlOrderBy) - 1)
	End
    Else
	Begin
		Set @StrSqlOrderBy = ''
	End


	Set @p_fecha_vto_dde = @p_fecha_vencimiento

			  Set @strsql = 'SELECT X.* FROM (' + Char(13)
	Set @strsql = @strsql + 'SELECT RL.RL_ID, ' + Char(13)
	Set @strsql = @strsql + 'DD.CLIENTE_ID, ' + Char(13)
	Set @strsql = @strsql + 'DD.PRODUCTO_ID, ' + Char(13)
	Set @strsql = @strsql + 'SUM(IsNull(RL.CANTIDAD, 0)) as cantidad, ' + Char(13)
	Set @strsql = @strsql + 'DD.NRO_SERIE, ' + Char(13)
	Set @strsql = @strsql + 'DD.NRO_LOTE, ' + Char(13)
	Set @strsql = @strsql + 'DD.FECHA_VENCIMIENTO, ' + Char(13)
	Set @strsql = @strsql + 'DD.NRO_DESPACHO, ' + Char(13)
	Set @strsql = @strsql + 'DD.NRO_BULTO, ' + Char(13)
	Set @strsql = @strsql + 'DD.NRO_PARTIDA, ' + Char(13)
	Set @strsql = @strsql + 'DD.PESO,DD.VOLUMEN, ' + Char(13)
	Set @strsql = @strsql + 'RL.CAT_LOG_ID, ' + Char(13)
	Set @strsql = @strsql + 'DD.PROP1, ' + Char(13)
	Set @strsql = @strsql + 'DD.PROP2, ' + Char(13)
	Set @strsql = @strsql + 'DD.PROP3, ' + Char(13)
	Set @strsql = @strsql + 'D.FECHA_CPTE, ' + Char(13)
	Set @strsql = @strsql + 'D.FECHA_ALTA_GTW, ' + Char(13)
	Set @strsql = @strsql + 'DD.UNIDAD_ID, ' + Char(13)
	Set @strsql = @strsql + 'DD.UNIDAD_PESO, ' + Char(13)
	Set @strsql = @strsql + 'DD.UNIDAD_VOLUMEN, ' + Char(13)
	Set @strsql = @strsql + 'RL.EST_MERC_ID, ' + Char(13)
	Set @strsql = @strsql + 'DD.MONEDA_ID, ' + Char(13)
	Set @strsql = @strsql + 'DD.COSTO ' + Char(13)
	Set @strsql = @strsql + 'FROM  RL_DET_DOC_TRANS_POSICION RL ' + Char(13)
	Set @strsql = @strsql + 'INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND  RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS AND RL.DISPONIBLE = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @strsql = @strsql + 'INNER JOIN CATEGORIA_LOGICA           CL ON CL.CLIENTE_ID = RL.CLIENTE_ID AND CL.CAT_LOG_ID = RL.CAT_LOG_ID AND CL.DISP_EGRESO = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @strsql = @strsql + 'INNER JOIN DOCUMENTO_TRANSACCION      DT ON DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID ' + Char(13)
	Set @strsql = @strsql + 'INNER JOIN DET_DOCUMENTO              DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA ' + Char(13)
	Set @strsql = @strsql + 'INNER JOIN DOCUMENTO                   D ON DD.DOCUMENTO_ID = D.DOCUMENTO_ID ' + Char(13)
	Set @strsql = @strsql + 'INNER JOIN PRODUCTO                 PROD ON PROD.CLIENTE_ID = DD.CLIENTE_ID AND PROD.PRODUCTO_ID = DD.PRODUCTO_ID ' + Char(13)
	Set @strsql = @strsql + 'INNER JOIN CLIENTE                     C ON C.CLIENTE_ID = DD.CLIENTE_ID ' + Char(13)
	Set @strsql = @strsql + 'LEFT JOIN ESTADO_MERCADERIA_RL     EMRL ON EMRL.CLIENTE_ID = DD.CLIENTE_ID AND EMRL.EST_MERC_ID = RL.EST_MERC_ID ' + Char(13)
	Set @strsql = @strsql + 'LEFT JOIN NAVE                        N ON RL.NAVE_ACTUAL = N.NAVE_ID ' + Char(13)
	Set @strsql = @strsql + 'LEFT JOIN POSICION                    P ON RL.POSICION_ACTUAL = P.POSICION_ID ' + Char(13)
	Set @strsql = @strsql + 'LEFT JOIN NAVE                       N2 ON P.NAVE_ID = N2.NAVE_ID ' + Char(13)
	Set @strsql = @strsql + 'LEFT JOIN CALLE_NAVE               CALN ON P.CALLE_ID = CALN.CALLE_ID ' + Char(13)
	Set @strsql = @strsql + 'LEFT JOIN COLUMNA_NAVE             COLN ON P.COLUMNA_ID = COLN.COLUMNA_ID ' + Char(13)
	Set @strsql = @strsql + 'LEFT JOIN NIVEL_NAVE                 NN ON P.NIVEL_ID = NN.NIVEL_ID ' + Char(13)
			Set  @StrSql1 = ' WHERE C.CLIENTE_ID = ' + Char(39) + @P_CLIENTE_ID + Char(39) + Char(13)
	Set @StrSql1 = @StrSql1 + 'AND PROD.PRODUCTO_ID = ' + Char(39) + @P_PRODUCTO_ID + Char(39) + Char(13)
	Set @StrSql1 = @StrSql1 + 'AND RL.RL_ID NOT IN (SELECT RL_ID FROM #TEMP_RL_EXISTENCIA_DOC) ' + Char(13)
	Set @StrSql1 = @StrSql1 + 'AND IsNull(N.DISP_EGRESO, IsNull(N2.DISP_EGRESO, ' + Char(39) + '1' + Char(39) + ')) = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @StrSql1 = @StrSql1 + 'AND IsNull(EMRL.DISP_EGRESO, ' + Char(39) + '1' + Char(39) + ') = ' + Char(39) + '1' + Char(39) + Char(13)
	
	Set @StrSql1 = @StrSql1 + 'AND IsNull(N.DEPOSITO_ID, N2.DEPOSITO_ID) = ' + char(39) + @vDepositoID + Char(39) + Char(13)
	Set @StrSql1 = @StrSql1 + 'AND ' + Char(13)
	Set @StrSql1 = @StrSql1 + '(SELECT CASE WHEN (Count(posicion_id)) > 0 THEN 1 ELSE 0 END' + Char(13)
	Set @StrSql1 = @StrSql1 + 'FROM   rl_posicion_prohibida_cliente' + Char(13)
	Set @StrSql1 = @StrSql1 + 'WHERE  Posicion_ID = IsNull(P.NAVE_ID, 0)' + Char(13)
	Set @StrSql1 = @StrSql1 + 'AND cliente_id = DD.CLIENTE_ID' + Char(13)
	Set @StrSql1 = @StrSql1 + ') = 0' + Char(13)
	Set @StrSql1 = @StrSql1 + 'AND IsNull(P.POS_LOCKEADA, ' + Char(39) + '0' + Char(39) + ') = ' + Char(39) + '0' + Char(39) + char(13)
	
	If @p_nro_serie is not null
	Begin
        Set @StrSql1 = @StrSql1 + 'AND dd.nro_serie = ' + Char(39) + @p_nro_serie + Char(39) + Char(13)
	End
	
    If @p_nro_partida is not null 
	Begin
        Set @StrSql1 = @StrSql1 + 'AND dd.nro_partida = ' + Char(39) + @p_nro_partida + Char(39) + Char(13)
	End

	If @p_nro_lote is not null
	Begin
		Set @StrSql1 = @StrSql1 + 'AND dd.nro_lote = ' + Char(39) + @p_nro_lote + Char(39) + Char(13)
	End

	If 	@p_Nro_Despacho is not null 
	Begin
	    Set @StrSql1 = @StrSql1 + 'AND dd.nro_despacho = ' + Char(39) + @p_Nro_Despacho + Char(39) + Char(13)
	End

	If @p_Nro_Bulto is not null 
	Begin
		Set @StrSql1 = @StrSql1 + 'AND dd.Nro_Bulto = ' + Char(39) + @p_Nro_Bulto + Char(39) + Char(13)
	End

	If @p_CatLog_ID is not null 
	Begin
	    Set @StrSql1 = @StrSql1 + 'AND rl.cat_log_id = ' + Char(39) + @p_CatLog_ID + Char(39) + Char(13)
	End

	If @P_PROP1 is not null 
	Begin
	    Set @StrSql1 = @StrSql1 + 'AND dd.prop1 = ' + Char(39) + @P_PROP1 + Char(39) + Char(13)
	End

	If @p_Prop2 is not null
	Begin
	    Set @StrSql1 = @StrSql1 + 'AND dd.prop2 = ' + Char(39) + @p_Prop2 + Char(39) + Char(13)
	End

	If @p_Prop3 is not null
	Begin
	    Set @StrSql1 = @StrSql1 + 'AND dd.prop3 = ' + Char(39) + @p_Prop3 + Char(39) + Char(13)
	End

	If @p_fecha_vto_dde is not null And @p_fecha_vto_dde <> '' 
	Begin
		Set @p_fecha_vto_hta = Cast(@p_fecha_vencimiento as datetime)+ 1
		Set @StrSql1 = @StrSql1 + 'AND Convert(varchar,dd.fecha_vencimiento,101)' + Char(13)
		Set @StrSql1 = @StrSql1 + 'BETWEEN Convert(Varchar,Cast(' + Char(39) + Cast(@p_fecha_vto_dde as Varchar(17)) + Char(39) +' as Datetime),101)' +Char(13)
		Set @StrSql1 = @StrSql1 + 'AND Convert(Varchar,Cast(' + Char(39) + Cast(@p_fecha_vto_hta as varchar(17))+ Char(39) +' as Datetime),101)' + Char(13)
		--CONVERT(VARCHAR, @sParametro ,101)
	End

	If @P_UNIDAD_ID is not null
	Begin
		Set @StrSql1 = @StrSql1 + 'AND dd.unidad_id = ' + Char(39) + @P_UNIDAD_ID + Char(39) + Char(13)
	End

	If @p_Est_Merc_Id is not null
	Begin
        Set @StrSql1 = @StrSql1 + 'AND rl.est_merc_id = ' + Char(39) + @p_Est_Merc_Id + Char(39) + Char(13)
	End
	
    If @p_Ilimitado = '0'
	Begin
        --'Significa que es Limitado a la posicion donde esta el Producto
        If @p_posicion_id is not null
		Begin
            Set @StrSql1 = @StrSql1 + 'AND  p.posicion_id = ' + Char(39) + @p_posicion_id + Char(39) + Char(13)
		End
        Else
		Begin
            If @p_nave_id is not null
			Begin
                Set @StrSql1 = @StrSql1 + 'AND IsNull(n2.NAVE_ID, n.NAVE_ID) = ' + Char(39) + @p_nave_id + Char(39) + Char(13)
       		End
        End 
	End

	Set @StrSql1 = @StrSql1 + ' GROUP BY ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.CLIENTE_ID, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.PRODUCTO_ID, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.NRO_SERIE, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.NRO_LOTE, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.FECHA_VENCIMIENTO, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.NRO_DESPACHO, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.NRO_BULTO, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.NRO_PARTIDA, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.PESO, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.VOLUMEN, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' RL.CAT_LOG_ID, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.PROP1, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.PROP2, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.PROP3, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' D.FECHA_CPTE, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' D.FECHA_ALTA_GTW, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.UNIDAD_ID, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.UNIDAD_PESO, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.UNIDAD_VOLUMEN, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' RL.EST_MERC_ID, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.MONEDA_ID, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' DD.COSTO, ' + Char(13)
	Set @StrSql1 = @StrSql1 + ' RL.RL_ID ' + Char(13)

	If @p_doc_trans_id is not null
	Begin
		Set @NUnion=' Union ' + Char(13)
	    Set @NStrSql = ' SELECT RL.RL_ID, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.CLIENTE_ID, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.PRODUCTO_ID, ' + Char(13)
		Set @NStrSql = @NStrSql + 'SUM(IsNull(RL.CANTIDAD,0)) as cantidad, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.NRO_SERIE, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.NRO_LOTE, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.FECHA_VENCIMIENTO, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.NRO_DESPACHO, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.NRO_BULTO, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.NRO_PARTIDA, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.PESO,DD.VOLUMEN, ' + Char(13)
		Set @NStrSql = @NStrSql + 'RL.CAT_LOG_ID_FINAL, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.PROP1, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.PROP2, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.PROP3, ' + Char(13)
		Set @NStrSql = @NStrSql + 'D.FECHA_CPTE, ' + Char(13)
		Set @NStrSql = @NStrSql + 'D.FECHA_ALTA_GTW, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.UNIDAD_ID, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.UNIDAD_PESO, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.UNIDAD_VOLUMEN, ' + Char(13)
		Set @NStrSql = @NStrSql + 'RL.EST_MERC_ID, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.MONEDA_ID, ' + Char(13)
		Set @NStrSql = @NStrSql + 'DD.COSTO ' + Char(13)
		Set @NStrSql = @NStrSql + 'FROM  RL_DET_DOC_TRANS_POSICION RL ' + Char(13)
		Set @NStrSql = @NStrSql + 'INNER JOIN DET_DOCUMENTO_TRANSACCION DDT ON RL.DOC_TRANS_ID = DDT.DOC_TRANS_ID AND  RL.NRO_LINEA_TRANS = DDT.NRO_LINEA_TRANS AND RL.DISPONIBLE =' + Char(39) + '0' + Char(39) + Char(13)
		Set @NStrSql = @NStrSql + 'INNER JOIN CATEGORIA_LOGICA           CL ON CL.CLIENTE_ID = RL.CLIENTE_ID AND CL.CAT_LOG_ID = RL.CAT_LOG_ID_final AND CL.DISP_EGRESO =' + Char(39) + '1' + Char(39) + Char(13)
		Set @NStrSql = @NStrSql + 'INNER JOIN DOCUMENTO_TRANSACCION      DT ON DDT.DOC_TRANS_ID = DT.DOC_TRANS_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'INNER JOIN DET_DOCUMENTO              DD ON DDT.DOCUMENTO_ID = DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC = DD.NRO_LINEA ' + Char(13)
		Set @NStrSql = @NStrSql + 'INNER JOIN DOCUMENTO                   D ON DD.DOCUMENTO_ID = D.DOCUMENTO_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'INNER JOIN PRODUCTO                 PROD ON PROD.CLIENTE_ID = DD.CLIENTE_ID AND PROD.PRODUCTO_ID = DD.PRODUCTO_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'INNER JOIN CLIENTE                     C ON C.CLIENTE_ID = DD.CLIENTE_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'LEFT JOIN ESTADO_MERCADERIA_RL     EMRL ON EMRL.CLIENTE_ID = DD.CLIENTE_ID AND EMRL.EST_MERC_ID = RL.EST_MERC_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'LEFT JOIN NAVE                        N ON RL.NAVE_ANTERIOR = N.NAVE_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'LEFT JOIN POSICION                    P ON RL.POSICION_ANTERIOR = P.POSICION_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'LEFT JOIN NAVE                       N2 ON P.NAVE_ID = N2.NAVE_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'LEFT JOIN CALLE_NAVE               CALN ON P.CALLE_ID = CALN.CALLE_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'LEFT JOIN COLUMNA_NAVE             COLN ON P.COLUMNA_ID = COLN.COLUMNA_ID ' + Char(13)
		Set @NStrSql = @NStrSql + 'LEFT JOIN NIVEL_NAVE                 NN ON P.NIVEL_ID = NN.NIVEL_ID ' + Char(13)
		Set @NStrSql1 = ' WHERE C.CLIENTE_ID = '+ Char(39) + @p_cliente_id + Char(39) + Char(13)
		Set @NStrSql1 = @NStrSql1 + 'AND PROD.PRODUCTO_ID = ' + Char(39) + @p_producto_id + Char(39) + Char(13)
		Set @NStrSql1 = @NStrSql1 + 'AND RL.RL_ID NOT IN (SELECT RL_ID FROM #TEMP_RL_EXISTENCIA_DOC) ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + 'AND IsNull(N.DISP_EGRESO, IsNull(N2.DISP_EGRESO, ' + Char(39) + '1' + Char(39) + ')) = ' + Char(39) + '1' + Char(39) + Char(13)

		Set @NStrSql1 = @NStrSql1 + 'AND IsNull(EMRL.DISP_EGRESO, ' + Char(39) + '1' + Char(39) + ') = ' + Char(39) + '1' + Char(39) + Char(13)

		Set @NStrSql1 = @NStrSql1 + 'AND IsNull(N.DEPOSITO_ID, N2.DEPOSITO_ID) = ' + Char(39) + @vDepositoID + Char(39) + char(13)
		Set @NStrSql1 = @NStrSql1 + 'AND ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + '(SELECT CASE WHEN (Count(posicion_id)) > 0 THEN 1 ELSE 0 END' + Char(13)
		Set @NStrSql1 = @NStrSql1 + 'FROM   rl_posicion_prohibida_cliente' + Char(13)
		Set @NStrSql1 = @NStrSql1 + 'WHERE  Posicion_ID = IsNull(P.NAVE_ID, 0)' + Char(13)
		Set @NStrSql1 = @NStrSql1 + 'AND cliente_id = DD.CLIENTE_ID' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ') = 0' + Char(13)
		Set @NStrSql1 = @NStrSql1 + 'AND (IsNull(P.POS_LOCKEADA, ' + Char(39) + '0' + Char(39) +') = ' + Char(39) + '0' + Char(39) + Char(13)
		Set @NStrSql1 = @NStrSql1 + 'OR P.LCK_DOC_TRANS_ID = '  + Cast(@p_doc_trans_id as nvarchar(20))+  ')' + Char(13)

		If @p_nro_serie is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND dd.nro_serie = ' + char(39) + @p_nro_serie + Char(39) + Char(13)
		End

		If @p_nro_partida is not null
		Begin
		    Set @NStrSql1 = @NStrSql1 + 'AND dd.nro_partida = ' + Char(39) + @p_nro_partida + Char(39) + Char(13)
		End

		If @p_nro_lote is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND dd.nro_lote = ' + Char(39) + @p_nro_lote + Char(39)
		End

		If @p_Nro_Despacho is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND dd.nro_despacho = ' + Char(39) + @p_Nro_Despacho + Char(39) + Char(13)
		End
		
		If @p_Nro_Bulto is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND dd.Nro_Bulto = ' + Char(39) + @p_Nro_Bulto + Char(39) + Char(13)
		End

		If @p_CatLog_ID is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND rl.cat_log_id_final = ' + Char(39) + @p_CatLog_ID + Char(39) + Char(13)
		End

		If @P_PROP1 is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND dd.prop1 = ' + Char(39) + @P_PROP1 + Char(39) + Char(13)
		End

		If @P_PROP2 is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND dd.prop2 = ' + Char(39) + @P_PROP2 + Char(39) + Char(13)
		End

		If @P_PROP3 is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND dd.prop3 = ' + Char(39) + @P_PROP3 + Char(39) + Char(13)
		End

		If @P_FECHA_VTO_DDE is not null And @P_FECHA_VTO_DDE <> ''
		Begin
			Set @p_fecha_vto_hta = Cast(@p_fecha_vencimiento as datetime)+ 1
			Set @NStrSql1 = @NStrSql1 + 'AND Convert(Varchar,dd.fecha_vencimiento,101)' + Char(13)
			Set @NStrSql1 = @NStrSql1 + 'BETWEEN Convert(Varchar,Cast(' + Char(39) + Cast(@p_fecha_vto_dde as varchar(17)) + Char(39) +'as Datetime),101)' +Char(13)
			Set @NStrSql1 = @NStrSql1 + 'AND Convert(Varchar,Cast(' + Char(39) + Cast(@p_fecha_vto_hta as varchar(17)) + Char(39) +' as Datetime),101)' + Char(13)
		End

		If @p_unidad_id is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND dd.unidad_id = ' + CHar(39) + @p_unidad_id + Char(39) + Char(13)
		End
		
		If @p_Est_Merc_Id is not null
		Begin
			Set @NStrSql1 = @NStrSql1 + 'AND rl.est_merc_id = ' + Char(39) + @p_Est_Merc_Id + Char(39) + Char(13)
		End

		If @p_Ilimitado = '0'
		Begin
			If @p_posicion_id is not null
			Begin
				Set @NStrSql1 = @NStrSql1 + 'AND  p.posicion_id = ' + Char(39) + @p_posicion_id + Char(39) + Char(13)
			End
			Else
			Begin
				If @p_nave_id is not null
				Begin
					Set @NStrSql1 = @NStrSql1 + 'AND IsNull(n2.NAVE_ID, n.NAVE_ID) = ' + Char(39) + @p_nave_id + Char(39) + Char(13)
				End
			End 
		End
		Set @NStrSql1 = @NStrSql1 + ' GROUP BY ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.CLIENTE_ID, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.PRODUCTO_ID, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.NRO_SERIE, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.NRO_LOTE, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.FECHA_VENCIMIENTO, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.NRO_DESPACHO, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.NRO_BULTO, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.NRO_PARTIDA, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.PESO, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.VOLUMEN, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' RL.CAT_LOG_ID_FINAL, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.PROP1, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.PROP2, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.PROP3, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' D.FECHA_CPTE, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' D.FECHA_ALTA_GTW, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.UNIDAD_ID, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.UNIDAD_PESO, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.UNIDAD_VOLUMEN, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' RL.EST_MERC_ID, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.MONEDA_ID, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' DD.COSTO, ' + Char(13)
		Set @NStrSql1 = @NStrSql1 + ' RL.RL_ID ' + Char(13)
	End --end if @p_doc_trans_id

	Set @StrX = ')X ' + Char(13)

	If @p_doc_trans_id is not null
	Begin
		--Armo el string a ejecutar
		If @StrSql1 is null
		Begin
			Set @StrSql1=''
		End
		If @NUnion is null
		Begin
			Set @NUnion=''
		End
		If @NStrSql is null
		Begin
			Set @NStrSql=''
		End
		If @NStrSql1 is null
		Begin
			Set @NStrSql1=''
		End
		If @StrX is null
		Begin
			Set @StrX=''
		End
		If @StrSqlOrderBy is null
		Begin
			Set @StrSqlOrderBy=''
		End 
		Set @xSQL='Insert into #tmp_Q1  ' + Char(13) + @strsql 
		Set @xSQL = @xSQL + @StrSql1 
		Set @xSQL = @xSQL + @NUnion 
		Set @xSQL = @xSQL + @NStrSql 
		Set @xSQL = @xSQL + @NStrSql1
		Set @xSQL = @xSQL + @StrX 
		Set @xSQL = @xSQL + @StrSqlOrderBy 

		--Lo ejecuto
		EXECUTE (@xSQL)
		--Seteo el Cursor
		Set @CExistencia=Cursor For
			Select *
			From #Tmp_Q1
		--Lo Abro.
		Open @CExistencia
	End
	Else
	Begin

		Set @xSQL='Insert into #tmp_Q1 ' + Char(13) +@strsql + @StrSql1 + @StrX + @StrSqlOrderBy

		Execute @xSQL

		Set @CExistencia=Cursor For
			Select *
			From #Tmp_Q1

		Open @CExistencia
	End
	
	Fetch Next From @Cexistencia into    @rl_id			,@ClienteID		,@Productoid	,@Cantidad
										,@nro_serie		,@nro_lote		,@fecha_vencimiento
										,@nro_despacho	,@nro_bulto		,@nro_partida	,@peso
										,@volumen		,@cat_log_id	,@prop1			,@prop2
										,@prop3			,@fecha_cpte	,@fecha_alta_gtw
										,@unidad_id		,@unidad_peso	,@Unidad_volumen
										,@est_merc_id	,@moneda_id		,@costo
	While @@Fetch_Status=0
	Begin
		
                   Set @StrSql1 = 'SELECT t2.cliente_id, t2.producto_id, sum(IsNull(t2.cantidad,0)) as cantidad, t2.nro_serie, t2.nro_lote,t2.fecha_vencimiento, t2.nro_despacho, ' + Char(13)
        Set @StrSql1 = @StrSql1 + 't2.nro_bulto, t2.nro_partida,t2.peso,t2.volumen,t2.tie_in,null as cant_disp,null as code,null as description,t2.cat_log_id_final as Cat_Log_Id_Final, ' + Char(13)
        Set @StrSql1 = @StrSql1 + 't2.prop1, t2.prop2, t2.prop3,t2.unidad_id,t2.unidad_peso,t2.unidad_volumen, t2.est_merc_id, t2.moneda_id, t2.costo, null as orden ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'FROM (SELECT dd.cliente_id,dd.producto_id,sum(IsNull(dd.cantidad,0)) AS cantidad,dd.nro_serie,dd.nro_lote,dd.nro_partida, ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'dd.nro_despacho,dd.nro_bulto,dd.Fecha_vencimiento,dd.peso,dd.volumen,dd.tie_in,dd.cat_log_id_final, ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'dd.prop1,dd.prop2,dd.prop3,dd.unidad_id,dd.unidad_peso,dd.unidad_volumen,dd.est_merc_id,dd.moneda_id,dd.costo ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'FROM documento d, det_documento dd , categoria_logica cl ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'WHERE d.documento_id = dd.documento_id AND dd.cliente_id = cl.cliente_id AND dd.cat_log_id = cl.cat_log_id AND d.status = ' + Char(39) + 'D20' + Char(39) +' AND cl.categ_stock_id = ' + Char(39) + 'TRAN_EGR' + Char(39) + Char(13)

        If @p_documento_id is not null
		Begin
			Set @StrSql1 = @StrSql1 + ' AND dd.documento_id <> ' + Cast(@p_documento_id as varchar(20)) + Char(13)
        End
        Set @StrSql1 = @StrSql1 + 'GROUP BY dd.cliente_id ,dd.producto_id ,dd.nro_serie,dd.nro_lote,dd.nro_partida,dd.nro_despacho,dd.nro_bulto, ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'dd.fecha_vencimiento ,dd.peso,dd.volumen,dd.tie_in,dd.cat_log_id_final,dd.prop1,dd.prop2,dd.prop3,dd.unidad_id,dd.unidad_peso,dd.unidad_volumen,dd.est_merc_id,dd.moneda_id,dd.costo ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'UNION ALL ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'SELECT dd.cliente_id,dd.producto_id,sum(IsNull(dd.cantidad,0)) AS cantidad,dd.nro_serie,dd.nro_lote, ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'dd.nro_partida,dd.nro_despacho,dd.nro_bulto,dd.Fecha_vencimiento,dd.peso,dd.volumen,dd.tie_in,dd.cat_log_id_final, ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'dd.prop1,dd.prop2,dd.prop3,dd.unidad_id,dd.unidad_peso,dd.unidad_volumen,dd.est_merc_id,dd.moneda_id,dd.costo ' + Char(13)
        Set @StrSql1 = @StrSql1 + 'FROM det_documento dd , categoria_logica cl, det_documento_transaccion ddt, documento_transaccion dt ' + Char(13)
        
                   Set @StrSql2 = 'WHERE ddt.cliente_id=cl.cliente_id AND ddt.cat_log_id=cl.cat_log_id AND cl.categ_stock_id = ' + Char(39) + 'TRAN_EGR' + Char(39) + Char(13)
        Set @StrSql2 = @StrSql2 + ' AND dd.cliente_id = cl.cliente_id AND   ddt.documento_id = dd.documento_id AND ddt.nro_linea_doc = dd.nro_linea AND dt.doc_trans_id = ddt.doc_trans_id ' + Char(13)
        Set @StrSql2 = @StrSql2 + ' AND dt.status = ' + Char(39) + 'T10' + Char(39) + Char(13)

        If @p_documento_id is not null
		Begin
            Set @StrSql2 = @StrSql2 + ' AND dd.documento_id <> ' + Cast(@p_documento_id as varchar(20))+ Char(13)
        End
        
        If @p_doc_trans_id is not null
		Begin
            Set @StrSql2 = @StrSql2 + ' AND dT.doc_trans_id = ' + Cast(@p_doc_trans_id as varchar(20)) + Char(13)
		End
                    Set @StrSql2a = ' AND not EXISTS (SELECT rl_id FROM rl_det_doc_trans_posicion rl WHERE rl.doc_trans_id_egr = ddt.doc_trans_id AND rl.nro_linea_trans_egr = ddt.nro_linea_trans) ' + Char(13)
        Set @StrSql2a = @StrSql2a + ' GROUP BY dd.cliente_id ,dd.producto_id , dd.nro_serie, dd.nro_lote,dd.nro_partida,dd.nro_despacho,dd.nro_bulto,dd.fecha_vencimiento,dd.peso,dd.volumen,dd.tie_in,dd.cat_log_id_final, ' + Char(13)
        Set @StrSql2a = @StrSql2a + ' dd.prop1,dd.prop2,dd.prop3,dd.unidad_id,dd.unidad_peso,dd.unidad_volumen,dd.est_merc_id,dd.moneda_id,dd.costo ) T2 ' + Char(13)
        Set @StrSql2a = @StrSql2a + ' WHERE 1 <> 0 ' + Char(13)

        If @ClienteID is not null
		Begin
			Set @StrSql3 = ' AND t2.cliente_id = ' + Char(39) + @ClienteID + Char(39) + Char(13)
		End

		If @Productoid is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.producto_id = ' + Char(39) + @Productoid + Char(39) + Char(13)
		End
		
		If @nro_serie is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.nro_serie = ' + Char(39) + @nro_serie + Char(39) + Char(13)
			Set @vNro_Serie = @nro_serie
		End

		If @nro_partida is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.nro_partida = ' + Char(39) + @nro_partida + Char(39) + Char(13)
			Set @vNro_Partida = @nro_partida
		End

	    If @nro_lote is not null
		Begin
            Set @StrSql3 = @StrSql3 + ' AND t2.nro_lote = ' + Char(39) + @nro_lote + Char(39) + Char(13)
      	    Set @vNro_Lote = @nro_lote
		End

        If @nro_despacho is not null
		Begin
            Set @StrSql3 = @StrSql3 + ' AND t2.nro_despacho = ' + Char(39) + @nro_despacho + Char(39) + Char(13)
			Set @vNro_Despacho = @nro_despacho
		End

		If @nro_bulto is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.nro_bulto = ' + Char(39) + @nro_bulto + Char(39) + Char(13)
			Set @vNro_Bulto = @nro_bulto
		End

		If @cat_log_id is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.cat_log_id_final = ' + Char(39) + @cat_log_id + Char(39) + Char(13)
			Set @vCat_Log = @cat_log_id
		End

		If @prop1 is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.prop1 = ' + Char(39) + @prop1 + Char(39) + Char(13)
			Set @vProp1 = @prop1
		End

		If @prop2 is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.prop2 = ' + Char(39) + @prop2 + Char(39) + Char(13)
			Set @vProp2 = @prop2
		End

		If @prop3 is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.prop3 = ' + Char(39) + @prop3 + Char(39) + Char(13)
			Set @vProp3 = @prop3
		End

		If @fecha_vencimiento is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND Convert(Varchar,t2.fecha_vencimiento,101)= Convert(Varchar, Cast(' + Char(39) + Cast(@fecha_vencimiento as varchar(17)) + Char(39) + ' as Datetime),101)' + Char(13)
			Set @vFecha_Vencimiento = @fecha_vencimiento
		End 

		If @unidad_id is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.unidad_id = ' + Char(39) + @unidad_id + Char(39) + Char(13)
			Set @vUnidad_Id = @unidad_id
		End

		If @est_merc_id is not null
		Begin
			Set @StrSql3 = @StrSql3 + ' AND t2.est_merc_id = ' + Char(39) + @est_merc_id + Char(39) + Char(13)
			Set @vEst_Merc_Id = @est_merc_id
		End

        Set @StrSql3 = @StrSql3 + ' GROUP BY t2.cliente_id, t2.producto_id,t2.nro_serie, t2.nro_lote,t2.fecha_vencimiento, t2.nro_despacho, t2.nro_bulto, t2.nro_partida, ' + Char(13)
        Set @StrSql3 = @StrSql3 + ' t2.peso,t2.volumen,t2.tie_in, t2.cat_log_id_final , t2.prop1, t2.prop2, t2.prop3,t2.unidad_id,t2.unidad_peso,t2.unidad_volumen,t2.est_merc_id,t2.moneda_id,t2.costo ' + Char(13)
		--------------------------------------------------------------
		--Por Documento_id
		--------------------------------------------------------------
        If @P_DOCUMENTO_ID is not null And @P_DOC_TRANS_ID is null --Ok
		Begin
			If @Abierto='1'
			Begin
				Close @Creservados
				Deallocate @Creservados
				Set @Abierto='0'
			End
			Set @nSQL= N'SET @Creservados = CURSOR FOR ' + cast(@StrSql1 as nvarchar(4000)) + cast(@StrSql2 as nvarchar(4000)) + cast(@StrSql2a as nvarchar(4000))+ Cast(@StrSql3 as nvarchar(4000)) + '; Open @Creservados'
			Set @ParmDefinition=N'@Creservados Cursor Output'
			
			EXEC sp_executesql @nSQL,@ParmDefinition,@Creservados=@Creservados Output
			Set @Abierto='1'
        End 


        If @P_DOCUMENTO_ID is null And @P_DOC_TRANS_ID is not null --Ok
		Begin
			If @Abierto='1'
			Begin
				Close @Creservados
				Deallocate @Creservados
				Set @Abierto='0'
			End
			Set @nSQL= N'SET @Creservados = CURSOR FOR ' + @StrSql1 + @StrSql2 + @StrSql2a + @StrSql3 + '; OPEN @Creservados'
			Set @ParmDefinition=N'@Creservados Cursor Output'
			EXEC sp_executesql @nSQL,@ParmDefinition,@Creservados=@Creservados Output
			Set @Abierto='1'			
        End

        --Por Ambos
        If @P_DOCUMENTO_ID is not null And @P_DOC_TRANS_ID is not null
		Begin
			If @Abierto='1'
			Begin
				Close @Creservados
				Deallocate @Creservados
				Set @Abierto='0'
			End
			Set @nSQL= N'SET @Creservados = CURSOR FOR ' + @StrSql1 + @StrSql2 + @StrSql2a + @StrSql3 + '; OPEN @Creservados'
			Set @ParmDefinition=N'@Creservados Cursor Output'
			EXEC sp_executesql @nSQL,@ParmDefinition,@Creservados=@Creservados Output
			Set @Abierto='1'
        End

        --Por ninguno
        If @P_DOCUMENTO_ID is null And @P_DOC_TRANS_ID is null
		Begin
			If @Abierto='1'
			Begin
				Close @Creservados
				Deallocate @Creservados
				Set @Abierto='0'
			End
			Set @nSQL= N'SET @Creservados = CURSOR FOR ' + + @StrSql1 + @StrSql2 + @StrSql2a + @StrSql3 + '; OPEN @Creservados'
			Set @ParmDefinition=N'@Creservados Cursor Output'
			EXEC sp_executesql @nSQL,@ParmDefinition,@Creservados=@Creservados Output
			Set @Abierto='1'
        End

		If @p_ilimitado='0' --pase
		Begin
			Fetch Next from @CReservados into 	@cliente_idR		 	,@producto_idR,
												@cantidadR				,@nro_serieR,
												@nro_loteR				,@fecha_vencimientoR,
												@nro_despachoR			,@nro_bultoR,
												@nro_partidaR			,@pesoR,
												@volumenR				,@tie_inR,
												@cant_dispR				,@codeR,
												@descriptionR			,@cat_log_id_finalR,
												@prop1R					,@prop2R,
												@prop3R					,@unidad_idR,
												@unidad_pesoR			,@unidad_volumenR,
												@est_merc_idR			,@moneda_idR,
												@costoR					,@ordenR
			While @@Fetch_Status=0
			Begin		
				If @intVarSuma >= @P_CANTIDAD
				Begin
					Break;
				End
			    If 	@vNro_Serie=@Nro_serieR And @vNro_Lote=@Nro_loteR And @vFecha_Vencimiento=@Fecha_vencimientoR And 
			        @vNro_Despacho=@Nro_despachoR And @vNro_Bulto=@Nro_bultoR And @vNro_Partida=@Nro_partidaR And
			        @vCat_Log=@Cat_log_id_FinalR And @vProp1=@Prop1R And @vProp2=@Prop2R And @vProp3=@Prop3R And
			        @vUnidad_Id=@Unidad_idR And @vEst_Merc_Id=@Est_merc_idR
			   	Begin
			        If 	@tCliente_Id=@ClienteID And @tProducto_Id=@Productoid And @tNro_Serie=@nro_serie And 
						@tNro_Lote=@nro_lote And @tNro_Despacho=@nro_despacho And @tNro_Bulto=@nro_bulto And
						@tNro_Partida=@nro_partida And @tCat_Log=@cat_log_id And @tProp1=@prop1 And
						@tProp2=@prop2 And @tProp3=@prop3 And @tUnidad_Id=@unidad_id And @intVarContador > 0
					Begin
			            If @intVarReservados < @cantidadR
						Begin
			                If @CantidadR - @intVarReservados > @Cantidad 
							Begin
			                    Set @intVarReservados = @intVarReservados + @Cantidad
								Set @Cantidad = 0
							End
			                Else
							Begin
			                    Set @Cantidad = @Cantidad - (@cantidadR - @intVarReservados)
			                    Set @intVarReservados = @cantidadR
			                End
			            End
					End
			        Else
					Begin
			            Set @intVarReservados = 0
			            Set @intVarSuma = 0
			            Set @varRowId = 0
			            If @varRowId <> @@Cursor_Rows
						Begin
			                Set @intVarReservados = 0
			                If @intVarReservados < @cantidadR
							Begin
								If @cantidadR > @Cantidad 
								Begin
								   Set @intVarReservados = @intVarReservados + @Cantidad
								   Set @Cantidad = 0
								End
								Else
								Begin
								   Set @intVarReservados = @intVarReservados + @cantidadR
								   Set @Cantidad = @Cantidad - @intVarReservados
								End
			                End 
			            End 
			        End 
			
					INSERT INTO #TEMP_EXISTENCIA_LOCATOR_RL VALUES (
									@rl_id,						@ClienteID,
									@Productoid,				@Cantidad,
									@vNro_Serie,				@vNro_Lote,
									@vFecha_Vencimiento,		@vNro_Despacho,
									@vNro_Bulto,				@vNro_Partida,
									@vPeso,						@vVolumen,
									@vCat_Log,					@vProp1,
									@vProp2,					@vProp3,
									@fecha_cpte,	        	@fecha_alta_gtw,
									@vUnidad_Id,				@vUnidad_Peso,
									@vUnidad_Volumen,			@vEst_Merc_Id,
									@vMoneda_id,			    @vCosto)
			               
					INSERT INTO #TEMP_RL_EXISTENCIA_DOC VALUES(@rl_id)
			
					Set @intVarContador = @intVarContador + 1
					Set @tCliente_Id 	= @ClienteID
					Set @tProducto_Id 	= @Productoid
					Set @tNro_Serie 	= @nro_serie
					Set @tNro_Lote 		= @nro_lote
					Set @tNro_Despacho 	= @nro_despacho
					Set @tNro_Bulto 	= @nro_bulto
					Set @tNro_Partida 	= @nro_partida
					Set @tCat_Log 		= @cat_log_id
					Set @tProp1 		= @prop1
					Set @tProp2 		= @prop2
					Set @tProp3 		= @prop3
					Set @tUnidad_Id 	= @unidad_id
					Set @tEst_Merc_Id 	= @est_merc_id
					
					Set @varRowId = @@Cursor_Rows
				End
			    Else
				Begin
			        If @intVarReservados < @cantidadR
					Begin
			            If @cantidadR > @Cantidad
						Begin
							Set @intVarReservados = @intVarReservados + (@CantidadR - @Cantidad)
						End
			            Else
						Begin
			                Set @intVarReservados = @intVarReservados + @Cantidad
			            End 
			            Set @Cantidad = @Cantidad - @intVarReservados
			        End 
			
			        If @Cantidad < 0
					Begin
			            Set @Cantidad = 0
			        End
			
			
					INSERT INTO #TEMP_EXISTENCIA_LOCATOR_RL VALUES (
						@rl_id,				@ClienteID,
						@Productoid,		@Cantidad,
						@vNro_Serie,		@vNro_Lote,
						@vFecha_Vencimiento,@vNro_Despacho,
						@vNro_Bulto,		@vNro_Partida,
						@vPeso,				@vVolumen,
						@vCat_Log,			@vProp1,
						@vProp2,			@vProp3,
						@fecha_cpte,		@fecha_alta_gtw,
						@vUnidad_Id,		@vUnidad_Peso,
						@vUnidad_Volumen,	@vEst_Merc_Id,
						@vMoneda_id,		@vCosto)
					
					INSERT INTO #TEMP_RL_EXISTENCIA_DOC VALUES(@rl_id)
			
			            
			    End
		
				Fetch Next from @CReservados into 	@cliente_idR		 	,@producto_idR,
													@cantidadR				,@nro_serieR,
													@nro_loteR				,@fecha_vencimientoR,
													@nro_despachoR			,@nro_bultoR,
													@nro_partidaR			,@pesoR,
													@volumenR				,@tie_inR,
													@cant_dispR				,@codeR,
													@descriptionR			,@cat_log_id_finalR,
													@prop1R					,@prop2R,
													@prop3R					,@unidad_idR,
													@unidad_pesoR			,@unidad_volumenR,
													@est_merc_idR			,@moneda_idR,
													@costoR					,@ordenR
			End
			Close @CReservados
			Deallocate @CReservados
		End
		If @p_ilimitado='1' --pase
		Begin
			Fetch Next from @CReservados into 	@cliente_idR		 	,@producto_idR,
												@cantidadR				,@nro_serieR,
												@nro_loteR				,@fecha_vencimientoR,
												@nro_despachoR			,@nro_bultoR,
												@nro_partidaR			,@pesoR,
												@volumenR				,@tie_inR,
												@cant_dispR				,@codeR,
												@descriptionR			,@cat_log_id_finalR,
												@prop1R					,@prop2R,
												@prop3R					,@unidad_idR,
												@unidad_pesoR			,@unidad_volumenR,
												@est_merc_idR			,@moneda_idR,
												@costoR					,@ordenR
			While @@Fetch_Status=0
			Begin
				If 	@vNro_Serie=@NRO_SERIER And 	@vNro_Lote=@NRO_LOTER And @vFecha_Vencimiento=@FECHA_VENCIMIENTOR And
					@vNro_Despacho=@NRO_DESPACHOR And @vNro_Bulto=@NRO_BULTOR And @vNro_Partida=@NRO_PARTIDAR And
					@vCat_Log=@CAT_LOG_ID_FinalR And @vProp1=@PROP1R And @vProp2=@PROP2R And @vProp3=@PROP3R And 
					@vUnidad_Id=@UNIDAD_IDR And @vEst_Merc_Id=@EST_MERC_IDR
				Begin
					If 	@tCliente_Id=@ClienteID And @tProducto_Id=@Productoid And @tNro_Serie=@nro_serie And 
						@tNro_Lote=@nro_lote And @tNro_Despacho=@nro_despacho And @tNro_Bulto=@nro_bulto And
						@tNro_Partida=@nro_partida And @tCat_Log=@cat_log_id And @tProp1=@prop1 And
						@tProp2=@prop2 And @tProp3=@prop3 And @tUnidad_Id=@unidad_id And
						@tEst_Merc_Id=@est_merc_id And @intVarContador > 0
					Begin
                        If @intVarReservados < @cantidadR
						Begin
                            If @cantidadR - @intVarReservados > @Cantidad
							Begin 
                                Set @intVarReservados = @intVarReservados + @Cantidad
                                Set @Cantidad = 0
							End
                            Else
							Begin
                                Set @Cantidad = @Cantidad - (@CantidadR - @intVarReservados)
                                Set @intVarReservados = @CantidadR
                            End
                        End
					End
					Else
					Begin
						Set @intVarReservados = 0
						Set @intVarSuma = 0
						Set @varRowId = 0

						If @varRowId <> @@Cursor_Rows
						Begin
						    Set @intVarReservados = 0
						    If  @intVarReservados < @cantidad
							Begin
						        If @cantidad > @Cantidad
								Begin 
						            Set @intVarReservados = @intVarReservados + @Cantidad
						            Set @Cantidad = 0
								End
						        Else
								Begin
						            Set @intVarReservados = @intVarReservados + @CANTIDADr
						            Set @Cantidad = @Cantidad - @intVarReservados
						        End 
						    End 
						End 
					End

					INSERT INTO #TEMP_EXISTENCIA_LOCATOR_RL VALUES (
								 @rl_id						,@ClienteID
								,@Productoid				,@Cantidad
								,@vNro_Serie				,@vNro_Lote
								,@vFecha_Vencimiento		,@vNro_Despacho
								,@vNro_Bulto				,@vNro_Partida
								,@vPeso						,@vVolumen
								,@vCat_Log					,@vProp1
								,@vProp2					,@vProp3
								,@fecha_cpte				,@fecha_alta_gtw
								,@vUnidad_Id				,@vUnidad_Peso
								,@vUnidad_Volumen			,@vEst_Merc_Id
								,@vMoneda_id				,@vCosto )
					
					INSERT INTO #TEMP_RL_EXISTENCIA_DOC VALUES(@rl_id);
					
					Set @intVarContador = @intVarContador + 1
					Set @tCliente_Id 	= @ClienteID
					Set @tProducto_Id 	= @Productoid
					Set @tNro_Serie 	= @nro_serie
					Set @tNro_Lote 		= @nro_lote
					Set @tNro_Despacho 	= @nro_despacho
					Set @tNro_Bulto 	= @nro_bulto
					Set @tNro_Partida 	= @nro_partida
					Set @tCat_Log 		= @cat_log_id
					Set @tProp1 		= @prop1
					Set @tProp2 		= @prop2
					Set @tProp3 		= @prop3
					Set @tUnidad_Id 	= @unidad_id
					Set @tEst_Merc_Id 	= @est_merc_id
					Set @varRowId 		= @@Cursor_Rows
				End
				Else
				Begin
					--aca va lo q estoy desarrollando.
					If @intVarReservados < @cantidadR
					Begin
					    If @cantidadR > @Cantidad
						Begin
					        Set @intVarReservados = @intVarReservados + (@CantidadR - @Cantidad)
						End
					    Else
						Begin
					        Set @intVarReservados = @intVarReservados + @CantidadR
					    End
					
					    Set @Cantidad = @Cantidad - @intVarReservados
					End
					
					
					If @Cantidad < 0
					Begin
					    Set @Cantidad = 0
					End
					
					INSERT INTO #TEMP_EXISTENCIA_LOCATOR_RL VALUES (
								 @rl_id								,@ClienteID
								,@Productoid						,@Cantidad
								,@vNro_Serie						,@vNro_Lote
								,@vFecha_Vencimiento				,@vNro_Despacho
								,@vNro_Bulto						,@vNro_Partida
								,@vPeso								,@vVolumen
								,@vCat_Log							,@vProp1
								,@vProp2							,@vProp3
								,@fecha_cpte						,@fecha_alta_gtw
								,@vUnidad_Id						,@vUnidad_Peso
								,@vUnidad_Volumen					,@vEst_Merc_Id
								,@vMoneda_id						,@vCosto)
					
					INSERT INTO #TEMP_RL_EXISTENCIA_DOC VALUES(@rl_id)

				End

				Fetch Next from @CReservados into 	@cliente_idR		 	,@producto_idR,
													@cantidadR				,@nro_serieR,
													@nro_loteR				,@fecha_vencimientoR,
													@nro_despachoR			,@nro_bultoR,
													@nro_partidaR			,@pesoR,
													@volumenR				,@tie_inR,
													@cant_dispR				,@codeR,
													@descriptionR			,@cat_log_id_finalR,
													@prop1R					,@prop2R,
													@prop3R					,@unidad_idR,
													@unidad_pesoR			,@unidad_volumenR,
													@est_merc_idR			,@moneda_idR,
													@costoR					,@ordenR
			End --Fin While Reservados con p_ilimitado='1'
			If @@Cursor_Rows = 0 And @intVarSuma < @P_CANTIDAD
			Begin
			    If @@Cursor_Rows > 0
				Begin
			        If @intVarReservados < @cantidadR
					Begin
			            If @cantidadR > @Cantidad
						Begin
			                Set @intVarReservados = @intVarReservados + (@CantidadR - @Cantidad)
						End
			            Else
						Begin
			                Set @intVarReservados = @intVarReservados + @CantidadR
			            End 
			            Set @Cantidad = @Cantidad - @intVarReservados
			        End
			    End 
			    --'FIN CAMBIO


				INSERT INTO #TEMP_EXISTENCIA_LOCATOR_RL VALUES (
																 @rl_id
																,@ClienteID
																,@Productoid
																,@Cantidad
																,@vNro_Serie
																,@vNro_Lote
																,@vFecha_Vencimiento
																,@vNro_Despacho
																,@vNro_Bulto
																,@vNro_Partida
																,@vPeso
																,@vVolumen
																,@vCat_Log
																,@vProp1
																,@vProp2
																,@vProp3
																,@fecha_cpte
																,@fecha_alta_gtw
																,@vUnidad_Id
																,@vUnidad_Peso
																,@vUnidad_Volumen
																,@vEst_Merc_Id
																,@vMoneda_id
																,@vCosto)
			    
				INSERT INTO #TEMP_RL_EXISTENCIA_DOC VALUES(@rl_id)
			   
			End
			Close @Creservados
			Deallocate @Creservados
			Set @Abierto='0' --Variable de Control
		End

		Fetch Next From @Cexistencia into    @rl_id				,@ClienteID		,@Productoid		,@Cantidad
											,@nro_serie			,@nro_lote		,@fecha_vencimiento
											,@nro_despacho		,@nro_bulto		,@nro_partida		,@peso
											,@volumen			,@cat_log_id
											,@prop1				,@prop2			,@prop3				,@fecha_cpte
											,@fecha_alta_gtw	,@unidad_id		,@unidad_peso
											,@Unidad_volumen	,@est_merc_id	,@moneda_id	,@costo
	End --Fin While Existencia

	Close 		@Cexistencia
	Deallocate 	@Cexistencia

	DELETE FROM #TEMP_EXISTENCIA_LOCATOR_RL WHERE CANTIDAD <= 0


	INSERT INTO #TEMP_EXISTENCIA_LOCATOR
				SELECT 	  clienteid
						, productoid
						, sum(IsNull(cantidad,0)) AS CANTIDAD
						, nro_serie
						, nro_lote
						, fecha_vencimiento
						, nro_despacho
						, nro_bulto
						, nro_partida
						, peso,volumen
						, cat_log_id
						, prop1
						, prop2
						, prop3
						, fecha_cpte
						, fecha_alta_gtw
						, unidad_id
						, unidad_peso
						, unidad_volumen
						, est_merc_id
						, moneda_id
						, costo
				FROM 	#TEMP_EXISTENCIA_LOCATOR_RL
				GROUP BY 	  
						 ClienteID, ProductoID, Nro_Serie, Nro_Lote, fecha_vencimiento , nro_despacho, nro_bulto
						,nro_partida, peso, volumen, cat_log_id, prop1, prop2, prop3, fecha_cpte
						,fecha_alta_gtw, unidad_id, unidad_peso, unidad_volumen, est_merc_id, moneda_id, costo
/*
	Set @nSQL= N'SET @C_egr1 = CURSOR FOR SELECT * FROM #TEMP_EXISTENCIA_LOCATOR ' + @StrSqlOrderBy + '; Open @C_egr1'
	Set @ParmDefinition=N'@C_egr1 Cursor Output'
	EXEC sp_executesql @nSQL,@ParmDefinition,@C_egr1=@C_egr2 Output

	Set @C_Egr=@C_Egr2
	
	--Open @C_Egr

	Close @C_Egr2
	Deallocate @C_Egr2
*/
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

ALTER              Procedure [dbo].[Locator_Api#Get_Productos_Locator_Rl]
  @p_cliente_id 				as varchar(15)
, @p_producto_id 				as varchar(30)
, @p_cantidad 					As numeric(20,5)
, @p_TieIN 						As varchar(1)
, @p_nro_serie 					As varchar(50)
, @p_nro_lote 					As varchar(50)
, @p_fecha_vencimiento 			As varchar(20)
, @p_Nro_Despacho 				As varchar(50)
, @p_Nro_Bulto 					As varchar(50)
, @p_nro_partida 				As varchar(50)
, @p_CatLog_ID 					As varchar(50)
, @P_PESO 						As numeric(20,5)
, @P_VOLUMEN 					As numeric(20,5)
, @p_Nave 						As numeric(20,0)
, @P_CALLE 						As numeric(20,0)
, @P_COLUMNA 					As numeric(20,0)
, @p_Nivel 						As numeric(20,0)
, @p_TipoOperacion_ID 			As varchar(5)
, @p_UseNullOnEmpty 			As Char(1)
, @p_EstMercID 					As varchar(15)
, @p_Llamada 					As varchar(50)
, @P_PROP1 						As varchar(100)
, @p_Prop2 						As varchar(100)
, @p_Prop3 						As varchar(100)
, @P_UNIDAD_ID 					As varchar(5)
, @P_UNIDAD_PESO 				As varchar(5)
, @P_UNIDAD_VOLUMEN 			As varchar(5)
, @P_DOCUMENTO_ID 				As numeric(20,0)
, @P_DOC_TRANS_ID 				As numeric(20,0)
, @p_Moneda_Id 					As varchar(20)
, @p_Costo 						As numeric(20,5)
, @p_Ilimitado 					As Char(1)
, @pLocator_Automatico			As Char(1)
, @P_NAVE_ID 					As Numeric(20,0)
, @P_POSICION_ID 				As numeric(20,0)
--, @c_egr						Cursor varying Output
As
Begin
	-------------------------------------------------
	--			Cursores
	-------------------------------------------------
	Declare @pAux 				Cursor
	Declare @InCur 				Cursor
	Declare @pcur 				Cursor
	Declare @cpCur 				Cursor
	Declare @TedCur 			Cursor
	-------------------------------------------------
	--			Generales
	-------------------------------------------------
	Declare @xSQL 				As varchar(8000)
	Declare @strsql 			As varchar(4000)
	Declare @StrSql1 			As varchar(4000)
	Declare @StrSql2 			As varchar(4000)
	Declare @StrSql3 			As varchar(4000)
	Declare @StrSql4 			As varchar(4000)
	Declare @StrSql5 			As varchar(4000)
	Declare @NtrSql2 			As varchar(4000)
	Declare @NtrSql3 			As varchar(4000)
	Declare @NtrSql4 			As varchar(4000)
	Declare @NtrSql5 			As varchar(4000)
	Declare @StrSql6 			As varchar(4000)
	Declare @StrSqlOrderBy 		As varchar(4000)
	Declare @varStrIn 			As varchar(4000)
	Declare @varSumCantidad 	As Float
	Declare @vDepositoID 		As varchar(4000)
	Declare @PCLIENTE_ID 		As varchar(15)
	Declare @PCANTIDAD 			As Float
	Declare @VCANTIDAD 			As Float
	Declare @PNRO_SERIE 		As varchar(50)
	Declare @PNRO_PARTIDA 		As varchar(50)
	Declare @PNRO_LOTE 			As varchar(50)
	Declare @PNRO_DESPACHO 		As varchar(50)
	Declare @PPRODUCTO_ID 		As varchar(30)
	Declare @PPESO 				As Float
	Declare @PVOLUMEN 			As Float
	Declare @PNRO_BULTO 		As varchar(50)
	Declare @P_FECHA_VTO_DDE 	As Datetime
	Declare @P_FECHA_VTO_HTA 	As Datetime
	Declare @P_TIE_IN 			As Char(1)
	Declare @pCat_Log_ID 		As varchar(50)
	Declare @PPROP1 			As varchar(100)
	Declare @PPROP2 			As varchar(100)
	Declare @PPROP3 			As varchar(100)
	Declare @nSQL				As nvarchar(4000)
	Declare @ParmDefinition 	As nvarchar(500)
	-------------------------------------------------
	-- 		Cursor @TedCur
	-------------------------------------------------
	Declare @clienteidT         VARCHAR(15)
	Declare @productoidT        VARCHAR(30)
	Declare @cantidadT          NUMERIC(20,5)
	Declare @nro_serieT         VARCHAR(50)
	Declare @nro_loteT          VARCHAR(50)
	Declare @fecha_vencimientoT DATETIME
	Declare @nro_despachoT      VARCHAR(50)
	Declare @nro_bultoT         VARCHAR(50)
	Declare @nro_partidaT       VARCHAR(50)
	Declare @pesoT              NUMERIC(20,5)
	Declare @volumenT           NUMERIC(20,5)
	Declare @tie_inT            CHAR(1)
	Declare @cantidad_dispT     NUMERIC(20,5)
	Declare @codeT              CHAR(1)
	Declare @descriptionT       VARCHAR(100)
	Declare @cat_log_idT        VARCHAR(50)
	Declare @prop1T             VARCHAR(100)
	Declare @prop2T             VARCHAR(100)
	Declare @prop3T             VARCHAR(100)
	Declare @unidad_idT         VARCHAR(5)
	Declare @unidad_pesoT       VARCHAR(5)
	Declare @unidad_volumenT    VARCHAR(5)
	Declare @est_merc_idT       VARCHAR(15)
	Declare @moneda_idT         VARCHAR(20)
	Declare @costoT             NUMERIC(10,3)
	Declare @ordenT             NUMERIC(20,0)
	-------------------------------------------------	
	--		Cursor @Incur
	-------------------------------------------------
	Declare @rl_idL             NUMERIC(20,0)
	Declare @clienteidL         VARCHAR(15)
	Declare @productoidL        VARCHAR(30)
	Declare @cantidadL          NUMERIC(20,5)
	Declare @nro_serieL         VARCHAR(50)
	Declare @nro_loteL          VARCHAR(50)
	Declare @fecha_vencimientoL DATETIME
	Declare @nro_despachoL      VARCHAR(50)
	Declare @nro_bultoL         VARCHAR(50)
	Declare @nro_partidaL       VARCHAR(50)
	Declare @pesoL              NUMERIC(20,5)
	Declare @volumenL           NUMERIC(20,5)
	Declare @cat_log_idL        VARCHAR(50)
	Declare @prop1L             VARCHAR(100)
	Declare @prop2L             VARCHAR(100)
	Declare @prop3L             VARCHAR(100)
	Declare @fecha_cpteL        DATETIME
	Declare @fecha_alta_gtwL    DATETIME
	Declare @unidad_idL         VARCHAR(5)
	Declare @unidad_pesoL       VARCHAR(5)
	Declare @unidad_volumenL    VARCHAR(5)
	Declare @est_merc_idL       VARCHAR(15)
	Declare @moneda_idL         VARCHAR(20)
	Declare @costoL             NUMERIC(10,3)
	------------------------------------------------
	--			Sys_Criterios_Locator
	------------------------------------------------
	Declare @Criterio_id		as varchar(30)
	Declare @Order_id			as varchar(5)
	Declare @Forma_id			as Varchar(30)
	-------------------------------------------------



	Select 	@vDepositoId= Deposito_Default from #Temp_Usuario_loggin;

    Set @P_TIE_IN 			= @p_TieIN
    Set @PNRO_SERIE 		= @p_nro_serie
    Set @PNRO_PARTIDA 		= @p_nro_partida
    Set @PPESO 				= @P_PESO
    Set @PVOLUMEN 			= @P_VOLUMEN
    Set @PNRO_LOTE 			= @p_nro_lote
    Set @PNRO_DESPACHO 		= @p_Nro_Despacho
    Set @PNRO_BULTO 		= @p_Nro_Bulto
    Set @P_FECHA_VTO_DDE 	= @p_fecha_vencimiento
    Set @pCat_Log_ID 		= @p_CatLog_ID
	Set @PPROP1 			= @P_PROP1
	Set @PPROP2 			= @p_Prop2
	Set @PPROP3 			= @p_Prop3



	Truncate table #temp_rl_existencia_doc

	Set @pAux=Cursor For
		select 	criterio_id,order_id,forma_id
		from 	sys_criterio_locator
		where 	cliente_id =ltrim(rtrim(upper(@p_cliente_id))) 
				and producto_id =ltrim(rtrim(upper(@p_producto_id)))
				and criterio_id <> 'ORDEN_PICKING'
		order by posicion_id
	Open @pAux
	Set  @StrSqlOrderBy='ORDER BY '

	Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	While @@Fetch_Status=0
	Begin
		if @Forma_id='TO_NUMBER'
		Begin
			Set	@StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(NUMERIC(20, 5), CASE WHEN ISNUMERIC(' + @Criterio_id + ') = 1 THEN ' + @CRITERIO_ID + ' ELSE NULL END) ' + @ORDER_ID + ', '
		End
		Else
		Begin
			if @Forma_id='TO_CHAR'
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + ' ' + @Criterio_id + ' ' + @Order_id + ', '
			End
			Else
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(DATETIME, ' + ' (' + @CRITERIO_ID + ')) ' + @ORDER_ID + ', '
			End	
		End				
		Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	End --fin While @pAux

	Close @pAux
	Deallocate @pAux

	If @StrSqlOrderBy <> 'ORDER BY '
	Begin
		Set @StrSqlOrderBy = Substring(@StrSqlOrderBy, 1, Len(@StrSqlOrderBy) - 1)
	End
    Else
	Begin
		Set @StrSqlOrderBy = ''
	End

	Exec Locator_Api#Verifica_Existencia_Ubic_Mov 	@P_CLIENTE_ID		, @P_PRODUCTO_ID, 
													@P_CANTIDAD			, @p_TieIN,
													@p_nro_serie		, @p_nro_lote,
													@p_fecha_vencimiento, @p_Nro_Despacho, 
													@p_Nro_Bulto		, @p_nro_partida, 
													@p_CatLog_ID		, @P_PESO, 
													@P_VOLUMEN			, @p_Nave, 
													@P_CALLE			, @P_COLUMNA, 
													@p_Nivel			, @P_PROP1, 
													@p_Prop2			, @p_Prop3, 
													@P_UNIDAD_ID		, @P_UNIDAD_PESO, 
													@P_UNIDAD_VOLUMEN	, @p_EstMercID, 
													@p_Moneda_Id		, @p_Costo, 
													@P_DOCUMENTO_ID		, @P_DOC_TRANS_ID, 
													@p_TipoOperacion_ID	, @p_Ilimitado, 
													@P_NAVE_ID			, @P_POSICION_ID--, 
													--@cpCur

	Set @strsql = ' SELECT * FROM #TEMP_EXISTENCIA_LOCATOR_RL '

	Set @varStrIn = ' and RL.RL_id in('
	

	Set @TedCur= Cursor For
		Select * from #temp_existencia_doc order by orden asc

	Open @TedCur
	
	Fetch Next from @TedCur Into     	  @clienteidT		, @productoidT
										, @cantidadT		, @nro_serieT
										, @nro_loteT		, @fecha_vencimientoT
										, @nro_despachoT	, @nro_bultoT
										, @nro_partidaT		, @pesoT
										, @volumenT			, @tie_inT
										, @cantidad_dispT	, @codeT
										, @descriptionT		, @cat_log_idT
										, @prop1T			, @prop2T
										, @prop3T			, @unidad_idT
										, @unidad_pesoT		, @unidad_volumenT
										, @est_merc_idT		, @moneda_idT
										, @costoT			, @ordenT
	While @@Fetch_Status=0
	Begin
		

		Set @ParmDefinition=N'@inCur Cursor Output'
		Set @nSQL=N' Set @inCur= Cursor For ' + @StrSQl + 	@StrSqlOrderBy + '; Open @Incur'
		Exec sp_executesql @nSQL,@ParmDefinition,@inCur=@inCur Output

		Fetch Next From @inCur Into  @rl_idL				,@clienteidL
									,@productoidL			,@cantidadL
									,@nro_serieL			,@nro_loteL
									,@fecha_vencimientoL	,@nro_despachoL
									,@nro_bultoL			,@nro_partidaL
									,@pesoL					,@volumenL
									,@cat_log_idL			,@prop1L
									,@prop2L				,@prop3L
									,@fecha_cpteL			,@fecha_alta_gtwL
									,@unidad_idL			,@unidad_pesoL
									,@unidad_volumenL		,@est_merc_idL
									,@moneda_idL			,@costoL
		While @@Fetch_Status=0
		Begin
			If 	@ProductoidT=@ProductoidL And @Nro_serieT=@Nro_serieL And @Nro_loteT=@Nro_loteL And
				@Fecha_vencimientoT=@Fecha_vencimientoL And @Nro_despachoT=@Nro_despachoL And 
				@Nro_bultoT= @Nro_bultoL And @Nro_partidaT=@Nro_partidaL And @Cat_log_idT=@Cat_log_idL And
                @Prop1T=@Prop1L And @Prop2T=@Prop2L And @Prop3T=@Prop3L  And @Unidad_idT=@Unidad_idL And
                @Est_merc_idT=@Est_merc_idL
			Begin
                If @pLocator_Automatico = '0'
				Begin
                    Set @varStrIn = @varStrIn + Cast(@rl_idL as varchar(20)) + ', '
				End

                If @pLocator_Automatico ='1'
				Begin
                    If @varSumCantidad < @P_CANTIDAD
					Begin
						Set @varStrIn = @varStrIn + Cast(@rl_idL as varchar(20)) + ', '
                        Set @varSumCantidad = @varSumCantidad + @cantidadL
                    End
                End 
            End 

			Fetch Next From @inCur Into  @rl_idL				,@clienteidL
										,@productoidL			,@cantidadL
										,@nro_serieL			,@nro_loteL
										,@fecha_vencimientoL	,@nro_despachoL
										,@nro_bultoL			,@nro_partidaL
										,@pesoL					,@volumenL
										,@cat_log_idL			,@prop1L
										,@prop2L				,@prop3L
										,@fecha_cpteL			,@fecha_alta_gtwL
										,@unidad_idL			,@unidad_pesoL
										,@unidad_volumenL		,@est_merc_idL
										,@moneda_idL			,@costoL
		End
		Close @InCur
		Deallocate @Incur

		Fetch Next from @TedCur Into     	  @clienteidT		, @productoidT
											, @cantidadT		, @nro_serieT
											, @nro_loteT		, @fecha_vencimientoT
											, @nro_despachoT	, @nro_bultoT
											, @nro_partidaT		, @pesoT
											, @volumenT			, @tie_inT
											, @cantidad_dispT	, @codeT
											, @descriptionT		, @cat_log_idT
											, @prop1T			, @prop2T
											, @prop3T			, @unidad_idT
											, @unidad_pesoT		, @unidad_volumenT
											, @est_merc_idT		, @moneda_idT
											, @costoT			, @ordenT
	End	
	
    Set @varStrIn = Upper(Substring(@varStrIn, 1, Len(@varStrIn) - 2))
    Set @varStrIn = @varStrIn + ')'

    If @varStrIn = ' AND RL.RL_ID I)'
	Begin
        Set @varStrIn = ''
    End

	Set @StrSql1 = ' select X.* from (' + Char(13)
	Set @StrSql2 = ' SELECT DD.DOCUMENTO_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.CLIENTEID AS CLIENTEID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PRODUCTOID AS PRODUCTOID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,IsNull(SUM(TEL.CANTIDAD),0) AS CANTIDAD ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.UNIDAD_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_SERIE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_LOTE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.FECHA_VENCIMIENTO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_DESPACHO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_BULTO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.NRO_PARTIDA ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PESO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.VOLUMEN ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,PROD.KIT ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,DD.TIE_IN ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,DD.NRO_TIE_IN_PADRE AS TIE_IN_PADRE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,DD.NRO_TIE_IN ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,IsNull(N.NAVE_COD,N2.NAVE_COD) AS STORAGE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,IsNull(RL.NAVE_ACTUAL,P.NAVE_ID) AS NAVEID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,CALN.CALLE_COD AS CALLECOD ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,CALN.CALLE_ID AS CALLEID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,COLN.COLUMNA_COD AS COLUMNACOD ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,COLN.COLUMNA_ID AS COLUMNAID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,NN.NIVEL_COD AS NIVELCOD ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,NN.NIVEL_ID AS NIVELID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.CAT_LOG_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.EST_MERC_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,RL.POSICION_ACTUAL AS POSICIONID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PROP1 ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PROP2 ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.PROP3 ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.FECHA_CPTE ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.FECHA_ALTA_GTW ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.RL_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.UNIDAD_PESO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.UNIDAD_VOLUMEN ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.MONEDA_ID ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,TEL.COSTO ' + Char(13)
	Set @StrSql2 = @StrSql2 + ' ,CASE ISNULL(N2.NAVE_TIENE_LAYOUT,N.NAVE_TIENE_LAYOUT) WHEN 1 THEN P.ORDEN_PICKING WHEN 0 THEN CAST(ISNULL(N.ORDEN_LOCATOR,N2.ORDEN_LOCATOR) AS INT) END AS ORDEN_PICKING ' + Char(13)
	Set @StrSql3 = ' FROM RL_DET_DOC_TRANS_POSICION RL ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN DET_DOCUMENTO_TRANSACCION  DDT ON DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN DET_DOCUMENTO               DD ON DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN CLIENTE                      C ON C.CLIENTE_ID = DD.CLIENTE_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN PRODUCTO                  PROD ON DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN CATEGORIA_LOGICA            CL ON DD.CLIENTE_ID = CL.CLIENTE_ID AND RL.CAT_LOG_ID = CL.CAT_LOG_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN NAVE                          N ON RL.NAVE_ACTUAL = N.NAVE_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN POSICION                      P ON RL.POSICION_ACTUAL = P.POSICION_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN NAVE                         N2 ON N2.NAVE_ID = P.NAVE_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN CALLE_NAVE                 CALN ON CALN.CALLE_ID = P.CALLE_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN COLUMNA_NAVE               COLN ON COLN.COLUMNA_ID = P.COLUMNA_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' LEFT JOIN NIVEL_NAVE                   NN ON NN.NIVEL_ID = P.NIVEL_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' INNER JOIN #TEMP_EXISTENCIA_LOCATOR_RL TEL ON TEL.CLIENTEID = DD.CLIENTE_ID AND TEL.PRODUCTOID = DD.PRODUCTO_ID ' + Char(13)
	Set @StrSql3 = @StrSql3 + ' AND rl.rl_id = tel.rl_id ' + Char(13)
	Set @StrSql4 = ' WHERE RL.RL_ID = TEL.RL_ID ' + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND CL.DISP_EGRESO = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND RL.DISPONIBLE = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND IsNull(N.DISP_EGRESO, IsNull(N2.DISP_EGRESO, ' + Char(39) + '1' + Char(39) + ')) = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND IsNull(P.POS_LOCKEADA, ' + Char(39) + '0' + Char(39) + ') = ' + Char(39) + '0' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND IsNull(n.deposito_id, n2.deposito_Id) = ' + Char(39) + @vDepositoID + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND ' + Char(13)
	Set @StrSql4 = @StrSql4 + ' (SELECT CASE WHEN (Count(posicion_id)) > 0 THEN 1 ELSE 0 END' + Char(13)
	Set @StrSql4 = @StrSql4 + '  FROM   rl_posicion_prohibida_cliente' + Char(13)
	Set @StrSql4 = @StrSql4 + '  WHERE  Posicion_ID = IsNull(P.NAVE_ID, 0)' + Char(13)
	Set @StrSql4 = @StrSql4 + '        AND cliente_id = DD.CLIENTE_ID' + Char(13)
	Set @StrSql4 = @StrSql4 + ' ) = 0' + Char(13)
	           Set @StrSql5 = ' GROUP BY ' + Char(13)
	Set @StrSql5 = @StrSql5 + '  dd.documento_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.clienteid ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.productoid ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.nro_serie ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Nro_lote ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Fecha_vencimiento ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Nro_Despacho ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Nro_Bulto ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Nro_Partida ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Peso ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.Volumen ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,prod.kit ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,dd.tie_in ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,dd.nro_tie_in_padre ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,dd.nro_tie_in ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,n.nave_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,n2.nave_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,rl.nave_actual ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,p.nave_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,caln.calle_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,caln.calle_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,coln.columna_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,coln.columna_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,nn.nivel_cod ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,nn.nivel_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.cat_log_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,rl.posicion_actual ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.est_merc_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.prop1 ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.prop2 ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.prop3 ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.fecha_cpte ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.fecha_alta_gtw ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.rl_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.unidad_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.unidad_peso ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.unidad_volumen ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.moneda_id ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,tel.costo ' + Char(13)
	Set @StrSql5 = @StrSql5 + ' ,CASE ISNULL(N2.NAVE_TIENE_LAYOUT,N.NAVE_TIENE_LAYOUT) WHEN 1 THEN P.ORDEN_PICKING WHEN 0 THEN CAST(ISNULL(N.ORDEN_LOCATOR,N2.ORDEN_LOCATOR) AS INT) END ' + Char(13)
	Set @NtrSql2 = ' UNION ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' SELECT DD.DOCUMENTO_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.CLIENTEID AS CLIENTEID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PRODUCTOID AS PRODUCTOID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,IsNull(SUM(TEL.CANTIDAD),0) AS CANTIDAD ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.UNIDAD_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_SERIE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_LOTE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.FECHA_VENCIMIENTO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_DESPACHO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_BULTO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.NRO_PARTIDA ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PESO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.VOLUMEN ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,PROD.KIT ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,DD.TIE_IN ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,DD.NRO_TIE_IN_PADRE AS TIE_IN_PADRE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,DD.NRO_TIE_IN ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,IsNull(N.NAVE_COD,N2.NAVE_COD) AS STORAGE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,IsNull(RL.NAVE_ANTERIOR,P.NAVE_ID) AS NAVEID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,CALN.CALLE_COD AS CALLECOD ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,CALN.CALLE_ID AS CALLEID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,COLN.COLUMNA_COD AS COLUMNACOD ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,COLN.COLUMNA_ID AS COLUMNAID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,NN.NIVEL_COD AS NIVELCOD ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,NN.NIVEL_ID AS NIVELID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.CAT_LOG_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.EST_MERC_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,RL.POSICION_ANTERIOR AS POSICIONID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PROP1 ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PROP2 ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.PROP3 ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.FECHA_CPTE ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.FECHA_ALTA_GTW ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.RL_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.UNIDAD_PESO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.UNIDAD_VOLUMEN ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.MONEDA_ID ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,TEL.COSTO ' + Char(13)
	Set @NtrSql2 = @NtrSql2 + ' ,CASE ISNULL(N2.NAVE_TIENE_LAYOUT,N.NAVE_TIENE_LAYOUT) WHEN 1 THEN P.ORDEN_PICKING WHEN 0 THEN CAST(ISNULL(N.ORDEN_LOCATOR,N2.ORDEN_LOCATOR) AS INT) END AS ORDEN_PICKING ' + Char(13)
	Set @NtrSql3 = ' FROM RL_DET_DOC_TRANS_POSICION RL ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN DET_DOCUMENTO_TRANSACCION  DDT ON DDT.DOC_TRANS_ID = RL.DOC_TRANS_ID AND DDT.NRO_LINEA_TRANS = RL.NRO_LINEA_TRANS ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN DET_DOCUMENTO               DD ON DD.DOCUMENTO_ID = DDT.DOCUMENTO_ID AND DD.NRO_LINEA = DDT.NRO_LINEA_DOC ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN CLIENTE                      C ON C.CLIENTE_ID = DD.CLIENTE_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN PRODUCTO                  PROD ON DD.CLIENTE_ID = PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN CATEGORIA_LOGICA            CL ON DD.CLIENTE_ID = CL.CLIENTE_ID AND RL.CAT_LOG_ID_FINAL = CL.CAT_LOG_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN NAVE                          N ON RL.NAVE_ANTERIOR = N.NAVE_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN POSICION                      P ON RL.POSICION_ANTERIOR = P.POSICION_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN NAVE                         N2 ON N2.NAVE_ID = P.NAVE_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN CALLE_NAVE                 CALN ON CALN.CALLE_ID = P.CALLE_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN COLUMNA_NAVE               COLN ON COLN.COLUMNA_ID = P.COLUMNA_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' LEFT JOIN NIVEL_NAVE                   NN ON NN.NIVEL_ID = P.NIVEL_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' INNER JOIN #TEMP_EXISTENCIA_LOCATOR_RL TEL ON TEL.CLIENTEID = DD.CLIENTE_ID AND TEL.PRODUCTOID = DD.PRODUCTO_ID ' + Char(13)
	Set @NtrSql3 = @NtrSql3 + ' AND rl.rl_id = tel.rl_id ' + Char(13)
	Set @NtrSql4 = ' WHERE RL.RL_ID = TEL.RL_ID ' + Char(13)
	Set @NtrSql4 = @NtrSql4 + ' AND CL.DISP_EGRESO = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @NtrSql4 = @NtrSql4 + ' AND RL.DISPONIBLE = ' + Char(39) + '0' + Char(39) + Char(13)
	Set @NtrSql4 = @NtrSql4 + ' AND IsNull(N.DISP_EGRESO, IsNull(N2.DISP_EGRESO, ' + Char(39) + '1' + Char(39) + ')) = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @NtrSql4 = @NtrSql4 + ' AND IsNull(P.POS_LOCKEADA, ' + Char(39) + '1' + Char(39) + ') = ' + Char(39) + '1' + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND IsNull(n.deposito_id, n2.deposito_Id) = ' + Char(39) + @vDepositoID + Char(39) + Char(13)
	Set @StrSql4 = @StrSql4 + ' AND ' + Char(13)
	Set @StrSql4 = @StrSql4 + ' (SELECT CASE WHEN (Count(posicion_id)) > 0 THEN 1 ELSE 0 END' + Char(13)
	Set @StrSql4 = @StrSql4 + '  FROM   rl_posicion_prohibida_cliente' + Char(13)
	Set @StrSql4 = @StrSql4 + '  WHERE  Posicion_ID = IsNull(P.NAVE_ID, 0)' + Char(13)
	Set @StrSql4 = @StrSql4 + '         AND cliente_id = DD.CLIENTE_ID' + Char(13)
	Set @StrSql4 = @StrSql4 + '  ) = 0' + Char(13)
	Set @NtrSql5 = ' GROUP BY ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + '  dd.documento_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.clienteid ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.productoid ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.nro_serie ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Nro_lote ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Fecha_vencimiento ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Nro_Despacho ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Nro_Bulto ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Nro_Partida ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Peso ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.Volumen ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,prod.kit ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,dd.tie_in ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,dd.nro_tie_in_padre ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,dd.nro_tie_in ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,n.nave_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,n2.nave_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,rl.nave_ANTERIOR ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,p.nave_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,caln.calle_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,caln.calle_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,coln.columna_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,coln.columna_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,nn.nivel_cod ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,nn.nivel_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.cat_log_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,rl.posicion_ANTERIOR ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.est_merc_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.prop1 ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.prop2 ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.prop3 ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.fecha_cpte ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.fecha_alta_gtw ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.rl_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.unidad_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.unidad_peso ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.unidad_volumen ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.moneda_id ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,tel.costo ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ' ,CASE ISNULL(N2.NAVE_TIENE_LAYOUT,N.NAVE_TIENE_LAYOUT) WHEN 1 THEN P.ORDEN_PICKING WHEN 0 THEN CAST(ISNULL(N.ORDEN_LOCATOR,N2.ORDEN_LOCATOR) AS INT) END  ' + Char(13)
	Set @NtrSql5 = @NtrSql5 + ') x '

	Set @pAux=Cursor For
	select 	criterio_id,order_id,forma_id
	from 	sys_criterio_locator
	where 	cliente_id =ltrim(rtrim(upper(@p_cliente_id))) 
			and producto_id =ltrim(rtrim(upper(@p_producto_id)))
	order by posicion_id

	Open @pAux
	Set  @StrSqlOrderBy='ORDER BY '

	Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	While @@Fetch_Status=0
	Begin
		if @Forma_id='TO_NUMBER'
		Begin
			Set	@StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(NUMERIC(20, 5), CASE WHEN ISNUMERIC(' + @Criterio_id + ') = 1 THEN ' + @CRITERIO_ID + ' ELSE NULL END) ' + @ORDER_ID + ', '
		End
		Else
		Begin
			if @Forma_id='TO_CHAR'
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + ' ' + @Criterio_id + ' ' + @Order_id + ', '
			End
			Else
			Begin
				Set @StrSqlOrderBy = @StrSqlOrderBy + 'CONVERT(DATETIME, ' + ' (' + @CRITERIO_ID + ')) ' + @ORDER_ID + ', '
			End	
		End				
		Fetch Next From @pAux into @Criterio_id,@Order_id,@Forma_id
	End --fin While @pAux

	Close @pAux
	Deallocate @pAux

	If @StrSqlOrderBy <> 'ORDER BY '
	Begin
		Set @StrSqlOrderBy = Substring(@StrSqlOrderBy, 1, Len(@StrSqlOrderBy) - 1)
	End
    Else
	Begin
		Set @StrSqlOrderBy = ''
	End

	Set @xSQL=' Insert into #Tmp_Q2 '
	Set @xSQL= @xSQL + @StrSql1
	Set @xSQL= @xSQL + @StrSql2
	Set @xSQL= @xSQL + @StrSql3
	Set @xSQL= @xSQL + @StrSql4
	Set @xSQL= @xSQL + @varStrIn
	Set @xSQL= @xSQL + @StrSql5
	Set @xSQL= @xSQL + @NtrSql2
	Set @xSQL= @xSQL + @NtrSql3
	Set @xSQL= @xSQL + @NtrSql4
	Set @xSQL= @xSQL + @varStrIn
	Set @xSQL= @xSQL + @NtrSql5
	Set @xSQL= @xSQL + @StrSqlOrderBy

	Execute (@xSQL)

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

ALTER PROCEDURE [dbo].[Locator_Transferencia]
	@UbicacionOrigen	varchar(50)
--	@NroPallet			varchar(100)
As
Begin
	Declare @CantProducto 	as Float
	Declare @Cliente_ID 		as varchar(15)
	Declare @Producto_id	as varchar(30)
	Declare @vCant			as float		--con esto puedo conocer si tiene posiciones permitidas o no.
	declare @posicion_id 	as numeric(20,0)
	declare @posicion_cod 	as varchar(45)
	declare @nave_id		as numeric(20,0)
	declare @orden_locator 	as numeric(6)
	declare @caso			as int

	select 	distinct
			@CantProducto=count(dd.producto_id), @cliente_id=dd.cliente_id, @producto_id=dd.producto_id
	from	det_documento dd inner join det_documento_transaccion ddt 	on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
			inner join rl_det_doc_trans_posicion rl 						on(ddt.doc_trans_id=rl.doc_trans_id and ddt.nro_linea_trans=rl.nro_linea_trans)
			left join posicion p											on(rl.posicion_actual=p.posicion_id)
			left join nave n												on(rl.nave_actual=n.nave_id)
			left join estado_mercaderia_rl em							on(rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)
			inner join categoria_logica cl									on(rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id)
	where	((p.posicion_cod=@UbicacionOrigen) or(n.nave_cod=@UbicacionOrigen))
			and cl.disp_transf='1'
			and ((em.disp_transf is null) or (em.disp_transf='1'))
			and rl.disponible='1'
	group by dd.producto_id, dd.cliente_id
--dd.prop1=@NroPallet			and 
	
	select 	@vcant=count(*)
	from 	rl_producto_posicion_permitida
	where 	cliente_id=@cliente_id and producto_id=@producto_id
			and cliente_id is not null and producto_id is not null

	if @vcant > 0
		begin
			select top 1
					 @posicion_id=x.posicion_id
					,@posicion_cod=x.posicion_cod
					,@nave_id=x.nave_id
					,@orden_locator=x.ordenlocator
					,@caso=x1
			from(	select 	 Top 5
							 p.posicion_id  as posicion_id
							,p.posicion_cod as posicion_cod
							,null as nave_id
							,isnull(p.orden_locator,999999) as ordenlocator
							,1 as x1
					from 	posicion p inner join
							rl_producto_posicion_permitida rlpp
							on(p.posicion_id=rlpp.posicion_id)
					where	p.pos_lockeada='0'
							--p.pos_vacia='1' 
							--and rlpp.posicion_id not in(	select 	isnull(posicion_id,0)	from 	sys_locator_ing)
							--and rlpp.posicion_id not in(select posicion_actual from rl_det_doc_trans_posicion where posicion_actual is not null)
							--and rlpp.producto_id=ltrim(rtrim(upper(@producto_id))) 
							and rlpp.cliente_id=ltrim(rtrim(upper(@cliente_id)))
							--and p.posicion_cod<>@UbicacionOrigen
					union all
					select 	top 5
							 null as 	posicion_id
							,n.nave_cod as posicion_cod
							,n.nave_id  as nave_id
							,isnull(n.orden_locator,999999) as ordenlocator
							,0 as x1
					from 	nave n inner join
							rl_producto_posicion_permitida rlpp
							on(n.nave_id=rlpp.nave_id)
					where	n.disp_transf='1' and n.pre_ingreso='0' 
							and pre_egreso='0'
							--and rlpp.producto_id=ltrim(rtrim(upper(@producto_id)))
							and rlpp.cliente_id=ltrim(rtrim(upper(@cliente_id)))
							and nave_cod<>@UbicacionOrigen
			
			)as x
			order by x.ordenlocator asc
			/*
				DELETE FROM SYS_LOCATOR_ING WHERE DOCUMENTO_ID=@DOCUMENTO_ID AND NRO_LINEA=@NRO_LINEA
				IF @POSICION_ID IS NULL AND @NAVE_ID IS NULL
				BEGIN
					RAISERROR('-1021 SQL - NO QUEDAN UBICACIONES DISPONIBLES PARA UBICAR EL PALLET.',16,1)
				END
				INSERT INTO  SYS_LOCATOR_ING (DOCUMENTO_ID, NRO_LINEA, NRO_PALLET, POSICION_ID)
				VALUES (@DOCUMENTO_ID, @NRO_LINEA, @NROPALLET, @POSICION_ID )
			*/
			If @posicion_cod is not null
			begin
				select 	@posicion_id as posicion_id, @posicion_cod as posicion_cod,@nave_id as nave_id, @orden_locator as orden_locator
			end
			else
			begin
				raiserror('No quedan Ubicaciones disponibles para el pallet',16,1)
				return
			end
		end		
	else
		begin
				select top 1
						 @posicion_id=x.posicion_id
						,@posicion_cod=x.posicion_cod
						,@nave_id=x.nave_id
						,@orden_locator=x.ordenlocator
						,@caso=x1
				from(	select 	 posicion_id  as posicion_id
								,posicion_cod as posicion_cod
								,null as nave_id
								,isnull(orden_locator,999999) as ordenlocator
								,1 as x1
						from 	posicion p
						where	'1'='1' and p.pos_lockeada='0'
								--and p.posicion_cod not in (@UbicacionOrigen)
						union all
						select 	 null as posicion_id
								,nave_cod as posicion_cod
								,nave_id  as nave_id
								,isnull(orden_locator,999999) as ordenlocator
								,0 as x1
						from 	nave n
						where	n.disp_transf='1' and n.pre_ingreso='0' 
								and pre_egreso='0'
								and nave_tiene_layout='0'
								and nave_cod<>@UbicacionOrigen
				)as x
				order by x.ordenlocator
			select 	@posicion_id as posicion_id, @posicion_cod as posicion_cod,@nave_id as nave_id, @orden_locator as orden_locator
	end
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

ALTER        Procedure [dbo].[LocatorEgreso]
@pDocumento_id 		as Numeric(20,0) Output,
@pViaje_id			as varchar(100) Output
As
Begin

declare @Fecha_Vto				as datetime
declare @OrdenPicking			as numeric(10,0)
declare @Tipo_Posicion			as varchar(10)
declare @Codigo_Posicion		as varchar(100)
declare @Cliente_id				as varchar(15)
declare @Producto_id			as varchar(30)
declare @Cantidad				as numeric(20,5)
declare @Aux					as varchar(50)
declare @NewProducto			as varchar(30)
declare @OldProducto			as varchar(30)
declare @vQtyResto				as numeric(20,5)
declare @vRl_id					as numeric(20)
declare @QtySol					as numeric(20,5)
declare @vNroLinea				as numeric(20)
declare @NRO_BULTO				as varchar(50)
declare @NRO_LOTE				as varchar(50)
declare @EST_MERC_ID			as varchar(15)
declare @NRO_DESPACHO			as varchar(50)
declare @NRO_PARTIDA			as varchar(50)
declare @UNIDAD_ID				as varchar(5)
declare @PROP1					as varchar(100)
declare @PROP2					as varchar(100)
declare @PROP3					as varchar(100)
declare @DESC					as varchar(200)
declare @CAT_LOG_ID				as varchar(50)
declare @id						as numeric(20,0)
declare @Documento_id 			as Numeric(20,0)
declare @Saldo					as numeric(20,5)
declare @TipoSaldo				as varchar(20)
declare @Doc_Trans 				as numeric(20)
declare @QtyDetDocumento		as numeric(20)
declare @vUsuario_id			as varchar(50)
declare @vTerminal				as varchar(50)
declare @RsExist				as Cursor
declare @RsActuRL				as Cursor
declare @Crit1					as varchar(30)
declare @Crit2					as varchar(30)
declare @Crit3					as varchar(30)
declare @fecha_alta_gtw			as datetime
declare @nro_serie				as varchar(50)
declare @NewLoteProveedor			as varchar(100)
declare @OldLoteProveedor			as varchar(100)
declare @NewNroPartida			as varchar(100)
declare @OldNroPartida			as varchar(100)
declare @NewNroSerie			as varchar(50)
declare @OldNroSerie			as varchar(50)
declare @RSDOCEGR				as cursor
declare @DOCIDPIVOT				as numeric(20,0)
declare @NROLINEAPIVOT			as numeric(20,0)

SET NOCOUNT ON;
SET @vNroLinea = 0
--Obtengo los criterios de ordenamiento.
Select	@Crit1=CRITERIO_1, @Crit2=CRITERIO_2, @Crit3=CRITERIO_3
From	RL_CLIENTE_LOCATOR
Where	Cliente_id=(select Cliente_id from documento where documento_id=@pDocumento_id)

if (@Crit1 is null) and (@Crit2 is null) and (@Crit3 is null)
begin
	--Si todos son nulos entonces x default salgo con orden de picking.
	Set @Crit1='ORDEN_PICKING'
end

select @Cliente_id = cliente_id from documento where documento_id = @pDocumento_id

SET @RSDOCEGR = CURSOR FOR
SELECT DOCUMENTO_ID, NRO_LINEA FROM DET_DOCUMENTO WHERE DOCUMENTO_ID = @pDocumento_id

OPEN @RSDOCEGR
FETCH NEXT FROM @RSDOCEGR INTO @DOCIDPIVOT, @NROLINEAPIVOT

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @QtySol=0
	set @QtySol=dbo.GetQtySol(@pDocumento_id,@NROLINEAPIVOT,@Cliente_id)
	set @vQtyResto=@QtySol

	Set @RsExist = Cursor For
		Select	X.*
		from	(
			SELECT	 dd.fecha_vencimiento
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
			FROM	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
					inner join posicion p on (rl.posicion_actual=p.posicion_id)
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
					inner join documento d on(dd.documento_id=d.documento_id)
			WHERE	rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and p.pos_lockeada='0' and p.picking='1'
					and cl.disp_egreso='1' and cl.picking='1'
					and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
					--and dd.producto_id in (select producto_id from det_documento where documento_id=@pDocumento_id)
					and exists (select 1 from det_documento ddegr
								where	ddegr.documento_id = @pDocumento_id AND ddegr.nro_linea = @NROLINEAPIVOT
										and ddegr.producto_id = dd.producto_id
										and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
										and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
										and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
					and d.cliente_id = @cliente_id
			UNION
			SELECT	 dd.fecha_vencimiento
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
			FROM	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
					inner join nave n on (rl.nave_actual=n.nave_id)
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
					inner join documento d on(dd.documento_id=d.documento_id)
			WHERE	rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and rl.cat_log_id<>'TRAN_EGR'
					and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1'
					and cl.disp_egreso='1' and cl.picking='1'
					--and dd.producto_id in (select producto_id from det_documento where documento_id=@pDocumento_id)
					and exists (select 1 from det_documento ddegr
								where	ddegr.documento_id = @pDocumento_id AND ddegr.nro_linea = @NROLINEAPIVOT
										and ddegr.producto_id = dd.producto_id
										and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
										and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
										and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
					and d.cliente_id = @cliente_id
			)X		
			order by--order by producto,dd.fecha_vencimiento asc,orden  
					(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
					(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
					(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
					(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
	Fetch Next From @RsExist into	@Fecha_Vto,
									@OrdenPicking,
									@Tipo_Posicion,
									@Codigo_Posicion,
									@Cliente_id,
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
					set @vNroLinea=@vNroLinea+1
					set @vQtyResto=@vQtyResto-@Cantidad
					insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
								values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')
					--Insert con todas las propiedades en det_documento
					insert into det_documento_aux 
							(	documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
								cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
								unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
					values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
							,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
							,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)		
				end
				else begin
					set @vNroLinea=@vNroLinea+1
					insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)
								values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@vQtyResto,@vRl_id,@Cantidad-@vQtyResto,'2',getdate(),'N')
					--Insert con todas las propiedades en det_documento
					insert into det_documento_aux (
								documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
								cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
								unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
								values 
								(@pDocumento_id,@vNroLinea
								,@Cliente_id,@Producto_id,@vQtyResto,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
								,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
								,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)	
					set @vQtyResto=0
				end --if
		end --if
		Fetch Next From @RsExist into	@Fecha_Vto,
										@OrdenPicking,
										@Tipo_Posicion,
										@Codigo_Posicion,
										@Cliente_id,
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
	
	
	FETCH NEXT FROM @RSDOCEGR INTO @DOCIDPIVOT, @NROLINEAPIVOT
END
CLOSE @RSDOCEGR
DEALLOCATE @RSDOCEGR


--GUARDO SERIES INICIALES
--SELECT DISTINCT NRO_SERIE INTO #TMPSERIES FROM DET_DOCUMENTO WHERE DOCUMENTO_ID = @pDocumento_id

--Borro det_documento y lo vuelvo a insertar con las nuevas propiedades
delete det_documento where documento_id=@pDocumento_id
insert into det_documento select * from det_documento_aux where documento_id=@pDocumento_id


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
End	--End While @RsActuRL.
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


Set NoCount Off;
End -- Fin Procedure.
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

ALTER        Procedure [dbo].[LocatorEgreso_RemanenteDoc]
@pDocumento_id 	as Numeric(20,0) Output,
@pCliente_id	as varchar(15) Output,
@pViaje_id		as varchar(100) Output,
@vNroLinea		as Numeric(20,0) Output,
@pProducto_id   as varchar(100) Output,
@pCantRem		as Numeric(20,0) Output,
@Crit1			as varchar(30) Output,
@Crit2			as varchar(30) Output,
@Crit3			as varchar(30) Output			

As
Begin

declare @Fecha_Vto				as datetime
declare @OrdenPicking			as numeric(10,0)
declare @Tipo_Posicion			as varchar(10)
declare @Codigo_Posicion		as varchar(100)
declare @Cliente_id				as varchar(15)
declare @Producto_id			as varchar(30)
declare @Cantidad				as numeric(20,5)
declare @Aux					as varchar(50)
declare @NewProducto			as varchar(30)
declare @OldProducto			as varchar(30)
declare @vQtyResto				as numeric(20,5)
declare @vRl_id					as numeric(20)
declare @QtySol					as numeric(20,5)
declare @NRO_BULTO				as varchar(50)
declare @NRO_LOTE				as varchar(50)
declare @EST_MERC_ID			as varchar(15)
declare @NRO_DESPACHO			as varchar(50)
declare @NRO_PARTIDA			as varchar(50)
declare @UNIDAD_ID				as varchar(5)
declare @PROP1					as varchar(100)
declare @PROP2					as varchar(100)
declare @PROP3					as varchar(100)
declare @DESC					as varchar(200)
declare @CAT_LOG_ID				as varchar(50)
declare @Fecha_Alta_GTW			as datetime
declare @RsRem			        as Cursor
declare @auxErr					as varchar(4000)
declare @nro_serie				as varchar(50)


SET NOCOUNT ON;

		set @vQtyResto = @pCantRem  
		set @QtySol = @pCantRem  
		--set @vNroLinea=0
		Set @RsRem = Cursor For
			Select	X.*
			From	(
				SELECT	 dd.fecha_vencimiento
						,isnull(p.orden_picking,99999) as ORDEN_PICKING
						,'POS' as ubicacion
						,p.posicion_cod as posicion
						,dd.cliente_id
						,dd.producto_id as producto
						,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
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
						,D.FECHA_ALTA_GTW
						,dd.nro_serie
				FROM	rl_det_doc_trans_posicion rl
						inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
						inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
						inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
						inner join posicion p on (rl.posicion_actual=p.posicion_id)
						left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
						inner join documento d on(dd.documento_id=d.documento_id)
						left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
				WHERE	rl.doc_trans_id_egr is null
						and rl.nro_linea_trans_egr is null
						and rl.disponible='1'
						and isnull(em.disp_egreso,'1')='1'
						and isnull(em.picking,'1')='1'
						and p.pos_lockeada='0' and p.picking='1'
						and cl.disp_egreso='1' and cl.picking='1'
						and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
						and dd.producto_id =@pProducto_id
						--and rl.rl_id not in (select rl_id from #tmp_consumo_locator_egr)
						and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
				UNION
				SELECT	 dd.fecha_vencimiento
						,isnull(n.orden_locator,99999) as ORDEN_PICKING
						,'NAV' as ubicacion
						,n.nave_cod as posicion
						,dd.cliente_id
						,dd.producto_id as producto
						,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
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
						,D.FECHA_ALTA_GTW
						,dd.nro_serie
				FROM	rl_det_doc_trans_posicion rl
						inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
						inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
						inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
						inner join nave n on (rl.nave_actual=n.nave_id)
						left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
						inner join documento d on(dd.documento_id=d.documento_id)
						left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
				WHERE	rl.doc_trans_id_egr is null
						and rl.nro_linea_trans_egr is null
						and rl.disponible='1'
						and isnull(em.disp_egreso,'1')='1'
						and isnull(em.picking,'1')='1'
						and rl.cat_log_id<>'TRAN_EGR'
						and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1'
						and cl.disp_egreso='1' and cl.picking='1'
						and dd.producto_id =@pProducto_id
						--and rl.rl_id not in (select rl_id from #tmp_consumo_locator_egr)
						and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
						)X
				order by--order by producto,dd.fecha_vencimiento asc,orden  
						(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
						(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
						(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
						(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
								
	Open @RsRem
	Fetch Next From @RsRem into
											@Fecha_Vto,
											@OrdenPicking,
											@Tipo_Posicion,
											@Codigo_Posicion,
											@Cliente_id,
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
											@Fecha_Alta_GTW,
											@nro_serie
	While ((@@Fetch_Status=0) AND (@vQtyResto>0))
	begin --While Picking = 1
	-- Aca se replica la logica de Pickin=1
			if (@vQtyResto>=@Cantidad) 
				begin -- (@vQtyResto>=@Cantidad) 
				set @vNroLinea=@vNroLinea+1
				set @vQtyResto=@vQtyResto-@Cantidad
				insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
							values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')
				--Insert con todas las propiedades en det_documento
				insert into det_documento_aux (
							documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
							cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
							unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
							values 
							(@pDocumento_id,@vNroLinea
							,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
							,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
							,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)	
				insert into #tmp_consumo_locator_egr values (@vRl_id, @Cantidad)	
			end
			else begin
				set @vNroLinea=@vNroLinea+1
				insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)
							values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@vQtyResto,@vRl_id,@Cantidad-@vQtyResto,'2',getdate(),'N')
				--Insert con todas las propiedades en det_documento
				insert into det_documento_aux (
							documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
							cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
							unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
							values 
							(@pDocumento_id,@vNroLinea
							,@Cliente_id,@Producto_id,@vQtyResto,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
							,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
							,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_Serie)	
				insert into #tmp_consumo_locator_egr values (@vRl_id, @vQtyResto)	
				set @vQtyResto=0
			end --if (@vQtyResto>=@Cantidad) 
			Fetch Next From @RsRem into	@Fecha_Vto,
												@OrdenPicking,
												@Tipo_Posicion,
												@Codigo_Posicion,
												@Cliente_id,
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
												@Fecha_Alta_GTW,
												@nro_serie
		end -- End While Picking = 1
CLOSE @RsRem
DEALLOCATE @RsRem

--if @vQtyResto > 0 begin
--	set @auxErr = 'No se pudo asignar del producto ' + @pProducto_id + ', la cantidad total solicitada, para completar falta la cantidad de ' + convert(varchar,convert(int,@vQtyResto)) + ' unidades. '
--	RAISERROR (@auxErr,16,1)
--end --if


Set NoCount Off;
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

ALTER        Procedure [dbo].[LocatorEgreso_pallet_completo]
@pDocumento_id 	as Numeric(20,0) Output,
@pCliente_id	as varchar(15) Output,
@pViaje_id		as varchar(100) Output
As
Begin 

declare @Fecha_Vto				as datetime
declare @OrdenPicking			as numeric(10,0)
declare @Tipo_Posicion			as varchar(10)
declare @Codigo_Posicion		as varchar(100)
declare @Cliente_id				as varchar(15)
declare @Producto_id			as varchar(30)
declare @Cantidad				as numeric(20,5)
declare @Aux					as varchar(50)
declare @NewProducto			as varchar(30)
declare @OldProducto			as varchar(30)
declare @vQtyResto				as numeric(20,5)
declare @vRl_id					as numeric(20)
declare @QtySol					as numeric(20,5)
declare @vNroLinea				as numeric(20)
declare @NRO_BULTO				as varchar(50)
declare @NRO_LOTE				as varchar(50)
declare @EST_MERC_ID			as varchar(15)
declare @NRO_DESPACHO			as varchar(50)
declare @NRO_PARTIDA			as varchar(50)
declare @UNIDAD_ID				as varchar(5)
declare @PROP1					as varchar(100)
declare @PROP2					as varchar(100)
declare @PROP3					as varchar(100)
declare @DESC					as varchar(200)
declare @CAT_LOG_ID				as varchar(50)
declare @id						as numeric(20,0)
declare @Documento_id 			as Numeric(20,0)
declare @Saldo					as numeric(20,5)
declare @TipoSaldo				as varchar(20)
declare @Doc_Trans 				as numeric(20)
declare @QtyDetDocumento		as numeric(20)
declare @vUsuario_id			as varchar(50)
declare @vTerminal				as varchar(50)
declare @FLG_PALLET_COMPLETO	as varchar(1)
declare @RsExist				as cursor
declare @RsExist_no_pick		as Cursor
declare @RsExist_pick			as Cursor
declare @RsActuRL				as Cursor
declare @row					as int
declare @file					as varchar(max)
declare @Crit1					as varchar(30)
declare @Crit2					as varchar(30)
declare @Crit3					as varchar(30)
declare @Fecha_Alta_GTW			as datetime
declare @nro_serie				as varchar(50)
declare @NewLoteProveedor			as varchar(100)
declare @OldLoteProveedor			as varchar(100)
declare @NewNroPartida			as varchar(100)
declare @OldNroPartida			as varchar(100)
declare @NewNroSerie			as varchar(50)
declare @OldNroSerie			as varchar(50)
declare @DOCIDPIVOT				as numeric(20,0)
declare @NROLINEAPIVOT			as numeric(20,0)

SET NOCOUNT ON;

BEGIN TRY
	SELECT	@FLG_PALLET_COMPLETO = FLG_PALLET_COMPLETO 
	FROM	CLIENTE_PARAMETROS
	WHERE	CLIENTE_ID = @pCliente_id

	Select	@Crit1=CRITERIO_1, @Crit2=CRITERIO_2, @Crit3=CRITERIO_3
	From	RL_CLIENTE_LOCATOR
	Where	Cliente_id=(select Cliente_id from documento where documento_id=@pDocumento_id)

	if (@Crit1 is null) and (@Crit2 is null) and (@Crit3 is null)
	begin
		--Si todos son nulos entonces x default salgo con orden de picking.
		Set @Crit1='ORDEN_PICKING'
	end
	
	select @Cliente_id = cliente_id from DOCUMENTO where DOCUMENTO_ID = @pDocumento_id

	IF (@FLG_PALLET_COMPLETO = 0)
	BEGIN 
		EXEC LocatorEgreso @pDocumento_id,@pViaje_id
	END
	ELSE
	BEGIN
	Set @RsExist_no_pick = Cursor For
		Select	X.*
		from	(
			SELECT	 dd.fecha_vencimiento
					,isnull(p.orden_picking,99999) as ORDEN_PICKING
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
					,DD.NRO_SERIE
			FROM	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
					inner join posicion p on (rl.posicion_actual=p.posicion_id)
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
					inner join documento d on(dd.documento_id=d.documento_id)
			WHERE	rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and p.pos_lockeada='0' and p.picking='1'
					and cl.disp_egreso='1' and cl.picking='1'
					and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
					--and dd.producto_id in (select producto_id from det_documento where documento_id=@pDocumento_id)
					and exists (select 1 from det_documento ddegr
							where	ddegr.documento_id = @pDocumento_id
									and ddegr.producto_id = dd.producto_id
									and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
									and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
									and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
					and d.cliente_id = @cliente_id
			UNION
			SELECT	 dd.fecha_vencimiento
					,isnull(n.orden_locator,99999) as ORDEN_PICKING
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
					,DD.NRO_SERIE
			FROM	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
					inner join nave n on (rl.nave_actual=n.nave_id)
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
					inner join documento d on(dd.documento_id=d.documento_id)
			WHERE	rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and rl.cat_log_id<>'TRAN_EGR'
					and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' --and n.picking='1'
					and cl.disp_egreso='1' and cl.picking='1'
					and exists (select 1 from det_documento ddegr	
							where	ddegr.documento_id = @pDocumento_id
									and ddegr.producto_id = dd.producto_id
									and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
									and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
									and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
					and d.cliente_id = @cliente_id
			)X
			order by--order by producto,dd.fecha_vencimiento asc,orden  
					(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
					(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
					(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
					(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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

	Open @RsExist_no_pick
	-------------------------------------------------------------------------------------
	--	Este artilugio es porque no se si va a devolver registros el sp anterior.
	--	en caso de que no los devuelva asigno normalmente.
	-------------------------------------------------------------------------------------
	set @row=@@cursor_rows
	if @row=0
	begin
		-------------------------------------------------------------------------------------
		--llamo al locator para que asigne normalmente.
		-------------------------------------------------------------------------------------
		EXEC LocatorEgreso @pDocumento_id,@pViaje_id
		Close @RsExist_no_pick
		deallocate @RsExist_no_pick
		return
	end 
	-------------------------------------------------------------------------------------
	
	create table #tmp_consumo_locator_egr (rl_id numeric(20,0), cantidad numeric(20,5));

	Fetch Next From @RsExist_no_pick into
											@Fecha_Vto,
											@OrdenPicking,
											@Tipo_Posicion,
											@Codigo_Posicion,
											@Cliente_id,
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
											@FECHA_ALTA_GTW,
											@NRO_SERIE				
	 
	set @NewProducto=@Producto_id
	set @NewLoteProveedor=@nro_lote
	set @NewNroPartida=@nro_partida
	set @NewNroSerie=@nro_serie
	set @OldProducto=''
	set @OldLoteProveedor=''
	set @OldNroPartida=''
	set @OldNroSerie=''
	set @vNroLinea=0
	set @vQtyResto = 0
	While @@Fetch_Status=0
	Begin	

		--aca asignar si queda resto en vQtyResto y no hay mas registros para el producto anterior
		if (@OldProducto <> '') and (@NewProducto<>@OldProducto or @NewLoteProveedor<>@OldLoteProveedor or @NewNroPartida <> @OldNroPartida or @NewNroSerie<>@OldNroSerie) and (@vQtyResto>0) begin
			exec LocatorEgreso_RemanenteDoc @pDocumento_id output, @pCliente_id output, @pViaje_id output, @vNroLinea output, @OldProducto output,
						@vQtyResto, @Crit1, @Crit2, @Crit3
		end --if

		if (@NewProducto<>@OldProducto or @NewLoteProveedor<>@OldLoteProveedor or @NewNroPartida <> @OldNroPartida or @NewNroSerie<>@OldNroSerie) begin
			set @OldProducto=@NewProducto
			set @OldLoteProveedor=@NewLoteProveedor
			set @OldNroPartida=@NewNroPartida
			set @OldNroSerie=@NewNroSerie
			set @QtySol=dbo.GetQtySol(@pDocumento_id,@NROLINEAPIVOT,@Cliente_id)
			set @vQtyResto=@QtySol
		end --if			
		
		if (@vQtyResto>0) begin   
				if (@vQtyResto>=@Cantidad) begin
					set @vNroLinea=@vNroLinea+1
					set @vQtyResto=@vQtyResto-@Cantidad
					insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
								values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')
					--Insert con todas las propiedades en det_documento
					insert into det_documento_aux (
								documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
								cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
								unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
								values 
								(@pDocumento_id,@vNroLinea
								,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
								,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
								,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_Serie)	
					insert into #tmp_consumo_locator_egr values (@vRl_id, @Cantidad)	
				end
				else begin
						Set @RsExist_pick = Cursor For
							Select	X.*
							From	(
								SELECT	 dd.fecha_vencimiento
										,isnull(p.orden_picking,99999) as ORDEN_PICKING
										,'POS' as ubicacion
										,p.posicion_cod as posicion
										,dd.cliente_id
										,dd.producto_id as producto
										,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
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
										,D.FECHA_ALTA_GTW
										,dd.nro_serie
								FROM	rl_det_doc_trans_posicion rl
										inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
										inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
										inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
										inner join posicion p on (rl.posicion_actual=p.posicion_id)
										left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
										inner join documento d on(dd.documento_id=d.documento_id)
										left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
								WHERE	rl.doc_trans_id_egr is null
										and rl.nro_linea_trans_egr is null
										and rl.disponible='1'
										and isnull(em.disp_egreso,'1')='1'
										and isnull(em.picking,'1')='1'
										and p.pos_lockeada='0' and p.picking='1'
										and cl.disp_egreso='1' and cl.picking='1'
										and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
										--and dd.producto_id in (select producto_id from det_documento where documento_id=@pDocumento_id and producto_id =@Producto_id)
										--and rl.rl_id not in (select rl_id from #tmp_consumo_locator_egr)
										and exists (select 1 from det_documento ddegr	
												where	ddegr.documento_id = @pDocumento_id
														and ddegr.producto_id = dd.producto_id
														and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
														and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
														and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
										and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
								UNION
								SELECT	 dd.fecha_vencimiento
										,isnull(n.orden_locator,99999) as ORDEN_PICKING
										,'NAV' as ubicacion
										,n.nave_cod as posicion
										,dd.cliente_id
										,dd.producto_id as producto
										,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
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
										,D.FECHA_ALTA_GTW
										,dd.nro_serie
								FROM	rl_det_doc_trans_posicion rl
										inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
										inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
										inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
										inner join nave n on (rl.nave_actual=n.nave_id)
										left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
										inner join documento d on(dd.documento_id=d.documento_id)
										left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
								WHERE	rl.doc_trans_id_egr is null
										and rl.nro_linea_trans_egr is null
										and rl.disponible='1'
										and isnull(em.disp_egreso,'1')='1'
										and isnull(em.picking,'1')='1'
										and rl.cat_log_id<>'TRAN_EGR'
										and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1'
										and cl.disp_egreso='1' and cl.picking='1'
										--and dd.producto_id in (select producto_id from det_documento where documento_id=@pDocumento_id and producto_id =@Producto_id)
										--and rl.rl_id not in (select rl_id from #tmp_consumo_locator_egr)
										and exists (select 1 from det_documento ddegr	
												where	ddegr.documento_id = @pDocumento_id
														and ddegr.producto_id = dd.producto_id
														and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
														and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
														and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie)))
										and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
										)X
								order by--order by producto,dd.fecha_vencimiento asc,orden  
										(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
										(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
										(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
										(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
												
					Open @RsExist_pick
					Fetch Next From @RsExist_pick into
															@Fecha_Vto,
															@OrdenPicking,
															@Tipo_Posicion,
															@Codigo_Posicion,
															@Cliente_id,
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
															@Fecha_Alta_GTW,
															@nro_serie
					While ((@@Fetch_Status=0) AND (@vQtyResto>0))
					begin --While Picking = 1
					-- Aca se replica la logica de Pickin=1
							if (@vQtyResto>=@Cantidad) 
								begin -- (@vQtyResto>=@Cantidad) 
								set @vNroLinea=@vNroLinea+1
								set @vQtyResto=@vQtyResto-@Cantidad
								insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
											values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')
								--Insert con todas las propiedades en det_documento
								insert into det_documento_aux (
											documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
											cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
											unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
											values 
											(@pDocumento_id,@vNroLinea
											,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
											,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
											,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)
								insert into #tmp_consumo_locator_egr values (@vRl_id, @Cantidad)	
									
							end
							else begin
								set @vNroLinea=@vNroLinea+1
								insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)
											values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@vQtyResto,@vRl_id,@Cantidad-@vQtyResto,'2',getdate(),'N')
								--Insert con todas las propiedades en det_documento
								insert into det_documento_aux (
											documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
											cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
											unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
											values 
											(@pDocumento_id,@vNroLinea
											,@Cliente_id,@Producto_id,@vQtyResto,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
											,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
											,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)	
								insert into #tmp_consumo_locator_egr values (@vRl_id, @vQtyResto)	
								set @vQtyResto=0
							end --if (@vQtyResto>=@Cantidad) 
							Fetch Next From @RsExist_pick into	@Fecha_Vto,
																@OrdenPicking,
																@Tipo_Posicion,
																@Codigo_Posicion,
																@Cliente_id,
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
																@Fecha_Alta_GTW,
																@nro_serie
						end -- End While Picking = 1

					set @vQtyResto=0
				end --if
		end --if
		Fetch Next From @RsExist_no_pick into	@Fecha_Vto,
												@OrdenPicking,
												@Tipo_Posicion,
												@Codigo_Posicion,
												@Cliente_id,
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
												@Fecha_Alta_GTW,
												@nro_serie
		set @NewProducto=@Producto_id
	End	--End While @RsExist.

	-------------------------------------------------------------------------------------------------------------
	-- para contemplar el caso de que no hayan mas registros en el cursor @RsExist_no_pick y quedan remanente sin asignar
	-------------------------------------------------------------------------------------------------------------
	if (@vQtyResto>0) begin
		exec LocatorEgreso_RemanenteDoc @pDocumento_id, @pCliente_id, @pViaje_id, @vNroLinea output, @Producto_id,
						@vQtyResto, @Crit1, @Crit2, @Crit3
	end --if

	CLOSE @RsExist_no_pick
	DEALLOCATE @RsExist_no_pick

	-------------------------------------------------------------------------------------------------------------
	--Para contemplar el caso de que no hay en ubicaciones no pickeables.
	-------------------------------------------------------------------------------------------------------------
	Set @RsExist = Cursor For
		Select	x.*
		From	(
			SELECT	dd.fecha_vencimiento
					,isnull(p.orden_picking,99999) as ORDEN_PICKING
					,'POS' as ubicacion
					,p.posicion_cod as posicion
					,dd.cliente_id
					,dd.producto_id as producto
					,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
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
			FROM	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
					inner join posicion p on (rl.posicion_actual=p.posicion_id)
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 	
					inner join documento d on(dd.documento_id=d.documento_id)
					left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
			WHERE	rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and p.pos_lockeada='0' and p.picking='1'
					and cl.disp_egreso='1' and cl.picking='1'
					and rl.cat_log_id<>'TRAN_EGR' --para asegurarme que no este en proceso de egreso
					and exists (select 1 from det_documento ddegr	
								where	ddegr.documento_id = @pDocumento_id
										and ddegr.producto_id = dd.producto_id
										and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
										and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
										and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie))
										and ddegr.producto_id not in(	select	producto_id 
																			from	det_documento_aux 
																			where	documento_id=@pDocumento_id))
--					and dd.producto_id in (	select	producto_id 
--											from	det_documento 
--											where	documento_id=@pDocumento_id
--													and producto_id not in(	select	producto_id 
--																			from	det_documento_aux 
--																			where	documento_id=@pDocumento_id))
					--and rl.rl_id not in (select rl_id from #tmp_consumo_locator_egr)
					and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0

			UNION
			SELECT	 dd.fecha_vencimiento
					,isnull(n.orden_locator,99999) as ORDEN_PICKING
					,'NAV' as ubicacion
					,n.nave_cod as posicion
					,dd.cliente_id
					,dd.producto_id as producto
					,rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end) as cantidad
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
			FROM	rl_det_doc_trans_posicion rl
					inner join det_documento_transaccion ddt on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
					inner join det_documento dd ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
					inner join categoria_logica cl on (rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id )
					inner join nave n on (rl.nave_actual=n.nave_id)
					left join estado_mercaderia_rl em on (rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id) 
					inner join documento d on(dd.documento_id=d.documento_id)
					left join (select rl_id, sum(cantidad) as cantidad from #tmp_consumo_locator_egr group by rl_id) cle on (cle.rl_id = rl.rl_id)
			WHERE	rl.doc_trans_id_egr is null
					and rl.nro_linea_trans_egr is null
					and rl.disponible='1'
					and isnull(em.disp_egreso,'1')='1'
					and isnull(em.picking,'1')='1'
					and rl.cat_log_id<>'TRAN_EGR'
					and n.disp_egreso='1' and n.pre_egreso='0' and n.pre_ingreso='0' and n.picking='1'
					and cl.disp_egreso='1' and cl.picking='1'
					and exists (select 1 from det_documento ddegr	
								where	ddegr.documento_id = @pDocumento_id
										and ddegr.producto_id = dd.producto_id
										and ((isnull(ddegr.nro_lote,'')='') or (ddegr.nro_lote = dd.nro_lote))
										and ((isnull(ddegr.nro_partida,'')='') or (ddegr.nro_partida = dd.nro_partida))
										and ((isnull(ddegr.nro_serie,'')='') or (ddegr.nro_serie = dd.nro_serie))
										and ddegr.producto_id not in(	select	producto_id 
																			from	det_documento_aux 
																			where	documento_id=@pDocumento_id))
--					and dd.producto_id in (	select	producto_id 
--											from	det_documento 
--											where	documento_id=@pDocumento_id
--													and producto_id not in(	select	producto_id 
--																			from	det_documento_aux 
--																			where	documento_id=@pDocumento_id))
					--and rl.rl_id not in (select rl_id from #tmp_consumo_locator_egr)
					and (rl.cantidad - (case when cle.cantidad is null then 0 else cle.cantidad end)) > 0
					)X
			order by --producto,dd.fecha_vencimiento asc,orden 
					(CASE WHEN 1	  = 1					THEN X.PRODUCTO END), --Es Necesario para que quede ordenado el Found Set.
					(CASE WHEN @Crit1 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
					(CASE WHEN @Crit2 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
					(CASE WHEN @Crit3 = 'FECHA_VENCIMIENTO'	THEN x.FECHA_VENCIMIENTO END),
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
	Fetch Next From @RsExist into	@Fecha_Vto,
									@OrdenPicking,
									@Tipo_Posicion,
									@Codigo_Posicion,
									@Cliente_id,
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
									@Fecha_Alta_GTW,
									@nro_serie
	 
	set @NewProducto=@Producto_id
	set @NewLoteProveedor=@nro_lote
	set @NewNroPartida=@nro_partida
	set @NewNroSerie=@nro_serie
	set @OldProducto=''
	set @OldLoteProveedor=''
	set @OldNroPartida=''
	set @OldNroSerie=''
	set @vQtyResto = 0
	--set @vNroLinea=0
	While @@Fetch_Status=0
	Begin


--		if (@NewProducto<>@OldProducto) begin
--			set @OldProducto=@NewProducto
--			set @QtySol=dbo.GetQtySol(@pDocumento_id,@Cliente_id,@Producto_id)
--			set @vQtyResto=@QtySol
--		end --if	

		if (@NewProducto<>@OldProducto or @NewLoteProveedor<>@OldLoteProveedor or @NewNroPartida <> @OldNroPartida or @NewNroSerie<>@OldNroSerie) begin
			set @OldProducto=@NewProducto
			set @OldLoteProveedor=@NewLoteProveedor
			set @OldNroPartida=@NewNroPartida
			set @OldNroSerie=@NewNroSerie
			set @QtySol=dbo.GetQtySol(@pDocumento_id,@NROLINEAPIVOT,@Cliente_id)
			set @vQtyResto=@QtySol
		end --if		
		
		if (@vQtyResto>0) begin   
				if (@vQtyResto>=@Cantidad) begin
					set @vNroLinea=@vNroLinea+1
					set @vQtyResto=@vQtyResto-@Cantidad
					insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado) 
								values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@Cantidad,@vRl_id,@Cantidad-@Cantidad,'1',getdate(),'N')
					--Insert con todas las propiedades en det_documento
					insert into det_documento_aux (
								documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
								cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
								unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
								values 
								(@pDocumento_id,@vNroLinea
								,@Cliente_id,@Producto_id,@Cantidad,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
								,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
								,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_serie)	
					insert into #tmp_consumo_locator_egr values (@vRl_id, @Cantidad)	

				end
				else begin
					set @vNroLinea=@vNroLinea+1
					insert into consumo_locator_egr (documento_id,nro_linea,cliente_id,producto_id,cantidad,rl_id,saldo,tipo,fecha,procesado)
								values  (@pDocumento_id,@vNroLinea,@Cliente_id,@Producto_id,@vQtyResto,@vRl_id,@Cantidad-@vQtyResto,'2',getdate(),'N')
					--Insert con todas las propiedades en det_documento
					insert into det_documento_aux (
								documento_id,nro_linea,cliente_id,producto_id,cantidad,est_merc_id,
								cat_log_id,nro_bulto,descripcion,nro_lote,fecha_vencimiento,nro_despacho,nro_partida,
								unidad_id,tie_in,item_ok,cat_log_id_final,prop1,prop2,prop3,cant_solicitada,nro_serie)
								values 
								(@pDocumento_id,@vNroLinea
								,@Cliente_id,@Producto_id,@vQtyResto,@EST_MERC_ID,'TRAN_EGR',@NRO_BULTO,@DESC
								,@NRO_LOTE,@Fecha_Vto,@NRO_DESPACHO,@NRO_PARTIDA,@UNIDAD_ID,'0'
								,'1',@CAT_LOG_ID,@PROP1,@PROP2,@PROP3,@QtySol,@nro_Serie)
					insert into #tmp_consumo_locator_egr values (@vRl_id, @vQtyResto)		
					set @vQtyResto=0
				end --if
		end --if
		
		Fetch Next From @RsExist into	@Fecha_Vto,
										@OrdenPicking,
										@Tipo_Posicion,
										@Codigo_Posicion,
										@Cliente_id,
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
										@Fecha_Alta_GTW,
										@nro_serie
		set @NewProducto=@Producto_id
	End	--End While @RsExist.

	CLOSE @RsExist
	DEALLOCATE @RsExist

	--GUARDO SERIES INICIALES
	SELECT DISTINCT NRO_SERIE INTO #TMPSERIES FROM DET_DOCUMENTO WHERE DOCUMENTO_ID = @pDocumento_id

	--Borro det_documento y lo vuelvo a insertar con las nuevas propiedades
	delete det_documento where documento_id=@pDocumento_id
	insert into det_documento select 	DOCUMENTO_ID,	ROW_NUMBER()OVER(ORDER BY NRO_LINEA ASC),	CLIENTE_ID,
										PRODUCTO_ID,	CANTIDAD,	NRO_SERIE,	NRO_SERIE_PADRE,	EST_MERC_ID,
										CAT_LOG_ID,		NRO_BULTO,	DESCRIPCION,	NRO_LOTE,	FECHA_VENCIMIENTO,
										NRO_DESPACHO,	NRO_PARTIDA,	UNIDAD_ID,	PESO,	UNIDAD_PESO,	VOLUMEN,
										UNIDAD_VOLUMEN,	BUSC_INDIVIDUAL,	TIE_IN,	NRO_TIE_IN_PADRE,	NRO_TIE_IN,
										ITEM_OK,	CAT_LOG_ID_FINAL,	MONEDA_ID,	COSTO,	PROP1,	PROP2,
										PROP3,	LARGO,	ALTO,	ANCHO,	VOLUMEN_UNITARIO,	PESO_UNITARIO,
										CANT_SOLICITADA,	TRACE_BACK_ORDER
								from	det_documento_aux 
								where	documento_id=@pDocumento_id
	------CONTROLO QUE SERIES FUERON OBLIGATORIAS Y CUALES NO.
	UPDATE DET_DOCUMENTO
	SET NRO_SERIE = NULL
	WHERE DOCUMENTO_ID = @pDocumento_id
			AND NOT EXISTS (SELECT 1 FROM #TMPSERIES WHERE NRO_SERIE = DET_DOCUMENTO.NRO_SERIE)

	update documento set status='D20' where documento_id=@pDocumento_id
	Exec Asigna_Tratamiento#Asigna_Tratamiento_EGR @pDocumento_id

	select distinct @Doc_Trans=doc_trans_id from det_documento_transaccion where documento_id=@pDocumento_id

	--Hago la reserva en RL
	Set @RsActuRL = Cursor For select [id],documento_id,Nro_Linea,Cliente_id,Producto_id,Cantidad,rl_id,saldo,tipo from consumo_locator_egr where procesado='N' and Documento_id=@pDocumento_id

	Open @RsActuRL
	Fetch Next From @RsActuRL into 	@id,
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

		Fetch Next From @RsActuRL into 	@id,
										@Documento_id,
										@vNroLinea,
										@Cliente_id,
										@Producto_id,
										@Cantidad,
										@vRl_id,
										@Saldo,
										@TipoSaldo
	End	--End While @RsActuRL.
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

	Set NoCount Off;
	END -- ELSE @FLG_PALLET_COMPLETO = 0
END TRY

BEGIN CATCH
     EXEC usp_RethrowError;
END CATCH;


END
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