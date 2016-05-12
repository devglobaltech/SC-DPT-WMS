Alter Procedure DBO.Consolidar_Existencias
	@Cliente_id		as varchar(15),
	@Producto_id	as varchar(30)
as
begin
	
	select	rl.* into #tmp_rl 
	from	det_documento dd inner join det_documento_transaccion ddt
			on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
			inner join rl_det_doc_trans_posicion rl
			on(ddt.doc_trans_id=rl.doc_trans_id and ddt.nro_linea_trans=rl.nro_linea_trans)
	where	dd.cliente_id=@cliente_id
			and dd.producto_id=@Producto_id
			
	delete from rl_det_doc_trans_posicion where rl_id in(select rl_id from #tmp_rl)
	
	insert into rl_det_doc_trans_posicion (	DOC_TRANS_ID,		NRO_LINEA_TRANS,		POSICION_ANTERIOR,		POSICION_ACTUAL,		CANTIDAD,
											TIPO_MOVIMIENTO_ID,	ULTIMA_ESTACION,		ULTIMA_SECUENCIA,		NAVE_ANTERIOR,			NAVE_ACTUAL,
											DOCUMENTO_ID,		NRO_LINEA,				DISPONIBLE,				DOC_TRANS_ID_EGR,		NRO_LINEA_TRANS_EGR,
											DOC_TRANS_ID_TR,	NRO_LINEA_TRANS_TR,		CLIENTE_ID,				CAT_LOG_ID,				EST_MERC_ID)
											
	SELECT	DOC_TRANS_ID,		NRO_LINEA_TRANS,		POSICION_ANTERIOR,		POSICION_ACTUAL,
			SUM(CANTIDAD),		TIPO_MOVIMIENTO_ID,		ULTIMA_ESTACION,		ULTIMA_SECUENCIA,
			NAVE_ANTERIOR,		NAVE_ACTUAL,			DOCUMENTO_ID,			NRO_LINEA,
			DISPONIBLE,			DOC_TRANS_ID_EGR,		NRO_LINEA_TRANS_EGR,	DOC_TRANS_ID_TR,
			NRO_LINEA_TRANS_TR,	CLIENTE_ID,				CAT_LOG_ID,				EST_MERC_ID
	FROM	#tmp_rl
	GROUP BY
			DOC_TRANS_ID,		NRO_LINEA_TRANS,		POSICION_ANTERIOR,		POSICION_ACTUAL,
			TIPO_MOVIMIENTO_ID,	ULTIMA_ESTACION,		ULTIMA_SECUENCIA,		NAVE_ANTERIOR,		
			NAVE_ACTUAL,		DOCUMENTO_ID,			NRO_LINEA,				DISPONIBLE,			
			DOC_TRANS_ID_EGR,	NRO_LINEA_TRANS_EGR,	DOC_TRANS_ID_TR,		NRO_LINEA_TRANS_TR,	
			CLIENTE_ID,			CAT_LOG_ID,				EST_MERC_ID	
			
end--Fin procedure.