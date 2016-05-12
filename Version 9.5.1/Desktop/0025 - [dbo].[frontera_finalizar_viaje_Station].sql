
/****** Object:  StoredProcedure [dbo].[frontera_finalizar_viaje_Station]    Script Date: 09/10/2015 10:47:00 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[frontera_finalizar_viaje_Station]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[frontera_finalizar_viaje_Station]
GO

CREATE       PROCEDURE [dbo].[frontera_finalizar_viaje_Station]
@viaje_id	as varchar(100)output
as
begin
	
	declare @doc_trans_id	as numeric(20,0)
	declare @err			as int
	declare @status			as varchar(3)	
	declare @documento_id	as numeric(20,0)
	--declare @fi				as datetime --comentar solo sirve para trazar la ejecucion del proceso.

	declare cur_ffv cursor for
	SELECT	DISTINCT X.DOC_TRANS_ID
	FROM	(	/*
				select 	distinct
						ddt.doc_trans_id
				from 	sys_int_documento sd
						inner join sys_int_det_documento sdd
						--Catalina Castillo.Tracker 4909.Se agrega filtro por CLiente_Id
						on(sd.doc_ext=sdd.doc_ext) and (sd.Cliente_id=sdd.Cliente_id)
						inner join documento d
						on(sdd.documento_id=d.documento_id)
						inner join det_documento dd
						on(d.documento_id=dd.documento_id) 
						inner join det_documento_transaccion ddt
						on(dd.documento_id=ddt.documento_id and dd.nro_linea=ddt.nro_linea_doc)
				where 	codigo_viaje=ltrim(rtrim(upper(@viaje_id)))
						and sdd.documento_id is not null
				UNION
				*/
				SELECT	DISTINCT
						DDT.DOC_TRANS_ID
				FROM	PICKING P INNER JOIN DET_DOCUMENTO DD		ON(DD.DOCUMENTO_ID=P.DOCUMENTO_ID AND DD.NRO_LINEA=P.NRO_LINEA)
						INNER JOIN DET_DOCUMENTO_TRANSACCION DDT	ON(DDT.DOCUMENTO_ID=DD.DOCUMENTO_ID AND DDT.NRO_LINEA_DOC=DD.NRO_LINEA)
				WHERE	VIAJE_ID=@viaje_id)X

	open cur_ffv
	--set @fi=getdate()
	fetch next from cur_ffv into @doc_trans_id
	while @@fetch_status=0
	Begin
	
		select	@status=status
		from	documento_transaccion
		where	doc_trans_id=@doc_trans_id

		while @status <>'T40'
		begin
	
			exec egr_aceptar @doc_trans_id
	
			select	@status=status
			from	documento_transaccion
			where	doc_trans_id=@doc_trans_id
		end
		select distinct @documento_id=documento_id from det_documento_transaccion where doc_trans_id=@doc_trans_id
		update picking set facturado='1' where documento_id=@documento_id
		fetch next from cur_ffv into @doc_trans_id
	End

	--select datediff(ms,@fi,getdate())--comentar solo sirve para trazar la ejecucion del proceso.

	close cur_ffv
	deallocate cur_ffv

end
GO
