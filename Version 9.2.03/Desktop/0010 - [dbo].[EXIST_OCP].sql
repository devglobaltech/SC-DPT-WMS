/****** Object:  StoredProcedure [dbo].[EXIST_OCP]    Script Date: 09/10/2014 13:24:25 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EXIST_OCP]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[EXIST_OCP]
GO


CREATE PROCEDURE [dbo].[EXIST_OCP]
@CLIENTE_ID		  VARCHAR(15),
@ODC			      VARCHAR(100),
@PRODUCTO_ID	  VARCHAR(30),
@loteProveedor  VARCHAR(100),
@partida        VARCHAR(100),
@STATUS			    CHAR(1) output,
@doc_ext        varchar(100) output
AS
BEGIN
	/*
	STATUS=0 -> NO EXISTE
	STATUS=1 -> EXISTE OK
	STATUS=2 -> EXISTE PERO ESTA COMPLETADA.
	*/
	Declare @Control as smallint
	
	select 	@control=count(*) 
	from 	sys_int_documento 
	where 	orden_de_compra=@ODC
			and cliente_id=@cliente_id

	if @control>0
	begin
		set @control=0
    set @doc_ext=null
		select 	@doc_ext=sd.doc_ext
		from	sys_int_documento sd inner join sys_int_det_documento sdd 	on(sd.cliente_id=sdd.cliente_id and sd.doc_ext=sdd.doc_ext )
		where	sd.cliente_id=@cliente_id
				and sdd.producto_id=@producto_id
				and sd.orden_de_compra=@odc
				--AND isnull(sdd.nro_lote,'') = @loteProveedor
				--and isnull(sdd.NRO_PARTIDA,'') = @partida
				AND ((SDD.NRO_LOTE IS NULL OR SDD.NRO_LOTE='')OR(SDD.NRO_LOTE=@loteProveedor))
				AND ((SDD.NRO_PARTIDA IS NULL OR SDD.NRO_PARTIDA='')OR(SDD.NRO_PARTIDA=@partida))
				and sdd.fecha_estado_gt is null
				and sdd.estado_gt is null

    if (@doc_ext is not null)
      set @control=1
      
		if @control>0
		begin
			set @status='1'
			return
		end
		else
		begin
			set @status='2'
			raiserror('La orden de compra %s para el cliente %s ya esta finalizada.',16,1,@odc, @cliente_id)	
		end
	End
	else
	begin
		set @status=0
		raiserror('No existe la orden de compra %s para el cliente %s',16,1,@odc, @cliente_id)
		return
	end
END

GO


