/****** Object:  StoredProcedure [dbo].[Locator_Transf_Contenedora]    Script Date: 10/01/2015 12:38:05 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Locator_Transf_Contenedora]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Locator_Transf_Contenedora]
GO

CREATE  Procedure [dbo].[Locator_Transf_Contenedora]
	@UbicacionOrigen	varchar(50),
	@NroContenedora		varchar(100)
As
Begin
	Declare @CantProducto 	as Float
	Declare @Cliente_ID 	as varchar(15)
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
			inner join rl_det_doc_trans_posicion rl 					on(ddt.doc_trans_id=rl.doc_trans_id and ddt.nro_linea_trans=rl.nro_linea_trans)
			left join posicion p										on(rl.posicion_actual=p.posicion_id)
			left join nave n											on(rl.nave_actual=n.nave_id)
			left join estado_mercaderia_rl em							on(rl.cliente_id=em.cliente_id and rl.est_merc_id=em.est_merc_id)
			inner join categoria_logica cl								on(rl.cliente_id=cl.cliente_id and rl.cat_log_id=cl.cat_log_id)
	where	dd.nro_bulto=@NroContenedora
			and ((p.posicion_cod=@UbicacionOrigen) or(n.nave_cod=@UbicacionOrigen))
			and cl.disp_transf='1'
			and ((em.disp_transf is null) or (em.disp_transf='1'))
			and rl.disponible='1'
	group by dd.producto_id, dd.cliente_id

	
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
					where	p.pos_vacia='1' and p.pos_lockeada='0'
							and rlpp.posicion_id not in(	select 	isnull(posicion_id,0)	from 	sys_locator_ing)
							and rlpp.posicion_id not in(select posicion_actual from rl_det_doc_trans_posicion where posicion_actual is not null)
							and rlpp.producto_id=ltrim(rtrim(upper(@producto_id))) 
							and rlpp.cliente_id=ltrim(rtrim(upper(@cliente_id)))
							and p.posicion_cod<>@UbicacionOrigen
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
					where	n.disp_ingreso='1' and n.pre_ingreso='0' 
							and pre_egreso='0'
							and rlpp.producto_id=ltrim(rtrim(upper(@producto_id)))
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
						where	p.pos_vacia='1' and p.pos_lockeada='0'
								and p.posicion_id not in(	select posicion_id from sys_locator_ing where posicion_id is not null)
								and p.posicion_cod not in (@UbicacionOrigen)
						union all
						select 	 null as posicion_id
								,nave_cod as posicion_cod
								,nave_id  as nave_id
								,isnull(orden_locator,999999) as ordenlocator
								,0 as x1
						from 	nave n
						where	n.disp_ingreso='1' and n.pre_ingreso='0' 
								and pre_egreso='0'
								and nave_tiene_layout='0'
								and nave_cod<>@UbicacionOrigen
				)as x
				order by x.ordenlocator
			select 	@posicion_id as posicion_id, @posicion_cod as posicion_cod,@nave_id as nave_id, @orden_locator as orden_locator
	end



End

GO


