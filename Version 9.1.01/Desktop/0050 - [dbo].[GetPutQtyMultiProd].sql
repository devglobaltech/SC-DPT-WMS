/****** Object:  StoredProcedure [dbo].[GetPutQtyMultiProd]    Script Date: 09/23/2013 14:59:12 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetPutQtyMultiProd]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[GetPutQtyMultiProd]
GO

CREATE  procedure [dbo].[GetPutQtyMultiProd](
@pCliente_id 		as varchar(15),
@pDoc_ext			as varchar(100),
@pProducto_id		as varchar(30), 
@pNro_Linea			as numeric(20),
@pQtySol				as numeric(30,5)

) 
AS
BEGIN
declare @RsTemp			as Cursor
declare @vIdTemp			as numeric(30,0)
declare @vCliente_id		as varchar(15)
declare @vDoc_ext			as varchar(100)	
declare @vProducto_id	as varchar(30)
declare @vNave_id			as numeric(20,0)
declare @vNave_Cod		as varchar(100)
declare @vQty 				as numeric(30,5)
declare @pQtySolOriginal as numeric(30,5)
declare @vEst_Merc_id			varchar(50)		
declare @vcat_log_id			varchar(50)
declare @vNro_Bulto			varchar(100)
declare @vDescripcion			varchar(500)
declare @vNro_Lote				varchar(100)
declare @vNro_pallet			varchar(100)
declare @vFecha_Vto			datetime
declare @vNro_Despacho		varchar(100)
declare @vNro_Partida			varchar(100)
declare @vUnidad_id			varchar(5)
declare @vDocumento_id		numeric(20,0)	
declare @NewNroLinea			numeric(20,0)	

Set @RsTemp = Cursor For select * from #Temp_PedMultiPickeados where cliente_id=@pCliente_id and doc_ext=@pDoc_ext and producto_id=@pProducto_id
Set @pQtySolOriginal=@pQtySol

Open @RsTemp
Fetch Next From @RsTemp into @vIdTemp,@vCliente_id,@vDoc_ext,@vProducto_id,@vNave_id,@vNave_Cod,@vQty,@vEst_Merc_id,@vcat_log_id,
									  @vNro_Bulto,@vDescripcion,@vNro_Lote,@vNro_pallet,@vFecha_Vto,@vNro_Despacho,@vNro_Partida,@vUnidad_id,@vDocumento_id

While (@@Fetch_Status=0 and @pQtySol>0) Begin
	if (@pQtySol>=@vQty) begin
			--Averiguo la maxima linea para no probocar error de pk
			select @NewNroLinea=max(nro_linea) from sys_dev_det_documento where cliente_id=@pCliente_id and doc_ext=@pDoc_ext
			
			--Hago el insert en sys_dev_det_documento
				insert into sys_dev_det_documento 
						select	 doc_ext
								,isnull(@NewNroLinea,0)+1
								,cliente_id
								,producto_id
								,cantidad_solicitada
								,@vQty as cantidad
								,@vEst_Merc_id
								,@vcat_log_id
								,@vNro_Bulto
								,@vDescripcion
								,@vNro_Lote
								,@vNro_pallet
								,@vFecha_Vto
								,@vNro_Despacho
								,@vNro_Partida
								,@vUnidad_id
								,unidad_contenedora_id
								,peso
								,unidad_peso
								,volumen
								,unidad_volumen
								,prop1
								,prop2
								,prop3
								,largo
								,alto
								,ancho
								,doc_back_order
								,null
								,null
								,'PP'
								,getdate()
								,@vDocumento_id as documento_id
								,@vNave_id as nave_id
								,@vNave_Cod as nave_cod
								,Null	--Flg_Movimiento.
								,CUSTOMS_1
								,CUSTOMS_2
								,CUSTOMS_3
								,NULL AS NRO_CMR
						from	sys_int_det_documento
						where	cliente_id=@pCliente_id and doc_ext=@pDoc_ext and nro_linea=@pNro_Linea
			--Elimino el registro en #Temp_PedMultiPickeados y salgo del while
			delete #Temp_PedMultiPickeados where Id_Temp=@vIdTemp
			set @pQtySol=@pQtySol-@vQty
	end 
	else begin
			--Averiguo la maxima linea para no probocar error de pk
			select @NewNroLinea=max(nro_linea) from sys_dev_det_documento where cliente_id=@pCliente_id and doc_ext=@pDoc_ext

			--Hago el insert en sys_dev_det_documento
				insert into sys_dev_det_documento 
						select	 doc_ext
								,isnull(@NewNroLinea,0)+1
								,cliente_id
								,producto_id
								,cantidad_solicitada
								,@pQtySol as cantidad
								,@vEst_Merc_id
								,@vcat_log_id
								,@vNro_Bulto
								,@vDescripcion
								,@vNro_Lote
								,@vNro_pallet
								,@vFecha_Vto
								,@vNro_Despacho
								,@vNro_Partida
								,@vUnidad_id
								,unidad_contenedora_id
								,peso
								,unidad_peso
								,volumen
								,unidad_volumen
								,prop1
								,prop2
								,prop3
								,largo
								,alto
								,ancho
								,doc_back_order
								,null
								,null
								,'PP'
								,getdate()
								,@vDocumento_id as documento_id
								,@vNave_id as nave_id
								,@vNave_Cod as nave_cod
								,Null --Flg_Movimiento
								,CUSTOMS_1
								,CUSTOMS_2
								,CUSTOMS_3
								,NULL AS NRO_CMR
						from	sys_int_det_documento
						where	cliente_id=@pCliente_id and doc_ext=@pDoc_ext and nro_linea=@pNro_Linea

			--Update en #Temp_PedMultiPickeados
			update #Temp_PedMultiPickeados set QtyPend=QtyPend-@pQtySol where Id_Temp=@vIdTemp
			set @pQtySol=0
	end --if	
	Fetch Next From @RsTemp into @vIdTemp,@vCliente_id,@vDoc_ext,@vProducto_id,@vNave_id,@vNave_Cod,@vQty,@vEst_Merc_id,@vcat_log_id,
									  @vNro_Bulto,@vDescripcion,@vNro_Lote,@vNro_pallet,@vFecha_Vto,@vNro_Despacho,@vNro_Partida,@vUnidad_id,@vDocumento_id
end --while

if (@pQtySolOriginal=@pQtySol) begin
	
   --Averiguo la maxima linea para no probocar error de pk
	select @NewNroLinea=max(nro_linea) from sys_dev_det_documento where cliente_id=@pCliente_id and doc_ext=@pDoc_ext

	--Hago el insert de la linea con cantidad=0
	insert into sys_dev_det_documento 	
		select	 doc_ext
				,isnull(@NewNroLinea,0)+1
				,cliente_id
				,producto_id
				,cantidad_solicitada
				,0 as cantidad
				,@vEst_Merc_id
				,@vcat_log_id
				,@vNro_Bulto
				,@vDescripcion
				,@vNro_Lote
				,@vNro_pallet
				,@vFecha_Vto
				,@vNro_Despacho
				,@vNro_Partida
				,@vUnidad_id
				,unidad_contenedora_id
				,peso
				,unidad_peso
				,volumen
				,unidad_volumen
				,prop1
				,prop2
				,prop3
				,largo
				,alto
				,ancho
				,doc_back_order
				,Null
				,Null
				,'PP'
				,getdate()
				,@vDocumento_id as documento_id
				,@vNave_id as nave_id
				,@vNave_Cod as nave_cod
				,Null --Flg_Movimiento
				,CUSTOMS_1
				,CUSTOMS_2
				,CUSTOMS_3
				,NULL AS NRO_CMR
		from	sys_int_det_documento
		where	cliente_id=@pCliente_id and doc_ext=@pDoc_ext and nro_linea=@pNro_Linea

	set @pQtySolOriginal=@pQtySol --temporal
end --if

END

CLOSE @RsTemp
DEALLOCATE @RsTemp

GO


