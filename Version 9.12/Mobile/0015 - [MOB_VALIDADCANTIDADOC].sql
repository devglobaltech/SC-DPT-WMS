/****** Object:  StoredProcedure [dbo].[Mob_ValidadCantidadOC]    Script Date: 07/16/2013 12:48:02 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_ValidadCantidadOC]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_ValidadCantidadOC]
GO
CREATE procedure [dbo].[MOB_VALIDADCANTIDADOC]
@Cantidad numeric(20,0) ,
@Producto_id varchar(20),
@Agente_id varchar(20),
@result varchar(5) output
as
begin

declare @ToleranciaMinima REAL
declare @ToleranciaMaxima REAL
declare @CantidadSolicitada REAL
declare @CantidadIngresada REAL
declare @Dif REAL

 DECLARE @USUARIO VARCHAR(20)  
   
 SELECT @USUARIO=USUARIO_ID FROM #TEMP_USUARIO_LOGGIN  
	--SET @USUARIO = '001'
--MODIFICACION PARA CONTEMPLAR PRUCTOS INGRESADOS.
 set @CantidadIngresada = (select isnull( sum(tp.cantidad),0) 
													from tmp_producto tp 
													where tp.producto_id = @Producto_id --'0001RBL-P'
															and tp.proveedor_id = @Agente_id--'0001'
															and tp.usuario = @USUARIO)


SET @Cantidad = @Cantidad + @CantidadIngresada

 select 
	@ToleranciaMinima = p.tolerancia_min,
	@ToleranciaMaxima = p.tolerancia_max,	
	@cantidadSolicitada = sum (sdd.cantidad_solicitada),
	@Dif = (@cantidad * 100) / @cantidadSolicitada
 from sys_int_documento sd  
 
	inner join sys_int_det_documento sdd 
		on(SD.CLIENTE_ID=SDD.CLIENTE_ID AND SD.DOC_EXT=SDD.DOC_EXT)  
 
	inner join producto p 
		on (p.producto_id=sdd.producto_id)  
		and p.producto_id = @producto_id

 where agente_id = @agente_id and
	 sdd.estado_gt is null     
group by p.tolerancia_min, p.tolerancia_max

--para sacar el delta del total de la cant solicitada
set @Dif = @Dif - 100
set @result = '0'

if @Dif <  0 
	begin
		if @Dif * -1 > @toleranciaminima
			begin
				set @result = '-1'
			end
	end
else
	begin
		if @Dif > @toleranciamaxima
		begin
			set @result = '1'
		end
	end

--select @result as resultado, @dif   as dif, @toleranciaminima as toleranciamin, @toleranciamaxima as toleranciamax
end
