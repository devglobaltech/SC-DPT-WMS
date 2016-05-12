IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AJUSTES_MASIVOS_INS]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[AJUSTES_MASIVOS_INS]
GO

CREATE procedure [dbo].[AJUSTES_MASIVOS_INS](
	@CLIENTE_ID			varchar(15),
	@PRODUCTO_ID		varchar(30),
	@NRO_SERIE			varchar(50),
	@EST_MERC_ID		varchar(15),
	@CAT_LOG_ID			varchar(50),
	@NRO_BULTO			varchar(50),
	@NRO_LOTE			varchar(50),
	@FECHA_VENCIMIENTO	datetime,
	@NRO_DESPACHO		varchar(50),
	@NRO_PARTIDA		varchar(50),
	@PROP1				varchar(100),
	@PROP2				varchar(100),
	@PROP3				varchar(100),
	@POSICION			varchar(45),
	@NAVE				varchar(45),
	@CANTIDAD_AJUSTE	numeric(20,5)
)as
begin
	set xact_abort on
	
	declare @VCONTROL		numeric(20,0)
	declare @POSICION_ID	numeric(20,0)
	declare @NAVE_ID		numeric(20,0)
	declare @Serie_ing		char(1)
	
	begin try
	
		--Validaciones.
		if ((@NAVE IS NULL)AND(LTRIM(RTRIM(@NAVE))='')) OR((@POSICION IS NULL)AND(LTRIM(RTRIM(@POSICION))=''))
		Begin
			Raiserror('Debe especificar un codigo de nave o codigo de posicion para continuar.',16,1);
			return
		End
		
		if (LTRIM(rtrim(@NAVE))<>'')and(LTRIM(RTRIM(@POSICION))<>'') begin
			Raiserror('Debe especificar la posicion ó la nave.',16,1);
			return
		end
		
		select	@vcontrol=COUNT(*)
		from	PRODUCTO
		where	CLIENTE_ID=@CLIENTE_ID
				and PRODUCTO_ID=@PRODUCTO_ID;
		
		if @vcontrol=0 begin
			Raiserror('El producto ingresado no existe para el cliente indicado.',16,1);
			return		
		end;
		set @vcontrol=null;
		
		if (LTRIM(rtrim(@EST_MERC_ID))<>'')begin

			select	@VCONTROL=COUNT(*)
			from	ESTADO_MERCADERIA_RL
			where	CLIENTE_ID=@CLIENTE_ID
					and EST_MERC_ID=@EST_MERC_ID;
			if @VCONTROL=0 begin
				Raiserror('El codigo de estado de mercaderia no existe para el cliente indicado.',16,1);
				return			
			end;
		end
		
		if ltrim(rtrim(@CAT_LOG_ID))='' begin
			Raiserror('Debe indicar el estado logico de la mercaderia.',16,1);
			return		
		end
		else
		begin
			set @VCONTROL=null;
			select	@VCONTROL=COUNT(*) 
			from	CATEGORIA_LOGICA
			where	CLIENTE_ID=@CLIENTE_ID
					and CAT_LOG_ID=@CAT_LOG_ID;
			
			if @VCONTROL=0 begin
				Raiserror('El estado logico indicado no existe.',16,1);
				return			
			end
		end
		
		if LTRIM(rtrim(@posicion))<>'' begin
		
			select	@POSICION_ID=posicion_id
			from	POSICION
			where	POSICION_COD=LTRIM(rtrim(upper(@posicion)));
			
			if @posicion_id is null begin
				raiserror('La posicion ingresada no existe.',16,1)
			end
			
		end
		else
		begin
			select	@NAVE_ID=nave_id
			from	NAVE
			where	NAVE_COD=@NAVE;
					
			if @NAVE_ID is null begin
				raiserror('La Nave ingresada no existe.',16,1)
			end
		end
		
		IF @CANTIDAD_AJUSTE IS NULL BEGIN
			Raiserror('La cantidad a ajustar no puede ser nula.',16,1);
			return					
		END
		
		select	@serie_ing=isnull(serie_ing,'0')
		from	PRODUCTO
		where	CLIENTE_ID=@CLIENTE_ID
				and PRODUCTO_ID=@PRODUCTO_ID
				
		if (@Serie_ing='1')begin
			if ltrim(rtrim(@NRO_SERIE))='' or @NRO_SERIE is null begin
				raiserror('El producto requiere serializacion al ingreso, debe indicar el numero de serie.',16,1)
				return
			end
			if @CANTIDAD_AJUSTE>1 begin
				raiserror('El producto es serializado, solo puede ingresar una unidad por serie.',16,1)
				return
			end
			if @CANTIDAD_AJUSTE<0 begin
				raiserror('No es posible ajustar negativamente un producto serializado.',16,1)
				return
			end
			
		end
		
		INSERT INTO AJUSTES_MASIVOS (	CLIENTE_ID, PRODUCTO_ID, NRO_SERIE, EST_MERC_ID, CAT_LOG_ID, NRO_BULTO, NRO_LOTE, FECHA_VENCIMIENTO, NRO_DESPACHO, 
										NRO_PARTIDA, PROP1, PROP2, PROP3, POSICION_ID, NAVE_ID, CANTIDAD_AJUSTE)
		VALUES(	LTRIM(RTRIM(UPPER(@CLIENTE_ID))),
				LTRIM(RTRIM(UPPER(@PRODUCTO_ID))),
				LTRIM(RTRIM(UPPER(@NRO_SERIE))),
				@EST_MERC_ID,
				@CAT_LOG_ID,
				LTRIM(RTRIM(UPPER(@NRO_BULTO))),
				LTRIM(RTRIM(UPPER(@NRO_LOTE))),
				@FECHA_VENCIMIENTO,
				LTRIM(RTRIM(UPPER(@NRO_DESPACHO))),
				LTRIM(RTRIM(UPPER(@NRO_PARTIDA))),
				LTRIM(RTRIM(UPPER(@PROP1))),
				LTRIM(RTRIM(UPPER(@PROP2))),
				LTRIM(RTRIM(UPPER(@PROP3))),
				@POSICION_ID,
				@NAVE_ID,
				@CANTIDAD_AJUSTE);
			
	end try
	begin catch
		EXEC usp_RethrowError
	end catch
end
GO


