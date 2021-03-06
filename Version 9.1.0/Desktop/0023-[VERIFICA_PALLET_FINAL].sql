
ALTER       FUNCTION [dbo].[VERIFICA_PALLET_FINAL](
@POSICION_COD		VARCHAR(45),
@VIAJE_ID			VARCHAR(100),
@RUTA				VARCHAR(100),
@PALLET				VARCHAR(50)) Returns Int
AS
BEGIN
	DECLARE  @ControlRl		Float
	DECLARE  @ControlPick	Float
	DECLARE  @CONT			BigInt
	DECLARE  @Retorno		Int
	DECLARE	 @NAV			SMALLINT
	DECLARE  @POS			SMALLINT

	--N
	SELECT	@POS=COUNT(*) FROM POSICION (NOLOCK) WHERE POSICION_COD=@POSICION_COD
	SELECT	@NAV=COUNT(*) FROM NAVE (NOLOCK) WHERE NAVE_COD=@POSICION_COD

	Set @Retorno=5
	--------------------------------------------------------------------------------------------------------------------
	--Uno tengo que determinar si es lo unico en rl
	--------------------------------------------------------------------------------------------------------------------
	IF @POS=1 
	BEGIN
		Select 	@ControlRl=Count(*)
		from	Rl_det_doc_trans_posicion rl (NOLOCK)	inner join Posicion P (NOLOCK) on(Rl.Posicion_Actual=P.Posicion_ID)
		Where	P.Posicion_Cod=@Posicion_Cod
	END
	ELSE
	BEGIN
		--N
		SELECT	@ControlRl=count(rl.rl_id)
		FROM 	rl_det_doc_trans_posicion rl(NOLOCK)
				inner join det_documento_transaccion ddt (NOLOCK)on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (NOLOCK)ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join nave n on(rl.nave_actual=n.nave_id)
		Where	n.nave_COD=@Posicion_Cod
				and dd.prop1=@PALLET	
				and rl.disponible='1'	
	END
	--------------------------------------------------------------------------------------------------------------------
	--Luego tengo que saber si hay algo en picking pendiente a la tarea.
	--------------------------------------------------------------------------------------------------------------------
	/*
	Select 	@ControlPick=Count(*)	
	From	Picking 
	Where	Posicion_Cod=@Posicion_Cod
			and fecha_Inicio is null
			and fecha_fin is null
			and cant_confirmada is null
			and pallet_picking is null
			and facturado=0
	*/
	--1) Caso.... Productos distintos en la posicion para pickear y que no hayan sido pickeados.
	--2) la posicion no tenga nada en otros viajes
	--3) Que la posicion  este en otra ruta.
	--4) que la posicion no tenga otro pallet.
	if(@ControlRl>0)
	begin
		set @Retorno=0
	end
	Else
	begin
		set @cont=null
		IF @POS=1
		BEGIN
			SELECT @CONT=COUNT(VIAJE_ID) FROM PICKING WHERE POSICION_COD=@POSICION_COD AND VIAJE_ID<>@VIAJE_ID AND FIN_PICKING IN('0','1')
		END
		IF @NAV=1
		BEGIN
			SELECT @CONT=COUNT(VIAJE_ID) FROM PICKING WHERE POSICION_COD=@POSICION_COD AND VIAJE_ID<>@VIAJE_ID AND PROP1=@PALLET AND FIN_PICKING IN('0','1')
		END

		IF @CONT> 0
		begin
			set @retorno=0
		end
		else
		begin
			set @cont=null
			IF @POS=1
			BEGIN
				SELECT @CONT=COUNT(VIAJE_ID) FROM PICKING WHERE POSICION_COD=@POSICION_COD AND  ruta<>@ruta AND FIN_PICKING IN('0','1')
			END
			IF @NAV=1
			BEGIN
				SELECT @CONT=COUNT(VIAJE_ID) FROM PICKING WHERE POSICION_COD=@POSICION_COD AND PROP1=@PALLET AND ruta<>@ruta AND FIN_PICKING IN('0','1')
			END
			IF @CONT> 0
			begin
				set @retorno=0
			end
			else
			begin
				set @cont=null
				IF @POS=1
				BEGIN
					SELECT @CONT=COUNT(VIAJE_ID) FROM PICKING WHERE POSICION_COD=@POSICION_COD AND PROP1<>@PALLET AND FIN_PICKING IN('0','1')
				END
				ELSE
				BEGIN
					set @cont=0
				END
				IF @cont>0
				begin
					Set @Retorno=0
				end

				if  @retorno=5 begin set @retorno=1 end
				
			end
		end
	end	
	Return @Retorno
END--FIN FUNCTION
