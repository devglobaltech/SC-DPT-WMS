
/****** Object:  StoredProcedure [dbo].[Mob_IngresarViajes_Pallet]    Script Date: 02/10/2014 16:08:40 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_IngresarViajes_Pallet]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_IngresarViajes_Pallet]
GO

/****** Object:  StoredProcedure [dbo].[Mob_IngresarViajes_Pallet]    Script Date: 02/10/2014 16:08:40 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE         Procedure [dbo].[Mob_IngresarViajes_Pallet]
	@ViajeId as Varchar(100),
	@Pallet  as numeric(38)
As
Begin
	
	Declare @Total 		as Int
	Declare @Total_Fin 	as Int
	Declare @Control 	as Int
	Declare @Usr		as varchar(15)
	Declare @Terminal	as varchar(100)
	Declare @FacturaAT	as varchar(10)
	Declare @PalletCerrado	as char(1)
	Declare @Controla	as char(1)
	DECLARE @COUNT_FLG_APF NUMERIC(20,0)
	DECLARE @COUNT_CTRL_PALLET  NUMERIC(20,0)

		
	Set @Terminal	=Host_Name()
	Select @Usr=Usuario_id from #Temp_Usuario_loggin
	
	Select	distinct @PalletCerrado=isnull(pallet_cerrado,'0')
	from	picking	
	where	Viaje_ID=Ltrim(Rtrim(Upper(@ViajeId)))
			And Pallet_Picking=Ltrim(Rtrim(Upper(@Pallet)))

	set @Controla=null
	
	--El codigo comentado aca abajo no es aplicable a Papierttei, la razon es que papier puede utiliza pallets picking que puede
	--pertenecer a varios cliente_id. Esto provoca un error en el query del where.
  	/*select	@Controla=isnull(flg_control_apf,'0')
  	from	cliente_parametros
  	where	cliente_id=(select distinct cliente_id from picking(nolock) where pallet_picking=@Pallet)*/
	--El siguiente query toma en cuenta esto y se fija si de los clientes asignados al pallet_picking alguno requiere el control de armado de pallet final.
	--si es asi y el pallet_picking no esta cerrado se muestra el mensaje de advertencia, de lo contrario el proceso sigue normalmente.

	SELECT @COUNT_FLG_APF = COUNT(*)
	FROM CLIENTE_PARAMETROS
	where	cliente_id IN (select distinct cliente_id from picking(nolock) where pallet_picking=@Pallet)
	AND ISNULL(flg_control_apf,'0')='1'

	if (@COUNT_FLG_APF > 1)
    SET @CONTROLA = '1'

	if (@PalletCerrado='0') and (@Controla='1')
	begin
		raiserror('Debe terminar el Armado de pallet final antes de realizar la subida al camion.', 16,1)
		return
	end	
	Set @controla=null
		
	--El siguiente query tiene el mismo problema que el query anterior-.
	/*select	@Controla=isnull(flg_control_picking,'0')
	from	cliente_parametros 
	where	cliente_id=(select distinct cliente_id from picking(nolock) where pallet_picking=@Pallet)*/
  
	SELECT @COUNT_CTRL_PALLET = COUNT(*)
	from	cliente_parametros 
	where	cliente_id IN (select distinct cliente_id from picking(nolock) where pallet_picking=@Pallet)
        AND isnull(flg_control_picking,'0') = '1'
  
	IF (@COUNT_CTRL_PALLET>1)
	SET @CONTROLA = '1'

	if (@Controla='1')
	begin
		set @controla=null
		SELECT	@controla=isnull(pallet_controlado,0)
		From	picking p (nolock)
		Where 	P.PALLET_PICKING=@Pallet
				and pallet_picking is not null
		if @controla='0'
		begin
			raiserror('Debe realizar el control del pallet de picking antes de realizar la expedicion.', 16,1)
			return
		end
	end	

	Update	Picking	
	Set 	St_Control_Exp='1', 
			Fecha_Control_Exp=Getdate(),
			Usuario_Control_Exp=ltrim(rtrim(upper(@Usr))),
			Terminal_Control_Exp=@Terminal		
	Where	Viaje_ID=Ltrim(Rtrim(Upper(@ViajeId)))
			And Pallet_Picking=Ltrim(Rtrim(Upper(@Pallet)))

	update picking set st_control_exp=1 
	where  viaje_id=@ViajeId
			and pallet_picking in(	select 	pallet_picking
									from	picking
									where	viaje_id=@ViajeId
									group by
											pallet_picking
									having 	sum(cant_confirmada)=0
								)

	SELECT 	@TOTAL=ISNULL(COUNT(PICKING_ID),0)
	FROM 	PICKING
	WHERE 	VIAJE_ID=LTRIM(RTRIM(UPPER(@ViajeId)))

	SELECT 	@TOTAL_FIN=ISNULL(COUNT(PICKING_ID),0)
	FROM 	PICKING
	WHERE	VIAJE_ID=LTRIM(RTRIM(UPPER(@ViajeId)))
			AND FECHA_INICIO IS NOT NULL
			AND FECHA_FIN IS NOT NULL
			AND USUARIO IS NOT NULL
			AND CANT_CONFIRMADA IS NOT NULL
			AND PALLET_PICKING IS NOT NULL
			AND St_Control_Exp='1'

	If @Total= @Total_Fin
		Begin
			Update	Picking Set St_Camion = '2' 
			Where	Viaje_ID=Ltrim(Rtrim(Upper(@ViajeId)))
			
			--proceso por el cual se pretende q al momento de estar todo sobre el camion
			--salga de la frontera.
			Select	@FacturaAT=VALOR
			from	sys_parametro_proceso
			where	proceso_id='WARP'AND SUBPROCESO_ID='FACTURACION_AT' AND PARAMETRO_ID='FACTURACION'

			If @FacturaAt='1'
			Begin
				Update	Picking Set Facturado = '1' 
				Where	Viaje_ID=Ltrim(Rtrim(Upper(@ViajeId)))				
			End
		End

End

GO


