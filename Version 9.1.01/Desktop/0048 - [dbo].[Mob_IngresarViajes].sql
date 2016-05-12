/****** Object:  StoredProcedure [dbo].[Mob_IngresarViajes]    Script Date: 09/23/2013 11:28:12 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Mob_IngresarViajes]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Mob_IngresarViajes]
GO

CREATE                  PROCEDURE [dbo].[Mob_IngresarViajes]
@Codigo as nvarchar(100)

AS	
	DECLARE @CONTROLADO AS INT
	DECLARE @FINALIZADO AS INT
	DECLARE @CONTROL 	AS INT
	DECLARE @RC			AS INT
	DECLARE @EXISTE_V	AS INT
	DECLARE @QTY		AS INT
	DECLARE @ENV		AS INT
	DECLARE @Controla	AS CHAR(1)
	DECLARE @ContDesc	as char(1)

	SELECT 	@EXISTE_V=COUNT(PICKING_ID)
	FROM	PICKING
	WHERE	VIAJE_ID=LTRIM(RTRIM(UPPER(@CODIGO)))
	
	Select	@Controla=isnull(c.flg_control_exp,'0'),@ContDesc=ISNULL(c.FLG_DESCONSOLIDACION,'0')
	from	picking p inner join cliente_parametros c
			on(p.cliente_id=c.cliente_id)
	where	viaje_id=ltrim(rtrim(upper(@CODIGO)))
	
	if @ContDesc='1'
	begin
		Select @Qty=COUNT(ISNULL(estado,'0')) from picking where viaje_id=ltrim(rtrim(upper(@Codigo)))and ISNULL(ESTADO,'0')<>'2'
		
		if (@Qty>0 and @ContDesc='1') begin
			RAISERROR('Es obligatorio realizar la desconsolidacion del picking.',16,1)
			RETURN 	
		end --if		
	End
	
	IF @EXISTE_V > 0
		BEGIN
			SELECT @FINALIZADO=DBO.STATUS_PICKING(@CODIGO)
		END
	ELSE
		BEGIN
			RAISERROR('El viaje no existe',16,1)
			RETURN
		END
	
	IF @FINALIZADO =2 
		BEGIN
			-- Agregado para control de Carga.
			SELECT 	@QTY=COUNT(DD.DOC_EXT)
			FROM 	SYS_INT_DET_DOCUMENTO DD
					INNER JOIN SYS_INT_DOCUMENTO D ON (DD.CLIENTE_ID=D.CLIENTE_ID AND DD.DOC_EXT=D.DOC_EXT)
					INNER JOIN PRODUCTO PROD ON (DD.CLIENTE_ID=PROD.CLIENTE_ID AND DD.PRODUCTO_ID=PROD.PRODUCTO_ID)
			WHERE 	DD.ESTADO_GT IS NULL AND D.CODIGO_VIAJE=LTRIM(RTRIM(UPPER(@Codigo)))

			IF (@QTY>0) BEGIN
				RAISERROR('EL PICKING/VIAJE AUN TIENE PRODUCTOS PENDIENTES POR PROCESAR!',16,1)
				RETURN
			END 
			
			--Aca hago un Update para que no levante los pallet
			--que, sumado el total, den igual a 0
			update picking set st_control_exp=1 
			where  viaje_id=ltrim(rtrim(upper(@CODIGO)))
					and pallet_picking in(	select 	pallet_picking
											from	picking
											where	viaje_id=ltrim(rtrim(upper(@CODIGO)))
											group by
													pallet_picking
											having 	sum(cant_confirmada)=0
										)


			SELECT  DISTINCT   
					PALLET_PICKING as NRO_PALLET, VIAJE_ID as NRO_VIAJE,
					ST_CONTROL_EXP AS ST_CONTROL_EXP
			FROM    PICKING
			WHERE 	VIAJE_ID =LTRIM(RTRIM(UPPER(@Codigo)))
					AND FECHA_INICIO IS NOT NULL
					AND FECHA_FIN IS NOT NULL
					AND USUARIO IS NOT NULL
					AND CANT_CONFIRMADA IS NOT NULL
					AND PALLET_PICKING IS NOT NULL
					AND ISNULL(ST_CONTROL_EXP,'0')='0'
					AND ((@Controla='0') OR (FACTURADO=0))

			IF @@ROWCOUNT =0 
			BEGIN
				SELECT @ENV=COUNT(*) FROM RL_ENV_DOCUMENTO_VIAJE WHERE VIAJE_ID=Ltrim(Rtrim(Upper(@Codigo)))
				IF @ENV=1
				BEGIN
					RAISERROR('El viaje ya fue controlado',16,1)				
				END
				ELSE
				BEGIN

					SELECT 1 AS EXISTE
				END

			END
		END
		ELSE
		BEGIN
			RAISERROR('El viaje se encuentra en proceso de Picking',16,1)
			RETURN
		END

GO


