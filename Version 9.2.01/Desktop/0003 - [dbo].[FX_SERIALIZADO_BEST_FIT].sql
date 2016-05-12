/****** Object:  UserDefinedFunction [dbo].[FX_SERIALIZADO_BEST_FIT]    Script Date: 07/03/2014 12:10:54 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[FX_SERIALIZADO_BEST_FIT]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [dbo].[FX_SERIALIZADO_BEST_FIT]
GO

CREATE       FUNCTION [dbo].[FX_SERIALIZADO_BEST_FIT](
	@POSICION_COD		VARCHAR(45),
	@PALLET				VARCHAR(50)) Returns Int
AS
BEGIN
	DECLARE  @ControlRl		Float
	DECLARE  @ControlPick	Float
	DECLARE  @CONT			BigInt
	DECLARE  @Retorno		Int
	DECLARE	 @NAV			SMALLINT
	DECLARE  @POS			SMALLINT
	DECLARE	 @BEST_FIT		CHAR(1)
	--N
	SELECT	@POS=COUNT(*) FROM POSICION (NOLOCK) WHERE POSICION_COD=@POSICION_COD
	SELECT	@NAV=COUNT(*) FROM NAVE (NOLOCK) WHERE NAVE_COD=@POSICION_COD

	Set @Retorno=5
	--------------------------------------------------------------------------------------------------------------------
	--Uno tengo que determinar si es lo unico en rl
	--------------------------------------------------------------------------------------------------------------------
	IF @POS=1 
	BEGIN
		
		SELECT @BEST_FIT=ISNULL(BESTFIT,'0') FROM POSICION WHERE POSICION_COD=@POSICION_COD
	
		Select 	@ControlRl=Count(*)
		from	Rl_det_doc_trans_posicion rl (NOLOCK) inner join Posicion P (NOLOCK) 
				on(Rl.POSICION_ACTUAL=P.Posicion_ID)
				inner join det_documento_transaccion ddt (NOLOCK)
				on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (nolock)
				on(ddt.documento_id=dd.documento_id and ddt.nro_linea_doc=dd.nro_linea)
				inner join producto pr
				on(dd.cliente_id=pr.cliente_id and dd.producto_id=pr.producto_id)
		Where	P.Posicion_Cod=@Posicion_Cod
				and isnull(p.bestfit,'0')='1'
				and isnull(pr.serie_ing,'0')='1'
				
		if (@ControlRl=0 AND @BEST_FIT='1')begin
			select	@controlrl=count(distinct viaje_id)
			from	picking
			where	prop1=@PALLET
			
		end
				
	END
	ELSE
	BEGIN
		--N
		SELECT	@ControlRl=count(rl.rl_id)
		FROM 	rl_det_doc_trans_posicion rl(NOLOCK)
				inner join det_documento_transaccion ddt (NOLOCK)
				on(rl.doc_trans_id=ddt.doc_trans_id and rl.nro_linea_trans=ddt.nro_linea_trans)
				inner join det_documento dd (NOLOCK)
				ON (ddt.documento_id=dd.documento_id AND ddt.nro_linea_doc=dd.nro_linea)
				inner join nave n 
				on(rl.nave_actual=n.nave_id)
		Where	n.nave_COD=@Posicion_Cod
				and dd.prop1=@PALLET	
				and rl.disponible='1'	
	END
	
	if @ControlRl>0 begin
		Set @Retorno=1
	end
	
	Return @Retorno
END--FIN FUNCTION




GO


