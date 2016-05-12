/****** Object:  StoredProcedure [dbo].[MOB_EMPAQUETADO_VERIFICA_CONTENEDORA]    Script Date: 08/18/2015 12:09:20 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[MOB_EMPAQUETADO_VERIFICA_CONTENEDORA]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[MOB_EMPAQUETADO_VERIFICA_CONTENEDORA]
GO


CREATE PROCEDURE [dbo].[MOB_EMPAQUETADO_VERIFICA_CONTENEDORA]
	@USUARIO	VARCHAR(100),
	@CONTENEDOR	VARCHAR(100),
	@EXISTE		VARCHAR(1)	OUTPUT
AS
BEGIN
	SET XACT_ABORT	ON
	DECLARE @CONT	NUMERIC;
	DECLARE @ESTADO	VARCHAR(1)
	
	SELECT	DISTINCT @ESTADO=FIN_PICKING
	FROM	PICKING
	WHERE	PALLET_PICKING =@CONTENEDOR 
	
	IF @ESTADO<>'2' begin
		RAISERROR('La ola de picking asociada al contenedor no ha finalizado el proceso de picking. No es posible empaquetar la contenedora.',16,1)
		return 
	END
	
	SELECT 	@CONT=COUNT (*)
	From	documento d (nolock)
			inner join det_documento dd (nolock) on (d.documento_id=dd.documento_id)
			inner join sucursal s (nolock) on (d.cliente_id=s.cliente_id and d.sucursal_destino=s.sucursal_id)
			inner join picking p (nolock) on (dd.documento_id=p.documento_id and dd.nro_linea=p.nro_linea)
			inner join rl_sys_cliente_usuario su on(d.cliente_id=su.cliente_id)
			inner join cliente c on(d.cliente_id=c.cliente_id)
			inner join CLIENTE_PARAMETROS cp on(c.CLIENTE_ID=cp.CLIENTE_ID)
	Where	p.fin_picking in ('2')
			and ((ISNULL(cp.FLG_DESCONSOLIDACION,'0')='0')or(isnull(p.ESTADO,'0') in('2')))
			AND ISNULL(CP.FLG_EMPAQUETADO,'0')='1'
			and P.VIAJE_ID IN (	SELECT	DISTINCT VIAJE_ID
								FROM	PICKING P
										INNER JOIN CLIENTE_PARAMETROS CP ON(P.CLIENTE_ID=CP.CLIENTE_ID)
										INNER JOIN DOCUMENTO D ON (D.DOCUMENTO_ID = P.DOCUMENTO_ID)
								WHERE	P.NRO_UCEMPAQUETADO IS NULL
										AND P.FIN_PICKING = '2'
										AND P.FACTURADO = '0'
										AND P.CANT_CONFIRMADA <> 0
										AND CP.FLG_EMPAQUETADO = '1'
										AND D.STATUS <> 'D40')
			and p.FACTURADO='0'
			and su.usuario_id=@USUARIO
			and p.PALLET_PICKING=@CONTENEDOR
			and p.CANT_CONFIRMADA>0
			AND D.STATUS <> 'D40'

	IF @CONT>0 BEGIN
		SET @EXISTE='1'
	END	ELSE BEGIN
		SET @EXISTE='0'
	END
	
END
GO


