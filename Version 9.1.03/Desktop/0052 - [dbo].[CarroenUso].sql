IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CarroenUso]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[CarroenUso]
GO

CREATE procedure [dbo].[CarroenUso]         
 @Carro VARCHAR(20),    
 @USUARIO VARCHAR(20),        
 @VALUE AS NUMERIC(1) OUTPUT                
as         
SET @VALUE = (        
 select	CASE WHEN count(picking_id) > 0 THEN 1 ELSE 0 END        
 from	picking        
 where  FECHA_INICIO IS NOT NULL   
		AND PALLET_PICKING = @CARRO  
		AND isnull(estado,'0') in ('0','1')  
		and FACTURADO='0'
 )