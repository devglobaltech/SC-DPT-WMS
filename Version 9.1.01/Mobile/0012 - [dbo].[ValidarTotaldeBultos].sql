/****** Object:  StoredProcedure [dbo].[ValidarTotaldeBultos]    Script Date: 10/08/2013 10:59:29 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ValidarTotaldeBultos]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ValidarTotaldeBultos]
GO

CREATE procedure [dbo].[ValidarTotaldeBultos]
@GUIA VARCHAR(20),
@VALUE AS NUMERIC(1) OUTPUT              
as

set @value = (
select case when count(uc_empaque) > 0 then 0 else 1 end
from UC_EMPAQUE E
left JOIN TMPBULTO_DOCK T
on e.uc_empaque = t.bulto
WHERE NRO_GUIA = @GUIA
and bulto is null)


GO


