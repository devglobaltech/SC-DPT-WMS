
ALTER  Procedure [dbo].[Migra_Interfaces]
As
Begin
	Set xact_abort On
	Set Nocount On

	--=================================================================================================
	--Paso 1, muevo la Sys_int...
	--=================================================================================================

	select * into #cabeceraSI from Sys_int_Documento s  where DBO.VerificaDocExt(s.Cliente_Id, s.Doc_Ext)=1 and  s.fecha_estado_gt < DATEADD(DD,-120,GETDATE())

	Select * into #DetalleSI from Sys_Int_Det_Documento s Where exists (Select 1 from #cabeceraSI c where s.Cliente_Id=c.Cliente_ID and s.Doc_Ext=c.Doc_Ext)

	Begin Transaction

	--Guardo las cabeceras.
	Insert into Sys_Int_Documento_Historico
	Select * from #CabeceraSI A Where not exists(select 1 from SYS_INT_DOCUMENTO_HISTORICO B where a.cliente_id=b.cliente_id and a.doc_ext=B.DOC_EXT )

	--Guardo los Detalles
	Insert into Sys_Int_Det_Documento_Historico
	Select * from #DetalleSI

	--Borro Detalles
	Delete from Sys_Int_Det_Documento 
	Where Exists (Select 1 From #DetalleSI d  where Sys_Int_Det_Documento.Cliente_id=d.Cliente_Id and Sys_Int_Det_Documento.Doc_Ext=d.Doc_Ext)
	
	--Borro Cabeceras
	Delete from Sys_Int_Documento 
	Where Exists (Select 1 From #DetalleSI d  where Sys_Int_Documento.Cliente_id=d.Cliente_Id and Sys_Int_Documento.Doc_Ext=d.Doc_Ext)
	
	Drop Table #DetalleSI
	Drop Table #CabeceraSI

	Commit Transaction

	--=================================================================================================
	--Paso 2, muevo la Sys_Dev...
	--=================================================================================================
	select 	* into #cabeceraSD 
	from 	Sys_dev_Documento SD
	Where 	1=1--flg_movimiento='1' 
			and Fecha_Estado_Gt< DateAdd(DD,-120,Getdate()) 
			AND NOT EXISTS (SELECT 1 FROM SYS_INT_DOCUMENTO S WHERE SD.CLIENTE_ID =S.CLIENTE_ID AND SD.DOC_EXT=S.DOC_EXT)			

	select * into #DetalleSD 	from Sys_Dev_Det_Documento s 	where exists (Select 1 from #cabeceraSD c where s.Cliente_Id=c.Cliente_ID and s.Doc_Ext=c.Doc_Ext) --and  s.flg_movimiento='1' 

	begin transaction
	
	--Guardo Cabeceras
	Insert Into Sys_Dev_Documento_Historico
	Select * from #cabeceraSD

	--Guardo Detalles
	Insert Into Sys_Dev_Det_Documento_Historico						
	Select * from #detalleSD

	Delete from Sys_dev_Det_Documento 
	Where Exists (Select 1 From #DetalleSD d  where Sys_dev_Det_Documento.Cliente_id=d.Cliente_Id and Sys_dev_Det_Documento.Doc_Ext=d.Doc_Ext)
	
	--Borro Cabeceras
	Delete from Sys_Dev_Documento 
	Where Exists (Select 1 From #DetalleSD d  where Sys_Dev_Documento.Cliente_id=d.Cliente_Id and Sys_Dev_Documento.Doc_Ext=d.Doc_Ext)
	

	Commit Transaction

end