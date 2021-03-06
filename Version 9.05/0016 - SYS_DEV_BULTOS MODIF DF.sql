/****** Object:  StoredProcedure [dbo].[SYS_DEV_BULTOS]    Script Date: 02/08/2013 15:42:27 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER          PROCEDURE [dbo].[SYS_DEV_BULTOS]
@documento_id AS NUMERIC(20,0) output,
@estado	as numeric(2,0) output
AS
DECLARE @doc_Ext AS varchar(100)
DECLARE @td AS varchar(20)
DECLARE @qty AS numeric(3,0)
DECLARE @nro_lin AS numeric(20,0)
DECLARE @tc AS varchar(15)
DECLARE @status AS varchar(5)
DECLARE @Cur as cursor
BEGIN
	

	SET @CUR = CURSOR FOR
		select	distinct
				prop2
				,tipo_comprobante_id
				,status 
		from	documento d inner join det_documento dd
				on(d.documento_id=dd.documento_id)
		where	d.documento_id=@documento_id
	OPEN @CUR 
	FETCH NEXT FROM @CUR INTO @doc_ext, @tc,@status
	While @@Fetch_Status=0
	begin
		select	@qty=count(*) 
		from	sys_dev_documento 
		where	doc_ext=@doc_ext

		select	@td=tipo_documento_id 
		from	sys_int_documento 
		where	doc_ext=@doc_ext

		select	@nro_lin=max(nro_linea) 
		from	sys_dev_det_documento 
		where doc_ext=@doc_ext
		
		IF (@doc_ext <> '' and @doc_ext is not null and @status='D40')
		BEGIN
			
			IF (@td='I01' and @estado=1 and @tc='DO')
			BEGIN
				 exec SYS_DEV_I01_BULTOS
				 @doc_ext=@doc_ext
				,@estado=1 
				,@documento_id=@documento_id
			END --IF

			IF (@td='I01' and @estado=3 and @tc='DO')
			BEGIN
	     			 exec sys_dev_I03
				 @doc_ext=@doc_ext
				,@estado=1 
						,@documento_id=@documento_id
			END --IF

			IF (@td='I04' and @estado=1 and @tc='PP')
			BEGIN
	     			 exec sys_dev_I04
				 @doc_ext=@doc_ext
				,@estado=1 
						,@documento_id=@documento_id
			END --IF

			IF (@td is null and @estado=1 and @tc='DE')
			BEGIN
	     			exec    sys_dev_I08
							 @doc_ext=@doc_ext
							,@estado=@estado 
							,@documento_id=@documento_id
					break;
			END --IF


			IF (@td is null and @estado=1 and @tc='IM')
			BEGIN
	     			 exec sys_dev_I07
				 @doc_ext=@doc_ext
				,@estado=@estado 
						,@documento_id=@documento_id
			END --IF

		END --IF
		FETCH NEXT FROM @CUR INTO @doc_ext, @tc,@status	
	End
	close @cur
	deallocate @cur
END --PROCEDURE

