UPDATE SYS_VERSION SET	VER_ACTUAL_WARP='9.53',
						VER_MIN_COMPATIBLE='9.53',
						FECHA_INSTALACION=GETDATE()
WHERE NOM_DLL<>'WARP.EXE'						

UPDATE SYS_VERSION SET	VER_ACTUAL_WARP='9.53',
						VER_MIN_COMPATIBLE='9.53',
						FECHA_INSTALACION=GETDATE(),
						OBS_ACTUALIZACION='Debe actualizar la versi�n actual a la version 9.5.3. Por favor, comuniquese con el departamento de sistemas.'
WHERE NOM_DLL='WARP.EXE'
