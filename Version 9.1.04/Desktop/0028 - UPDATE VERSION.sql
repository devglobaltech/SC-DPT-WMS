
UPDATE SYS_VERSION SET VER_ACTUAL_WARP = '9.14',VER_MIN_COMPATIBLE = '9.14', FECHA_INSTALACION = GETDATE();
UPDATE SYS_VERSION SET OBS_ACTUALIZACION = 'Debe actualizar la versión actual a la version 9.1.4. 
Por favor, comuniquese con el departamento de sistemas.' WHERE NOM_DLL = 'WARP.EXE';