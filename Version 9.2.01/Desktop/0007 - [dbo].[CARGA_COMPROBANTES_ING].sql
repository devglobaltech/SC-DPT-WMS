INSERT INTO SYS_INT_TIPO_DOCUMENTO
SELECT TIPO_COMPROBANTE_ID, DESCRIPCION
FROM TIPO_COMPROBANTE
WHERE TIPO_COMPROBANTE_ID NOT IN (
SELECT TIPO_DOCUMENTO_ID FROM SYS_INT_TIPO_DOCUMENTO)