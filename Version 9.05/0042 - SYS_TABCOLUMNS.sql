DELETE FROM sys_tabColumns WHERE TABLE_NAME='PRODUCTO';
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'CLIENTE_ID', '1', 'VARCHAR2', 'N', '15', NULL, NULL, '15', 'Y');
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'PRODUCTO_ID', '2', 'VARCHAR2', 'N', '30', NULL, NULL, '30', 'Y');
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'CODIGO_PRODUCTO', '3', 'VARCHAR2', 'Y', '50', NULL, NULL, '50', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'SUBCODIGO_1', '4', 'VARCHAR2', 'Y', '50', NULL, NULL, '50', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'SUBCODIGO_2', '5', 'VARCHAR2', 'Y', '50', NULL, NULL, '50', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'DESCRIPCION', '6', 'VARCHAR2', 'N', '200', NULL, NULL, '200', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'NOMBRE', '7', 'VARCHAR2', 'Y', '50', NULL, NULL, '50', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'MARCA', '8', 'VARCHAR2', 'Y', '60', NULL, NULL, '60', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'FRACCIONABLE', '9', 'VARCHAR2', 'Y', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'UNIDAD_FRACCION', '10', 'VARCHAR2', 'Y', '5', NULL, NULL, '5', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'COSTO', '11', 'NUMBER', 'N', '13', '10', '3', '0', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'UNIDAD_ID', '12', 'VARCHAR2', 'N', '5', NULL, NULL, '5', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'TIPO_PRODUCTO_ID', '13', 'VARCHAR2', 'Y', '5', NULL, NULL, '5', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'PAIS_ID', '14', 'VARCHAR2', 'N', '5', NULL, NULL, '5', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'FAMILIA_ID', '15', 'VARCHAR2', 'N', '30', NULL, NULL, '30', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'CRITERIO_ID', '16', 'VARCHAR2', 'Y', '5', NULL, NULL, '5', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'OBSERVACIONES', '17', 'VARCHAR2', 'Y', '400', NULL, NULL, '400', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'POSICIONES_PURAS', '18', 'VARCHAR2', 'Y', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'KIT', '19', 'VARCHAR2', 'Y', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'SERIE_EGR', '20', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'MONEDA_ID', '21', 'VARCHAR2', 'Y', '20', NULL, NULL, '20', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'NO_AGRUPA_ITEMS', '22', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'LARGO', '23', 'NUMBER', 'Y', '13', '10', '3', '0', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'ALTO', '24', 'NUMBER', 'Y', '13', '10', '3', '0', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'ANCHO', '25', 'NUMBER', 'Y', '13', '10', '3', '0', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'UNIDAD_VOLUMEN', '26', 'VARCHAR2', 'Y', '5', NULL, NULL, '5', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'VOLUMEN_UNITARIO', '27', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'PESO', '28', 'NUMBER', 'Y', '13', '20', '5', '0', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'UNIDAD_PESO', '29', 'VARCHAR2', 'Y', '5', NULL, NULL, '5', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'PESO_UNITARIO', '30', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'LOTE_AUTOMATICO', '31', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'PALLET_AUTOMATICO', '32', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'INGRESO', '33', 'VARCHAR2', 'Y', '15', NULL, NULL, '15', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'EGRESO', '34', 'VARCHAR2', 'Y', '15', NULL, NULL, '15', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'INVENTARIO', '35', 'VARCHAR2', 'Y', '15', NULL, NULL, '15', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'TRANSFERENCIA', '36', 'VARCHAR2', 'Y', '15', NULL, NULL, '15', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'TOLERANCIA_MIN', '37', 'NUMBER', 'Y', '13', '6', '2', '0', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'TOLERANCIA_MAX', '38', 'NUMBER', 'Y', '13', '6', '2', '0', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'BACK_ORDER', '39', 'VARCHAR2', 'Y', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'CLASIFICACION_COT', '40', 'VARCHAR2', 'Y', '100', NULL, NULL, '100', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'CODIGO_BARRA', '41', 'VARCHAR2', 'Y', '100', NULL, NULL, '100', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'ING_CAT_LOG_ID', '42', 'VARCHAR2', 'Y', '50', NULL, NULL, '50', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'EGR_CAT_LOG_ID', '43', 'VARCHAR2', 'Y', '50', NULL, NULL, '50', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'SUB_FAMILIA_ID', '44', 'VARCHAR2', 'N', '30', NULL, NULL, '30', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'TIPO_CONTENEDORA', '45', 'VARCHAR2', 'N', '100', NULL, NULL, '100', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'GRUPO_PRODUCTO', '46', 'VARCHAR2', 'N', '30', NULL, NULL, '5', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'ENVASE', '47', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'VAL_COD_ING', '48', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'VAL_COD_EGR', '49', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'ROTACION_ID', '50', 'VARCHAR2', 'Y', '20', NULL, NULL, '20', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'FLG_BULTO', '51', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'QTY_BULTO', '52', 'NUMBER', 'Y', '20', '0', '0', '0', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'FLG_VOLUMEN_ETI', '53', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'QTY_VOLUMEN_ETI', '54', 'NUMBER', 'Y', '22', '20', '5', '0', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'FLG_CONTENEDORA', '55', 'VARCHAR2', 'Y', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'SERIE_ING', '56', 'VARCHAR2', 'Y', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'TIE_IN', '57', 'VARCHAR2', 'Y', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'INGLOTEPROVEEDOR', '58', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL);
INSERT INTO SYS_TABCOLUMNS VALUES ('PRODUCTO', 'INGPARTIDA', '59', 'VARCHAR2', 'N', '1', NULL, NULL, '1', NULL)