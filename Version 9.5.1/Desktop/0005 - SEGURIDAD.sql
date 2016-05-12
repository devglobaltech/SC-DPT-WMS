
CREATE TABLE [sec_users] (
    [login] VARCHAR(32) NOT NULL,
    [pswd] VARCHAR(32) NOT NULL,
    [name] VARCHAR(64) NULL,
    [email] VARCHAR(64) NULL,
    [active] VARCHAR(1) NULL,
    [activation_code] VARCHAR(32) NULL,
    [priv_admin] VARCHAR(1) NULL,
    PRIMARY KEY ([login]),
    UNIQUE ([email])
)


CREATE TABLE [sec_apps] (
    [app_name] VARCHAR(128) NOT NULL,
    [app_type] VARCHAR(255) NULL,
    [description] VARCHAR(255) NULL,
    PRIMARY KEY ([app_name])
)


CREATE TABLE [sec_groups] (
    [group_id] INTEGER NOT NULL,
    [description] VARCHAR(64) NULL,
    PRIMARY KEY ([group_id]),
    UNIQUE ([description])
)


CREATE TABLE [sec_users_groups] (
    [login] VARCHAR(32) NOT NULL,
    [group_id] INTEGER NOT NULL,
    PRIMARY KEY ([login], [group_id])
)


CREATE TABLE [sec_groups_apps] (
    [group_id] INTEGER NOT NULL,
    [app_name] VARCHAR(128) NOT NULL,
    [priv_access] VARCHAR(1) NULL,
    [priv_insert] VARCHAR(1) NULL,
    [priv_delete] VARCHAR(1) NULL,
    [priv_update] VARCHAR(1) NULL,
    [priv_export] VARCHAR(1) NULL,
    [priv_print] VARCHAR(1) NULL,
    PRIMARY KEY ([group_id], [app_name])
)


CREATE TABLE [sec_users_cliente] (
	[sec_users_cliente_id]			int  IDENTITY(1,1),
    [login]							VARCHAR(32) NOT NULL,
	[cliente_id]					VARCHAR(15) NOT NULL,
	PRIMARY KEY ([sec_users_cliente_id])
)

