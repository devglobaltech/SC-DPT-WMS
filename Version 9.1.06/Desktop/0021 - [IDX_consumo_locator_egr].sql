
/****** Object:  Index [IDX_consumo_locator_egr]    Script Date: 04/03/2014 15:22:44 ******/
IF  EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[consumo_locator_egr]') AND name = N'IDX_consumo_locator_egr')
DROP INDEX [IDX_consumo_locator_egr] ON [dbo].[consumo_locator_egr] WITH ( ONLINE = OFF )
GO

/****** Object:  Index [IDX_consumo_locator_egr]    Script Date: 04/03/2014 15:22:45 ******/
CREATE NONCLUSTERED INDEX [IDX_consumo_locator_egr] ON [dbo].[consumo_locator_egr] 
(
	[rl_id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO


