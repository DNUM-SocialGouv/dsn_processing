USE [DSNP3]
GO

SELECT [dbo].[Versement].[IdVersement]
      ,[dbo].[Versement].[IdIndividu]
      ,[dbo].[Versement].[DateVersement]
      ,[dbo].[Versement].[RemunarationNetteFiscale]
      ,[dbo].[Versement].[NumeroVersement]
      ,[dbo].[Versement].[MontantNetVerse]
      ,[dbo].[Versement].[TauxPrelevSource]
      ,[dbo].[Versement].[TypeTauxPrelevSource]
      ,[dbo].[Versement].[IdenTauxPrelevSource]
      ,[dbo].[Versement].[MontantPrelevSource]
      ,[dbo].[Versement].[MontantPartNonImpRevenu]
      ,[dbo].[Versement].[MontantAbattBaseFiscale]
      ,[dbo].[Versement].[MontantDiffAssPASEtRNF]
      ,[dbo].[Versement].[DateChargement]
      ,[dbo].[Versement].[IdDateChargement]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]	
  FROM [dbo].[Versement]
  INNER JOIN [dbo].[Individu]
	ON [dbo].[Versement].IdIndividu = [dbo].[Individu].IdIndividu
  INNER JOIN [dbo].[Etablissement]
	ON [dbo].[Individu].IdEtablissement = [dbo].[Etablissement].IdEtablissement
  INNER JOIN [dbo].[Declaration]
	ON [dbo].[Etablissement].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
