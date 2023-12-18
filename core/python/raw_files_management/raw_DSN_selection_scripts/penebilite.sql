USE [DSNP3]
GO

SELECT [dbo].[Penebilite].[IdPenebilite]
      ,[dbo].[Penebilite].[IdIndividu]
      ,[dbo].[Penebilite].[FacteurExposition]
      ,[dbo].[Penebilite].[NumeroContrat]
      ,[dbo].[Penebilite].[AnneeRattachement]
      ,[dbo].[Penebilite].[DateChargement]
      ,[dbo].[Penebilite].[IdDateChargement]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[Penebilite]
  INNER JOIN [dbo].[Individu]
	ON [dbo].[Penebilite].IdIndividu = [dbo].[Individu].IdIndividu
  INNER JOIN [dbo].[Etablissement]
	ON [dbo].[Individu].IdEtablissement = [dbo].[Etablissement].IdEtablissement
  INNER JOIN [dbo].[Declaration]
	ON [dbo].[Etablissement].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
