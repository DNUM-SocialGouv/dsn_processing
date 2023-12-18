USE [DSNP3]
GO

SELECT [dbo].[Anciennete].[IdAnciennete]
      ,[dbo].[Anciennete].[IdIndividu]
      ,[dbo].[Anciennete].[TypeAnciennete]
      ,[dbo].[Anciennete].[UniteMesure]
      ,[dbo].[Anciennete].[Valeur]
      ,[dbo].[Anciennete].[NumeroContrat]
      ,[dbo].[Anciennete].[DateChargement]
      ,[dbo].[Anciennete].[IdDateChargement]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[Anciennete]
  INNER JOIN [dbo].[Individu]
	ON [dbo].[Anciennete].IdIndividu = [dbo].[Individu].IdIndividu
  INNER JOIN [dbo].[Etablissement]
	ON [dbo].[Individu].IdEtablissement = [dbo].[Etablissement].IdEtablissement
  INNER JOIN [dbo].[Declaration]
	ON [dbo].[Etablissement].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
