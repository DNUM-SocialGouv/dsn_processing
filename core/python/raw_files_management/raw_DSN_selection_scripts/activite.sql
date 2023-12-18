USE [DSNP3]
GO

SELECT [dbo].[Activite].[IdActivite]
      ,[dbo].[Activite].[IdRemuneration]
      ,[dbo].[Activite].[TypeActivite]
      ,[dbo].[Activite].[Mesure]
      ,[dbo].[Activite].[UniteMesure]
      ,[dbo].[Activite].[DateChargement]
      ,[dbo].[Activite].[IdDateChargement]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[Activite]
  INNER JOIN [dbo].[Remuneration]
	ON [dbo].[Activite].IdRemuneration = [dbo].[Remuneration].IdRemuneration
  INNER JOIN [dbo].[Versement]
	ON [dbo].[Remuneration].IdVersement = [dbo].[Versement].IdVersement
  INNER JOIN [dbo].[Individu]
	ON [dbo].[Versement].IdIndividu = [dbo].[Individu].IdIndividu
  INNER JOIN [dbo].[Etablissement]
	ON [dbo].[Individu].IdEtablissement = [dbo].[Etablissement].IdEtablissement
  INNER JOIN [dbo].[Declaration]
	ON [dbo].[Etablissement].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
