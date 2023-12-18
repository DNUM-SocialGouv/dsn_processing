USE [DSNP3]
GO

SELECT [dbo].[IndividuChangement].[IdIndividuChangement]
      ,[dbo].[IndividuChangement].[IdIndividu]
      ,[dbo].[IndividuChangement].[DateModification]
      ,[dbo].[IndividuChangement].[AncienNIR]
      ,[dbo].[IndividuChangement].[NomFamille]
      ,[dbo].[IndividuChangement].[Prenoms]
      ,[dbo].[IndividuChangement].[DateNaissance]
      ,[dbo].[IndividuChangement].[DateChargement]
      ,[dbo].[IndividuChangement].[IdDateChargement]
      ,[dbo].[IndividuChangement].[EstCodifie]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[IndividuChangement]
  INNER JOIN [dbo].[Individu]
	ON [dbo].[IndividuChangement].IdIndividu = [dbo].[Individu].IdIndividu
  INNER JOIN [dbo].[Etablissement]
	ON [dbo].[Individu].IdEtablissement = [dbo].[Etablissement].IdEtablissement
  INNER JOIN [dbo].[Declaration]
	ON [dbo].[Etablissement].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
