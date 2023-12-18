USE [DSNP3]
GO

SELECT [dbo].[Remuneration].[IdRemuneration]
      ,[dbo].[Remuneration].[IdVersement]
      ,[dbo].[Remuneration].[DateDebutPeriodePaie]
      ,[dbo].[Remuneration].[DateFinPeriodePaie]
      ,[dbo].[Remuneration].[NumeroContrat]
      ,[dbo].[Remuneration].[TypeRemuneration]
      ,[dbo].[Remuneration].[NombreHeure]
      ,[dbo].[Remuneration].[Montant]
      ,[dbo].[Remuneration].[TauxRemunPositStatutaire]
      ,[dbo].[Remuneration].[TauxMajorResidentielle]
      ,[dbo].[Remuneration].[DateChargement]
      ,[dbo].[Remuneration].[IdDateChargement]
      ,[dbo].[Remuneration].[TauxRemuCotisee]
      ,[dbo].[Remuneration].[TauxMajorationExAExE]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[Remuneration]
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
