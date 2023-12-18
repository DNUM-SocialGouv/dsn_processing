USE [DSNP3]
GO

SELECT [dbo].[ArretDeTravail].[IdArretDeTravail]
      ,[dbo].[ArretDeTravail].[IdContrat]
      ,[dbo].[ArretDeTravail].[MotifArret]
      ,[dbo].[ArretDeTravail].[DateDernierJourTravaille]
      ,[dbo].[ArretDeTravail].[DateFinPrevisionelle]
      ,[dbo].[ArretDeTravail].[DateReprise]
      ,[dbo].[ArretDeTravail].[MotifReprise]
      ,[dbo].[ArretDeTravail].[DateChargement]
      ,[dbo].[ArretDeTravail].[IdDateChargement]
      ,[dbo].[ArretDeTravail].[Subrogation]
      ,[dbo].[ArretDeTravail].[DateDebutSubrogation]
      ,[dbo].[ArretDeTravail].[DateFinSubrogation]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[ArretDeTravail]
  INNER JOIN [dbo].[Contrat]
	ON [dbo].[ArretDeTravail].IdContrat = [dbo].[Contrat].IdContrat
  INNER JOIN [dbo].[Individu]
	ON [dbo].[Contrat].IdIndividu = [dbo].[Individu].IdIndividu
  INNER JOIN [dbo].[Etablissement]
	ON [dbo].[Individu].IdEtablissement = [dbo].[Etablissement].IdEtablissement
  INNER JOIN [dbo].[Declaration]
	ON [dbo].[Etablissement].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
