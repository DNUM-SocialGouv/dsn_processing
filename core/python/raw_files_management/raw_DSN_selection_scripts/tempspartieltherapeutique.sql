USE [DSNP3]
GO

SELECT [dbo].[TempsPartielTherapeutique].[IdTempsPartielTherapeutique]
      ,[dbo].[TempsPartielTherapeutique].[IdArretDeTravail]
      ,[dbo].[TempsPartielTherapeutique].[DateDebut]
      ,[dbo].[TempsPartielTherapeutique].[DateFin]
      ,[dbo].[TempsPartielTherapeutique].[Montant]
      ,[dbo].[TempsPartielTherapeutique].[DateChargement]
      ,[dbo].[TempsPartielTherapeutique].[IdDateChargement]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[TempsPartielTherapeutique]
  INNER JOIN [dbo].[ArretDeTravail]
	ON [dbo].[TempsPartielTherapeutique].IdArretDeTravail = [dbo].[ArretDeTravail].IdArretDeTravail
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
