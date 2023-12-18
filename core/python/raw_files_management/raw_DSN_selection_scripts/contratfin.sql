USE [DSNP3]
GO

SELECT [dbo].[ContratFin].[IdContratFin]
      ,[dbo].[ContratFin].[IdContrat]
      ,[dbo].[ContratFin].[DateFin]
      ,[dbo].[ContratFin].[CodeMotifRupture]
      ,[dbo].[ContratFin].[DernierJourTrav]
      ,[dbo].[ContratFin].[DeclarationFinContratUsage]
      ,[dbo].[ContratFin].[SoldeCongesAcqNonPris]
      ,[dbo].[ContratFin].[DateChargement]
      ,[dbo].[ContratFin].[IdDateChargement]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[ContratFin]
  INNER JOIN [dbo].[Contrat]
	ON [dbo].[ContratFin].IdContrat = [dbo].[Contrat].IdContrat
  INNER JOIN [dbo].[Individu]
	ON [dbo].[Contrat].IdIndividu = [dbo].[Individu].IdIndividu
  INNER JOIN [dbo].[Etablissement]
	ON [dbo].[Individu].IdEtablissement = [dbo].[Etablissement].IdEtablissement
  INNER JOIN [dbo].[Declaration]
	ON [dbo].[Etablissement].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
