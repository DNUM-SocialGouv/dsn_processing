USE [DSNP3]
GO

SELECT [dbo].[AutreSuspensionExecutionContrat].[IdAutreSuspensionExecutionContrat]
      ,[dbo].[AutreSuspensionExecutionContrat].[IdContrat]
      ,[dbo].[AutreSuspensionExecutionContrat].[MotifSuspension]
      ,[dbo].[AutreSuspensionExecutionContrat].[DateDebutSuspension]
      ,[dbo].[AutreSuspensionExecutionContrat].[DateFinSuspension]
      ,[dbo].[AutreSuspensionExecutionContrat].[DateChargement]
      ,[dbo].[AutreSuspensionExecutionContrat].[IdDateChargement]
      ,[dbo].[AutreSuspensionExecutionContrat].[PositionDetachement]
      ,[dbo].[AutreSuspensionExecutionContrat].[NombreJourOSuspenFractio]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[AutreSuspensionExecutionContrat]
  INNER JOIN [dbo].[Contrat]
	ON [dbo].[AutreSuspensionExecutionContrat].IdContrat = [dbo].[Contrat].IdContrat
  INNER JOIN [dbo].[Individu]
	ON [dbo].[Contrat].IdIndividu = [dbo].[Individu].IdIndividu
  INNER JOIN [dbo].[Etablissement]
	ON [dbo].[Individu].IdEtablissement = [dbo].[Etablissement].IdEtablissement
  INNER JOIN [dbo].[Declaration]
	ON [dbo].[Etablissement].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
