USE [DSNP3]
GO

SELECT [dbo].[LieuTravail].[IdLieuTravail]
      ,[dbo].[Declaration].[IdDeclaration]
      ,[dbo].[LieuTravail].[Siret]
      ,[dbo].[LieuTravail].[CodeAPET]
      ,[dbo].[LieuTravail].[Voie]
      ,[dbo].[LieuTravail].[CP]
      ,[dbo].[LieuTravail].[Localite]
      ,[dbo].[LieuTravail].[CodePays]
      ,[dbo].[LieuTravail].[Distribution]
      ,[dbo].[LieuTravail].[CompltConstruction]
      ,[dbo].[LieuTravail].[CompltVoie]
      ,[dbo].[LieuTravail].[CodeNatureJur]
      ,[dbo].[LieuTravail].[CodeINSEECommune]
      ,[dbo].[LieuTravail].[EnseigneEtab]
      ,[dbo].[LieuTravail].[DateChargement]
      ,[dbo].[LieuTravail].[IdDateChargement]
      ,[dbo].[Declaration].[DateDeclaration]
  FROM [dbo].[LieuTravail]
  INNER JOIN [dbo].[Declaration]
	ON [dbo].[LieuTravail].IdDeclaration = [dbo].[Declaration].IdDeclaration
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2
GO
