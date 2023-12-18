USE [DSNP3]
GO

SELECT [IdDeclaration]
      ,[IdEnvoi]
      ,[NatureDeclaration]
      ,[TypeDeclaration]
      ,[NumeroFraction]
      ,[NumeroOrdreDeclaration]
      ,[DateDeclaration]
      ,[DateFichier]
      ,[ChampDeclaration]
      ,[IdentifiantMetier]
      ,[DeviseDeclaration]
      ,[SiretEtab]
      ,[DateChargement]
      ,[IdDateChargement]
  FROM [dbo].[Declaration]
  WHERE [dbo].[Declaration].[IdDateChargement] >= AAAAMMJJ1
  AND [dbo].[Declaration].[IdDateChargement] < AAAAMMJJ2

GO
