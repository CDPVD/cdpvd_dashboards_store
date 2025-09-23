
select
    fiche, 
    IIF(ISNULL(adr.APP,'') <> '', adr.App + '-', '') +
    IIF(ISNULL(adr.NoCiv,'') <> '', adr.NoCiv + ' ', ' ') +
    IIF(ISNULL(adr.OrientRue,'') <> '', adr.OrientRue + ' ', ' ') +
    IIF(ISNULL(adr.GenreRue,'') <> '', adr.GenreRue + ' ', ' ') +
    IIF(ISNULL(adr.Rue,'') <> '', adr.Rue + ', ', ' ') +
    IIF(ISNULL(adr.Ville,'') <> '', adr.Ville + ', ', ' ') +
    IIF(ISNULL(adr.CodePost,'') <> '', adr.CodePost, ' ') as adresse

from {{ var("database_jade") }}.dbo.E_Adr adr 
where TypeAdr in ('4') and DateEffect = (select max(DateEffect) from {{ var("database_jade") }}.dbo.E_Adr where fiche = adr.Fiche and TypeAdr in ('4'))