    select
    ele.CodePerm, 
    ele.fiche,
    ele.Nom, ele.Pnom,
    ele.NomPere, ele.PnomPere,
    ele.NomMere, ele.PnomMere,
    ele.NomTuteur, ele.PnomTuteur,
    ele.ADR_ELECTR_ELE,
    ele.ADR_ELECTR_PERE, 
    ele.ADR_ELECTR_MERE,
    ele.ADR_ELECTR_TUTEUR,
    peres.adresse as adr_pere,
    meres.adresse as adr_mere,
    tuteurs.adresse as adr_tuteur,
    iif(eleves.adresse is not null, eleves.adresse, iif(elevesPost.adresse is not null, 'AP: '+elevesPost.adresse, '')) as adr_eleve


from {{ var("database_jade") }}.dbo.E_Ele ele
    LEFT JOIN {{ ref("i_gpm_e_ele_adr_peres") }} peres on ele.fiche = peres.fiche
    LEFT JOIN {{ ref("i_gpm_e_ele_adr_meres") }} meres on ele.fiche = meres.fiche
    LEFT JOIN {{ ref("i_gpm_e_ele_adr_tuteurs") }} tuteurs on ele.fiche = tuteurs.fiche
    LEFT JOIN {{ ref("i_jade_e_ele_adr_eleves") }} eleves on ele.fiche = eleves.fiche
    LEFT JOIN {{ ref("i_jade_e_ele_adrpost_eleves") }} elevesPost on ele.fiche = elevesPost.fiche