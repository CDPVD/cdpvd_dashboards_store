select
    ele.code_perm, 
    ele.fiche,
    ele.nom_pere, ele.pnom_pere,
    ele.nom_mere, ele.pnom_mere,
    ele.nom_tuteur, ele.pnom_tuteur,
    ele.adr_electr_pere, 
    ele.adr_electr_mere,
    ele.adr_electr_tuteur,

    peres.adresse as adr_pere,
    meres.adresse as adr_mere,
    tuteurs.adresse as adr_tuteur

from {{ var("database_gpi") }}.dbo.GPM_E_ELE ele
    LEFT JOIN {{ ref("i_gpm_e_ele_adr_peres") }} peres on ele.fiche = peres.fiche
    LEFT JOIN {{ ref("i_gpm_e_ele_adr_meres") }} meres on ele.fiche = meres.fiche
    LEFT JOIN {{ ref("i_gpm_e_ele_adr_tuteurs") }} tuteurs on ele.fiche = tuteurs.fiche