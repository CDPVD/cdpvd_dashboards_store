{#
    Règle « emploi actif » du RRM, sur un code ETAT de pai_hemp.

    Emploi actif = ETAT qui ne commence pas par C (A, P, S), ou ETAT C
    considéré comme actif (par défaut C10, mise à pied cyclique).

    Liste des ETAT C actifs propre au CSS, dans le dbt_project.yml du CSS :
        vars:
          rrm_etats_c_actifs: ['C10']

    Retourne une condition SQL (à utiliser dans un WHERE ou un CASE WHEN).
    Un ETAT NULL n'est ni actif ni une fin d'emploi : la condition et sa
    négation (NOT ...) sont toutes deux inconnues, donc fausses.

    Utilisée par fact_rrm_retention, int_rrm_evenements et int_rrm_emplois.
#}

{% macro rrm_est_etat_actif(etat) %}
    {%- set etats_c_actifs = var("rrm_etats_c_actifs", ["C10"]) -%}
    ({{ etat }} NOT LIKE 'C%'
    {%- if etats_c_actifs | length > 0 %}
        OR {{ etat }} IN (
            {%- for code in etats_c_actifs -%}
                '{{ code }}'{{ ", " if not loop.last }}
            {%- endfor -%}
        )
    {%- endif -%}
    )
{%- endmacro %}
