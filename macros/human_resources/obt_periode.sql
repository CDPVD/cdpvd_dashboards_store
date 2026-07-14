{% macro informations_date(date_column) %}

CASE
    WHEN {{ date_column }} >= DATEADD(MONTH, -3, GETDATE()) THEN 0
    WHEN {{ date_column }} >= DATEADD(MONTH, -5, GETDATE())
         AND {{ date_column }} < DATEADD(MONTH, -3, GETDATE()) THEN 1
    WHEN {{ date_column }} >= DATEADD(YEAR, -1, GETDATE())
         AND {{ date_column }} < DATEADD(MONTH, -5, GETDATE()) THEN 2
    WHEN {{ date_column }} >= DATEADD(YEAR, -2, GETDATE())
         AND {{ date_column }} < DATEADD(YEAR, -1, GETDATE()) THEN 3
    WHEN {{ date_column }} >= DATEADD(YEAR, -3, GETDATE())
         AND {{ date_column }} < DATEADD(YEAR, -2, GETDATE()) THEN 4
    WHEN {{ date_column }} >= DATEADD(YEAR, -4, GETDATE())
         AND {{ date_column }} < DATEADD(YEAR, -3, GETDATE()) THEN 5
    WHEN {{ date_column }} >= DATEADD(YEAR, -5, GETDATE())
         AND {{ date_column }} < DATEADD(YEAR, -4, GETDATE()) THEN 6
    ELSE 7
END AS PERIODE,

YEAR({{ date_column }}) AS ANNEE,

((MONTH({{ date_column }}) + 5) % 12) + 1 AS MOIS,

DATEDIFF(
    WEEK,
    DATEFROMPARTS(
        YEAR({{ date_column }}) -
        CASE WHEN MONTH({{ date_column }}) < 7 THEN 1 ELSE 0 END,
        7,
        1
    ),
    {{ date_column }}
) + 1 AS SEMAINE

{% endmacro %}