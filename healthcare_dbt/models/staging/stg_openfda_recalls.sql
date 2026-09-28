-- Light cleaning of the raw recalls table, plus a numeric severity rank
-- (lower = more serious) so we can sort/aggregate by severity in SQL,
-- since 'classification' alone is just text (Class I/II/III).

select
    recall_number,
    classification,
    case classification
        when 'Class I' then 1
        when 'Class II' then 2
        when 'Class III' then 3
        else 4
    end as severity_rank,
    status,
    recalling_firm,
    reason_for_recall,
    report_date::date as report_date,
    distribution_pattern,
    upper(trim(generic_name)) as generic_name_clean
from {{ source('raw', 'openfda_recalls') }}
where generic_name is not null
