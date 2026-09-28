-- Light cleaning of the raw CMS table, aggregated to one row per drug.
--
-- The source has multiple National-level rows per drug: one per
-- Place_Of_Srvc (Facility vs. Office). We aggregate those into a single
-- row per drug using a SERVICES-WEIGHTED average for the dollar amounts
-- (not a plain average), so a high-volume row counts more than a
-- low-volume one -- the statistically correct way to combine them.
--
-- payment_gap_pct and dollar_impact are recalculated here, after
-- aggregation, rather than carried over from the raw table -- they need
-- to reflect the combined (weighted) amounts, not any single
-- place-of-service row.

with national_rows as (

    select
        hcpcs_code,
        drug_desc,
        geography,
        total_providers,
        total_services,
        avg_submitted_charge,
        avg_medicare_allowed,
        avg_medicare_paid
    from {{ source('raw', 'cms_drug_payments') }}
    where geography = 'National'

),

aggregated as (

    select
        hcpcs_code,
        max(drug_desc) as drug_desc,          -- same for all rows of a code
        max(geography) as geography,
        sum(total_providers) as total_providers,
        sum(total_services) as total_services,

        sum(avg_submitted_charge * total_services) / nullif(sum(total_services), 0)
            as avg_submitted_charge,
        sum(avg_medicare_allowed * total_services) / nullif(sum(total_services), 0)
            as avg_medicare_allowed,
        sum(avg_medicare_paid * total_services) / nullif(sum(total_services), 0)
            as avg_medicare_paid

    from national_rows
    group by hcpcs_code

)

select
    *,
    (avg_submitted_charge - avg_medicare_paid) / nullif(avg_submitted_charge, 0) * 100
        as payment_gap_pct,
    total_services * (avg_submitted_charge - avg_medicare_paid)
        as dollar_impact
from aggregated
