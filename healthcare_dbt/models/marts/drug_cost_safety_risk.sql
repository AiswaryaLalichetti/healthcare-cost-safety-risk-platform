-- THE central model of the project: joins CMS payment/cost data to
-- openFDA recall data on drug name, producing one combined risk view.
--
-- The join is NOT a simple equality match -- CMS's drug_desc is a long
-- description (e.g. "Injection, rituximab, 10 mg") while openFDA's
-- generic_name is just the ingredient name ("RITUXIMAB"). So this uses a
-- text "contains" match instead of an exact join key, a real technique
-- for joining two sources that don't share a clean identifier.

with cms as (
    select * from {{ ref('stg_cms_drug_payments') }}
),

recalls as (
    select * from {{ ref('int_recall_summary') }}
),

joined as (
    select
        cms.hcpcs_code,
        cms.drug_desc,
        cms.total_providers,
        cms.total_services,
        cms.avg_submitted_charge,
        cms.avg_medicare_allowed,
        cms.avg_medicare_paid,
        cms.payment_gap_pct,
        cms.dollar_impact,
        recalls.generic_name_clean,
        coalesce(recalls.total_recalls, 0) as total_recalls,
        recalls.worst_severity_rank,
        recalls.most_recent_recall_date,
        coalesce(recalls.ongoing_recalls, 0) as ongoing_recalls,
        recalls.distinct_manufacturers,

        -- When a drug_desc matches more than one generic_name (e.g. a
        -- combination-drug description, or a short name matching as a
        -- substring), keep only the most specific match: the LONGEST
        -- matching generic name is the most precise one, with worse
        -- (lower-numbered) severity as the tiebreaker.
        row_number() over (
            partition by cms.hcpcs_code
            order by length(recalls.generic_name_clean) desc nulls last,
                     recalls.worst_severity_rank asc nulls last
        ) as match_rank

    from cms
    left join recalls
        on upper(cms.drug_desc) like '%' || recalls.generic_name_clean || '%'
),

deduped as (
    select * from joined where match_rank = 1
),

scored as (
    select
        *,
        -- Cost-risk tier: based on payment gap size
        case
            when payment_gap_pct >= 75 then 'High'
            when payment_gap_pct >= 40 then 'Medium'
            else 'Low'
        end as cost_risk_tier,

        -- Safety-risk tier: based on recall existence and severity
        case
            when total_recalls = 0 then 'None'
            when worst_severity_rank = 1 then 'High'      -- has a Class I recall
            when worst_severity_rank = 2 then 'Medium'     -- has a Class II recall
            else 'Low'
        end as safety_risk_tier,

        -- A single combined score: higher = more urgent to review.
        -- Cost side contributes the gap percentage; safety side adds a
        -- weighted penalty for recall severity and whether it's ongoing.
        round(
            coalesce(payment_gap_pct, 0)
            + case when total_recalls > 0 then (4 - worst_severity_rank) * 20 else 0 end
            + coalesce(ongoing_recalls, 0) * 10
        , 1) as combined_risk_score

    from deduped
)

select * exclude (match_rank) from scored
order by combined_risk_score desc
