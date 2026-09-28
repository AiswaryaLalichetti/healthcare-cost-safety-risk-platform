-- One row per drug, summarizing its recall history: how many recalls,
-- how severe the worst one was, and how recently one happened. This is
-- the aggregation that makes the join against CMS meaningful -- joining
-- at the raw recall level would multiply CMS rows by however many
-- recalls each drug happens to have.

select
    generic_name_clean,
    count(*) as total_recalls,
    min(severity_rank) as worst_severity_rank,  -- 1 = at least one Class I recall
    max(report_date) as most_recent_recall_date,
    sum(case when status = 'Ongoing' then 1 else 0 end) as ongoing_recalls,
    count(distinct recalling_firm) as distinct_manufacturers
from {{ ref('stg_openfda_recalls') }}
group by generic_name_clean
