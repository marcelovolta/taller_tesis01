{{ config(materialized='table') }}

with movie_data as (
    select mv.tmdb_id, imdb_id, 
        title, release_date, 
        original_language, 
        production_countries, 
        production_companies, 
        genres,
        coalesce(bo.production_budget, mv.budget) as production_budget,
        bo.opening_weekend_revenue, 
        bo.opening_theaters,
        runtime,
        cast_top_10, 
        directors, youtube_trailer_key
    from movies_2025 as mv
    inner join box_office as bo
        on mv.tmdb_id = bo.tmdb_id
    where runtime > 60
    and date(release_date) >= '2015-01-01'
    and coalesce(bo.production_budget, mv.budget) is not null
),

trailer_rollup as (
    select tc.tmdb_id,
        count(distinct video_id) as trailer_count, 
        count(distinct comment_id) as comment_count, 
        sum(like_count) as total_likes,
        avg(word_count) as avg_word_count, 
        mode() within group (order by word_count)  as mode_word_count,
        min(published_at) as earliest_comment_at 
    from trailer_comments as tc
    group by tc.tmdb_id
), 

sentiment_rollup as (
    select tc.tmdb_id,
        count(distinct tc.comment_id) as comment_count,
        sum(case when label = 'negative' then 1 else 0 end) as count_negative_comments, 
        sum(case when label = 'positive' then 1 else 0 end) as count_positive_comments, 
        avg(case when label = 'negative' then score else null end) as avg_score_negative_comments, 
        avg(case when label = 'positive' then score else null end) as avg_score_positive_comments 
    from trailer_comments_sentiment as tcs
    inner join trailer_comments as tc
        on tcs.comment_id = tc.comment_id
    group by tc.tmdb_id
),

sentiment_rollup_plus_controversy as (
select sr.*,
	(sr.count_positive_comments * sr.avg_score_positive_comments)/
	    (sr.count_negative_comments * sr.avg_score_negative_comments) as positive_ratio,
	1 - (abs((sr.count_negative_comments * sr.avg_score_negative_comments) - 
	(sr.count_positive_comments * sr.avg_score_positive_comments))/
        (nullif(sr.comment_count, 0))) as controversy_index
from sentiment_rollup as sr
)

select (md.opening_weekend_revenue * 1.0)/nullif(md.production_budget, 0.0) as success_indicator, 
	(case when (md.opening_weekend_revenue * 1.0)/nullif(md.production_budget, 0.0) >= 1 then 1
	else 0 end) as success_target,
	md.*, 
	tr.trailer_count,
    tr.comment_count,
    tr.total_likes,
    tr.avg_word_count,
    tr.mode_word_count,
    tr.earliest_comment_at,
    md.release_date::date - tr.earliest_comment_at::date as first_comment_anticipation,
    src.count_negative_comments,
    src.count_positive_comments,
    src.avg_score_negative_comments,
    src.avg_score_positive_comments,
    src.positive_ratio,
    src.controversy_index
from movie_data as md
left join trailer_rollup tr
    on md.tmdb_id = tr.tmdb_id
left join sentiment_rollup_plus_controversy as src
    on md.tmdb_id = src.tmdb_id
where md.opening_weekend_revenue/nullif(md.production_budget, 0) is not null