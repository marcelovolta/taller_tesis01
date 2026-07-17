{{ config(materialized='view') }}
select
    {{ dbt_utils.star(from=source('analytics', 'movies_2025'), except=["imdb_id"]) }}
from {{ source('analytics', 'movies_2025') }}