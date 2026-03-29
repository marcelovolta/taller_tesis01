{{ config(materialized='view') }}

select
  1 as id,
  'hola mundo y bienvenidos a dbt' as mensaje