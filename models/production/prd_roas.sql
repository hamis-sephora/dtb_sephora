{{ config(materialized='table') }}

select * from {{ref('int_media_cost')}}