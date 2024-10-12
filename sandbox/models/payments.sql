{{ config(alias='transactions') }}

select * from {{ ref('stg_payments') }}
