select * from {{ ref('orders') }}
where '__filter__.orders.order_date' = '__filter__.orders.order_date'
    and '__filter__.orders.status' = '__filter__.orders.status'
    and '__filter__.orders.amount' = '__filter__.orders.amount'