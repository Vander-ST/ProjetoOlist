SELECT COUNT(DISTINCT T2.seller_id),
        MIN(T1.order_approved_at),
        MAX(T1.order_approved_at)
FROM tb_orders as T1
LEFT JOIN tb_order_items as T2
ON T1.order_id = T2.order_id
WHERE T1.order_approved_at BETWEEN  '2017-06-01' AND '2018-06-01'
