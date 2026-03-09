create database ecommerce_db;
use ecommerce_db;

WITH last_order AS (
    SELECT
        c.customer_unique_id,
        MAX(o.order_purchase_timestamp) AS last_purchase_date
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_unique_id
),

customer_metrics AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.price + oi.freight_value) AS total_spend,
        AVG(oi.price + oi.freight_value) AS avg_spend_per_order,
        AVG(r.review_score) AS avg_review_score,
        COUNT(DISTINCT p.payment_type) AS num_payment_methods,
        -- Replace JULIANDAY with DATEDIFF for customer tenure
        DATEDIFF(MAX(o.order_purchase_timestamp), MIN(o.order_purchase_timestamp)) AS customer_tenure_days
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN order_reviews r ON o.order_id = r.order_id
    JOIN order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)

SELECT
    cm.*,
    lo.last_purchase_date,
    CASE
        WHEN DATEDIFF(
            (SELECT MAX(order_purchase_timestamp) FROM orders),
            lo.last_purchase_date
        ) > 180 THEN 1
        ELSE 0
    END AS churned
FROM customer_metrics cm
JOIN last_order lo ON cm.customer_unique_id = lo.customer_unique_id;

WITH logistics_performance AS (
    SELECT 
        o.order_id,
        -- Calculate delay: Positive numbers mean LATE delivery, Negative mean EARLY
        DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS days_delivery_delay,
        -- Calculate shipping time: How long did they wait total?
        DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS actual_shipping_days
    FROM orders o
    WHERE o.order_status = 'delivered'
),

financial_friction AS (
    SELECT 
        o.order_id,
        SUM(oi.price) as order_value,
        SUM(oi.freight_value) as freight_value,
        -- How much of the total bill was just shipping? (High ratio = Churn Risk)
        SUM(oi.freight_value) / SUM(oi.price + oi.freight_value) AS freight_ratio
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY o.order_id
),

payment_behavior AS (
    SELECT
        order_id,
        -- Did they use Vouchers? (Often indicates problem resolution or gifts)--
        MAX(CASE WHEN payment_type = 'voucher' THEN 1 ELSE 0 END) as used_voucher,
        -- High installments might indicate financial tightness
        MAX(payment_installments) as max_installments
    FROM order_payments
    GROUP BY order_id
)

SELECT 
    c.customer_unique_id,
    -- FIX: Use MAX() to select the state (or add to GROUP BY if tracking moves matters)
    MAX(c.customer_state) as customer_state,
    
    -- Interaction Metrics
    COUNT(DISTINCT o.order_id) as total_orders,
    MIN(o.order_purchase_timestamp) as first_order_date,
    MAX(o.order_purchase_timestamp) as last_order_date,
    
    -- Churn Definition: 1 if inactive for > 6 months
    CASE 
        WHEN DATEDIFF('2018-10-17', MAX(o.order_purchase_timestamp)) > 180 
        THEN 1 ELSE 0 
    END as is_churned,

    -- Financial Metrics
    AVG(f.order_value) as avg_ticket_size,
    AVG(f.freight_ratio) as avg_freight_sensitivity,
    
    -- Experience Metrics (Crucial for Diagnostics)
    AVG(l.days_delivery_delay) as avg_delivery_delay,
    AVG(l.actual_shipping_days) as avg_wait_time,
    AVG(r.review_score) as avg_satisfaction_score,
    MAX(p.used_voucher) as has_used_voucher

FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN logistics_performance l ON o.order_id = l.order_id
LEFT JOIN financial_friction f ON o.order_id = f.order_id
LEFT JOIN payment_behavior p ON o.order_id = p.order_id
LEFT JOIN order_reviews r ON o.order_id = r.order_id
GROUP BY c.customer_unique_id;
