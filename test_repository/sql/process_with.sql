WITH TAB_TMP1 AS
    (
    SELECT customer_id, SUM(amount) AS total_payment FROM TAB_E 
    GROUP BY customer_id
    ),
TAB_TMP2 AS
    (
    SELECT customer_id, SUM(amount) AS total_payment FROM TAB_E 
    GROUP BY customer_id
    ),

SELECT test1.customer_id, test1.total_payment, customer.first_name
FROM TAB_TMP1 INNER JOIN TAB_1 ON test1.customer_id=customer.customer_id
INNER JOIN TAB_TMP2 
WHERE test1.total_payment>150