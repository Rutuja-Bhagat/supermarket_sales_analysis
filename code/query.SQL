select
    b.city,
    p.product_line,
    SUM(s.gross_income) as sum1,
    RANK() OVER (
        PARTITION BY
            b.city
        ORDER BY
            SUM(s.gross_income) DESC
    ) AS Sales_Rank_By_Branch_Product
FROM
    Sales s
    LEFT JOIN Branch b ON s.branch_id = b.branch_id
    LEFT JOIN Product p ON s.product_id = p.product_id
GROUP BY
    city,
    product_line;

SELECT
    Gender,
    Product_Line,
    sum(quantity) AS c1
FROM
    Sales A
    LEFT JOIN Product B ON A.Product_ID = b.Product_ID
GROUP BY
    Gender,
    Product_Line
Order BY
    c1 desc,
    Gender;

SELECT
    City,
    SUM(Gross_income)
FROM
    Sales A
    LEFT JOIN Branch B ON A.Branch_ID = B.Branch_ID
GROUP BY
    City;