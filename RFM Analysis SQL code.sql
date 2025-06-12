-- Bước 1: Tạo bảng tạm thay thế bằng CTE (Common Table Expression)
WITH customer_countries AS (
  -- Bước mới: Lấy country chính xác cho từng customer
  SELECT 
    CustomerID,
    Country
  FROM (
    SELECT 
      CustomerID, 
      Country,
      ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY InvoiceDate DESC) AS rn
    FROM `e-commerce-data-461515.Ecommerce_data.ecommerce_order`
    WHERE CustomerID IS NOT NULL
  ) 
  WHERE rn = 1  -- Chỉ lấy country từ bản ghi gần nhất của mỗi customer
),


cleaned_data AS (
  SELECT 
    e.CustomerID as CustomerID,
    cc.Country as Country,
    e.InvoiceNo,
    e.InvoiceDate,
    e.Quantity,
    e.UnitPrice,
    e.Quantity * e.UnitPrice AS TotalPrice
  FROM `e-commerce-data-461515.Ecommerce_data.ecommerce_order`  e
  JOIN customer_countries cc ON e.CustomerID = cc.CustomerID
  WHERE 
    e.CustomerID IS NOT NULL 
    AND Quantity > 0
),

rfm_raw AS (
  SELECT 
    CustomerID,
    Max(Country) as Country,
    DATE_DIFF('2011-12-10',MAX(InvoiceDate),day) AS Recency, 
    COUNT(DISTINCT InvoiceNo) AS Frequency,
    SUM(TotalPrice) AS Monetary
  FROM cleaned_data d
  GROUP BY CustomerID
),

-- Bước 2: Tính điểm RFM (dùng NTILE thông qua subquery)
rfm_scores AS (
  SELECT 
    CustomerID,
    Country,
    Recency,
    Frequency,
    Monetary,
    NTILE(5) OVER (ORDER BY Recency DESC) AS R_Score,
    NTILE(5) OVER (ORDER BY Frequency ASC) AS F_Score,
    NTILE(5) OVER (ORDER BY Monetary ASC) AS M_Score
  FROM rfm_raw
)

-- Bước 3: Phân nhóm và xuất kết quả (không cần temporary table)
SELECT 
  CustomerID,
  Country,
  Recency,
  Frequency,
  Monetary,
  R_Score,
  F_Score,
  M_Score,
  CASE 
    WHEN (R_Score + F_Score + M_Score) >= 12 THEN 'VIP'
    WHEN (R_Score + F_Score + M_Score) >= 9 THEN 'Loyal'
    WHEN (R_Score + F_Score + M_Score) >= 6 THEN 'Potential'
    WHEN (R_Score + F_Score + M_Score) >= 4 THEN 'At Risk'
    ELSE 'Lost'
  END AS Segment
FROM rfm_scores;