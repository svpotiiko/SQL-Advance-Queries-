1.1
WITH LatestAddress AS (
    # retreieve address information
    SELECT 
        ca.CustomerID, 
        a.AddressID, 
        a.City, 
        a.AddressLine1, 
        # splits to select second address
        COALESCE(a.AddressLine2, '') AS AddressLine2, 
        # limits size of bits for readability
        RPAD(sp.Name, 20, ' ') AS State, 
        cr.Name AS Country,
        # select the most recent address as row = 1
        ROW_NUMBER() OVER (PARTITION BY ca.CustomerID ORDER BY a.ModifiedDate DESC) AS rn
    ## could use max
	FROM 
        `adwentureworks_db.customeraddress` AS ca
    JOIN 
        `adwentureworks_db.address` AS a ON ca.AddressID = a.AddressID
    JOIN 
        `adwentureworks_db.stateprovince` AS sp ON a.StateProvinceID = sp.StateProvinceID
    JOIN 
        `adwentureworks_db.countryregion` AS cr ON sp.CountryRegionCode = cr.CountryRegionCode
),

CustomerOrders AS (
    # selects order information 
    SELECT 
        soh.CustomerID,
        COUNT(soh.SalesOrderID) AS number_orders,
        ROUND(SUM(soh.TotalDue), 3) AS total_amount,
        MAX(soh.OrderDate) AS date_last_order
    FROM 
        `adwentureworks_db.salesorderheader` AS soh
    GROUP BY 
        soh.CustomerID
)

SELECT 
    i.CustomerID,
    c.FirstName,
    c.LastName,
    CONCAT(c.FirstName, ' ', c.LastName) AS full_name,
    CASE 
        WHEN c.Title IS NOT NULL AND c.Title <> '' THEN CONCAT(c.Title, ' ', c.LastName)
        ELSE CONCAT('Dear ', c.LastName)
    END AS addressing_title,
    c.EmailAddress,
    c.Phone,
    cust.AccountNumber,
    cust.CustomerType,
    la.City,
    la.AddressLine1,
    la.AddressLine2,
    la.State,
    la.Country,
    co.number_orders,
    co.total_amount,
    co.date_last_order
FROM 
    `adwentureworks_db.individual` AS i
JOIN 
    `adwentureworks_db.contact` AS c ON i.ContactID = c.ContactID
JOIN 
    `adwentureworks_db.customer` AS cust ON i.CustomerID = cust.CustomerID
JOIN 
    LatestAddress AS la ON cust.CustomerID = la.CustomerID AND la.rn = 1
JOIN 
    CustomerOrders AS co ON i.CustomerID = co.CustomerID
WHERE 
    cust.CustomerType = 'I'
ORDER BY 
    co.total_amount DESC;


1.2
WITH LatestOrderDate AS (
    # uses common table expression (CTE) to determine the current date
    # and identify if customer is active or not
    SELECT MAX(OrderDate) AS CurrentDate
    FROM `adwentureworks_db.salesorderheader`
),

LatestAddress AS (
    # selects address details under latest address
    SELECT 
        ca.CustomerID, 
        a.AddressID, 
        a.City, 
        a.AddressLine1, 
        # working around formatting 
        COALESCE(a.AddressLine2, '') AS AddressLine2, 
        # standardizing size for formatting
        RPAD(sp.Name, 20, ' ') AS State, 
        cr.Name AS Country,
        # identifies latest address and assigns the value to rn (rn=1 is most recent) 
        ROW_NUMBER() OVER (PARTITION BY ca.CustomerID ORDER BY a.ModifiedDate DESC) AS rn
    FROM 
        `adwentureworks_db.customeraddress` AS ca
    JOIN 
        `adwentureworks_db.address` AS a ON ca.AddressID = a.AddressID
    JOIN 
        `adwentureworks_db.stateprovince` AS sp ON a.StateProvinceID = sp.StateProvinceID
    JOIN 
        `adwentureworks_db.countryregion` AS cr ON sp.CountryRegionCode = cr.CountryRegionCode
),

CustomerOrders AS (
    # selects aggregate information on orders
    SELECT 
        soh.CustomerID,
        # counts n of orders
        COUNT(soh.SalesOrderID) AS number_orders,
        # selects and rounds total amount to 3 decimal points
        ROUND(SUM(soh.TotalDue), 3) AS total_amount,
        # latest order date
        MAX(soh.OrderDate) AS date_last_order
    FROM 
        `adwentureworks_db.salesorderheader` AS soh
    GROUP BY 
        soh.CustomerID
),

FilteredCustomers AS (
    SELECT 
        i.CustomerID,
        c.FirstName,
        c.LastName,
        CONCAT(c.FirstName, ' ', c.LastName) AS FullName,
        CASE 
        # conditional statement : if title is not specified -> 'dear' is used 
            WHEN c.Title IS NOT NULL AND c.Title <> '' THEN CONCAT(c.Title, ' ', c.LastName)
            ELSE CONCAT('Dear ', c.LastName)
        END AS addressing_title,
        c.EmailAddress,
        c.Phone,
        cust.AccountNumber,
        cust.CustomerType,
        la.City,
        la.AddressLine1,
        la.AddressLine2,
        la.State,
        la.Country,
        co.number_orders,
        co.total_amount,
        co.date_last_order
    FROM 
        `adwentureworks_db.individual` AS i
    JOIN 
        `adwentureworks_db.contact` AS c ON i.ContactID = c.ContactID
    JOIN 
        `adwentureworks_db.customer` AS cust ON i.CustomerID = cust.CustomerID
    JOIN 
        LatestAddress AS la ON cust.CustomerID = la.CustomerID AND la.rn = 1
    JOIN 
        CustomerOrders AS co ON i.CustomerID = co.CustomerID
    CROSS JOIN 
        LatestOrderDate AS lod
    WHERE 
        cust.CustomerType = 'I'
        # select customers who have not ordered in the last year
        AND co.date_last_order < DATE_SUB(lod.CurrentDate, INTERVAL 365 DAY)
)

SELECT 
    *
FROM 
    FilteredCustomers
ORDER BY 
    total_amount DESC
LIMIT 200;

1.3
WITH LatestOrderDate AS (
    SELECT MAX(OrderDate) AS CurrentDate
    FROM `adwentureworks_db.salesorderheader`
),

LatestAddress AS (
    SELECT 
        ca.CustomerID, 
        a.AddressID, 
        a.City, 
        a.AddressLine1, 
        COALESCE(a.AddressLine2, '') AS AddressLine2, 
        RPAD(sp.Name, 20, ' ') AS State, 
        cr.Name AS Country,
        ROW_NUMBER() OVER (PARTITION BY ca.CustomerID ORDER BY a.ModifiedDate DESC) AS rn
    FROM 
        `adwentureworks_db.customeraddress` AS ca
    JOIN 
        `adwentureworks_db.address` AS a ON ca.AddressID = a.AddressID
    JOIN 
        `adwentureworks_db.stateprovince` AS sp ON a.StateProvinceID = sp.StateProvinceID
    JOIN 
        `adwentureworks_db.countryregion` AS cr ON sp.CountryRegionCode = cr.CountryRegionCode
),

CustomerOrders AS (
    SELECT 
        soh.CustomerID,
        COUNT(soh.SalesOrderID) AS number_orders,
        ROUND(SUM(soh.TotalDue), 3) AS total_amount,
        MAX(soh.OrderDate) AS date_last_order
    FROM 
        `adwentureworks_db.salesorderheader` AS soh
    GROUP BY 
        soh.CustomerID
),

CustomerActivity AS (
    SELECT 
        i.CustomerID,
        c.FirstName,
        c.LastName,
        CONCAT(c.FirstName, ' ', c.LastName) AS FullName,
        CASE 
            WHEN c.Title IS NOT NULL AND c.Title <> '' THEN CONCAT(c.Title, ' ', c.LastName)
            ELSE CONCAT('Dear ', c.LastName)
        END AS addressing_title,
        c.EmailAddress,
        c.Phone,
        cust.AccountNumber,
        cust.CustomerType,
        la.City,
        la.AddressLine1,
        la.AddressLine2,
        la.State,
        la.Country,
        co.number_orders,
        co.total_amount,
        co.date_last_order,
        CASE 
            WHEN co.date_last_order >= DATE_SUB(lod.CurrentDate, INTERVAL 365 DAY) THEN 'Active'
            ELSE 'Inactive'
        END AS CustomerStatus
    FROM 
        `adwentureworks_db.individual` AS i
    JOIN 
        `adwentureworks_db.contact` AS c ON i.ContactID = c.ContactID
    JOIN 
        `adwentureworks_db.customer` AS cust ON i.CustomerID = cust.CustomerID
    JOIN 
        LatestAddress AS la ON cust.CustomerID = la.CustomerID AND la.rn = 1
    JOIN 
        CustomerOrders AS co ON i.CustomerID = co.CustomerID
    CROSS JOIN 
        LatestOrderDate AS lod
    WHERE 
        cust.CustomerType = 'I'
)

SELECT 
    *
FROM 
    CustomerActivity
ORDER BY 
    CustomerID DESC
LIMIT 500;

1.4
WITH LatestOrderDate AS 
(SELECT MAX(OrderDate) AS CurrentDate
   FROM `adwentureworks_db.salesorderheader`
),

LatestAddress AS
(
SELECT ca.CustomerID, 
       a.AddressID, 
       a.City, 
       a.AddressLine1, 
       COALESCE(a.AddressLine2, '') AS AddressLine2, 
       RPAD(sp.Name, 20, ' ') AS State, 
       cr.Name AS Country,
       ROW_NUMBER() OVER (PARTITION BY ca.CustomerID ORDER BY a.ModifiedDate DESC) AS rn

  FROM `adwentureworks_db.customeraddress` AS ca

  JOIN `adwentureworks_db.address` AS a 
   ON ca.AddressID = a.AddressID

  JOIN `adwentureworks_db.stateprovince` AS sp
    ON a.StateProvinceID = sp.StateProvinceID

  JOIN `adwentureworks_db.countryregion` AS cr
    ON sp.CountryRegionCode = cr.CountryRegionCode
),

CustomerOrders AS (
    SELECT 
        soh.CustomerID,
        COUNT(soh.SalesOrderID) AS number_orders,
        ROUND(SUM(soh.TotalDue), 3) AS total_amount,
        MAX(soh.OrderDate) AS date_last_order

    FROM `adwentureworks_db.salesorderheader` AS soh
    GROUP BY soh.CustomerID
),


ActiveNorthAmericanCustomers AS 
(
SELECT i.CustomerID,
       c.FirstName,
       c.LastName,
       CONCAT(c.FirstName, ' ', c.LastName) AS FullName,

       CASE WHEN c.Title IS NOT NULL AND c.Title <> '' 
            THEN CONCAT(c.Title, ' ', c.LastName)
            ELSE CONCAT('Dear ', c.LastName)
            END AS addressing_title,

        c.EmailAddress,
        c.Phone,
        cust.AccountNumber,
        cust.CustomerType,
        la.City,
        la.AddressLine1,
        la.AddressLine2,
        la.State,
        la.Country,
        co.number_orders,
        co.total_amount,
        co.date_last_order,

        CASE 
            WHEN co.date_last_order >= DATE_SUB(lod.CurrentDate, INTERVAL 365 DAY) THEN 'Active'
            ELSE 'Inactive'
            END AS CustomerStatus

  FROM `adwentureworks_db.individual` AS i
  
  JOIN `adwentureworks_db.contact` AS c 
    ON i.ContactID = c.ContactID

  JOIN `adwentureworks_db.customer` AS cust
    ON i.CustomerID = cust.CustomerID

  JOIN LatestAddress AS la 
    ON cust.CustomerID = la.CustomerID
    AND la.rn = 1

  JOIN CustomerOrders AS co
    ON i.CustomerID = co.CustomerID

    CROSS JOIN LatestOrderDate AS lod

 WHERE cust.CustomerType = 'I'
       AND la.Country IN ('Canada', 'United States', 'Mexico')
	   ## group territory (north america only)
       AND ((co.total_amount >= 2500 OR co.number_orders >= 5))
)

SELECT CustomerID,
       FirstName,
       LastName,
       FullName,
       addressing_title,
       EmailAddress,
       Phone,
       AccountNumber,
       CustomerType,
       City,
       SPLIT(addressline1, ' ')[OFFSET(0)] AS Address_No,
       RIGHT(addressline1, LENGTH(addressline1) - INSTR(addressline1, ' ')) AS Address_Street,
       ## REGX instead
	   AddressLine2,
       State,
       Country,
       number_orders,
       total_amount,
       date_last_order

  FROM ActiveNorthAmericanCustomers

WHERE CustomerStatus = 'Active'

ORDER BY Country,
         State,
         date_last_order

2.1
SELECT 
  FORMAT_TIMESTAMP('%Y-%m', soh.OrderDate) AS order_month,
  ## last_day
  st.CountryRegionCode,
  st.Name AS Region,
  COUNT(DISTINCT soh.SalesOrderId) AS number_orders,
  COUNT(DISTINCT soh.CustomerID) AS number_customers,
  COUNT(DISTINCT soh.SalesPersonID) AS no_salesPersons,
  CAST(ROUND(SUM(soh.TotalDue), 0) AS INT) AS total_amount

FROM `adwentureworks_db.salesorderheader` soh

JOIN `adwentureworks_db.salesterritory` st
  ON soh.TerritoryID = st.TerritoryID

GROUP BY 
  order_month, 
  CountryRegionCode,
  Region;

2.2
WITH MonthlySales AS (
  SELECT 
  FORMAT_TIMESTAMP('%Y-%m', soh.OrderDate) AS order_month,
  st.CountryRegionCode AS CountryRegionCode,
  st.Name AS Region,
  COUNT(DISTINCT soh.SalesOrderId) AS number_orders,
  COUNT(DISTINCT soh.CustomerID) AS number_customers,
  COUNT(DISTINCT soh.SalesPersonID) AS no_salesPersons,
  CAST(ROUND(SUM(soh.TotalDue), 0) AS INT) AS total_amount

FROM `adwentureworks_db.salesorderheader` soh

JOIN `adwentureworks_db.salesterritory` st
  ON soh.TerritoryID = st.TerritoryID

GROUP BY 
  order_month, 
  CountryRegionCode,
  Region
)

SELECT 
    order_month,
    CountryRegionCode,
    Region,
    number_orders,
    number_customers,
    no_salesPersons,
    total_amount,
    
    SUM(total_amount) OVER (
        PARTITION BY CountryRegionCode, Region
        ORDER BY order_month
    ) AS CumulativeTotalWithTax

FROM MonthlySales;

2.3
WITH MonthlySales AS (
  SELECT 
  FORMAT_TIMESTAMP('%Y-%m', soh.OrderDate) AS order_month,
  st.CountryRegionCode AS CountryRegionCode,
  st.Name AS Region,
  COUNT(DISTINCT soh.SalesOrderId) AS number_orders,
  COUNT(DISTINCT soh.CustomerID) AS number_customers,
  COUNT(DISTINCT soh.SalesPersonID) AS no_salesPersons,
  CAST(ROUND(SUM(soh.TotalDue), 0) AS INT) AS total_amount

FROM `adwentureworks_db.salesorderheader` soh

JOIN `adwentureworks_db.salesterritory` st
  ON soh.TerritoryID = st.TerritoryID

GROUP BY 
  order_month, 
  CountryRegionCode,
  Region
)

, CumulativeSales AS (
SELECT 
    order_month,
    CountryRegionCode,
    Region,
    number_orders,
    number_customers,
    no_salesPersons,
    total_amount,
    
    SUM(total_amount) OVER (
        PARTITION BY CountryRegionCode, Region
        ORDER BY order_month
    ) AS CumulativeTotalWithTax

FROM MonthlySales
)

SELECT 
    *,
    RANK() OVER (
        PARTITION BY CountryRegionCode, Region
        ORDER BY number_orders DESC
    ) AS country_sales_rank

FROM CumulativeSales;

2.4
WITH MonthlySales AS
(
  SELECT 
    FORMAT_TIMESTAMP('%Y-%m', soh.OrderDate) AS order_month,
    st.CountryRegionCode AS CountryRegionCode,
    st.Name AS Region,

    COUNT(DISTINCT soh.SalesOrderId) AS number_orders,
    COUNT(DISTINCT soh.CustomerID) AS number_customers,
    COUNT(DISTINCT soh.SalesPersonID) AS no_salesPersons,
    CAST(ROUND(SUM(soh.TotalDue), 0) AS INT) AS total_amount

  FROM adwentureworks_db.salesorderheader soh

  JOIN adwentureworks_db.salesterritory st
    ON soh.TerritoryID = st.TerritoryID

  GROUP BY order_month, CountryRegionCode, Region
),



CumulativeSales AS 
(
  SELECT 
    order_month,
    CountryRegionCode,
    Region,
    number_orders,
    number_customers,
    no_salesPersons,
    total_amount,

    SUM(total_amount) OVER
                          (
                           PARTITION BY CountryRegionCode, Region
                           ORDER BY order_month
                          ) AS CumulativeTotalWithTax

  FROM MonthlySales
),



TaxSummary AS
(
  SELECT 
    st.CountryRegionCode,
    MAX(tax.TaxRate) AS max_tax_rate,
    st.StateProvinceID

  FROM adwentureworks_db.salestaxrate AS tax

RIGHT JOIN `adwentureworks_db.stateprovince` AS st
    ON tax.StateProvinceID = st.StateProvinceID

  GROUP BY st.CountryRegionCode, st.StateProvinceID
),



CountryTax AS
(
  SELECT 
    CountryRegionCode,
    ROUND(AVG(max_tax_rate),1) AS mean_tax_rate,

    ROUND((COUNT(DISTINCT CASE WHEN max_tax_rate IS NOT NULL 
                               THEN StateProvinceID END) 
                                    / COUNT(DISTINCT StateProvinceID)), 2) AS perc_provinces_w_tax

  FROM TaxSummary

  GROUP BY CountryRegionCode
)



SELECT 
  cs.order_month,
  cs.CountryRegionCode,
  cs.Region,
  cs.number_orders,
  cs.number_customers,
  cs.no_salesPersons,
  cs.total_amount,
  cs.CumulativeTotalWithTax,
  ct.mean_tax_rate,
  ct.perc_provinces_w_tax,

  RANK() OVER
             (
              PARTITION BY cs.CountryRegionCode, cs.Region
              ORDER BY cs.total_amount DESC
             ) AS country_sales_rank

FROM CumulativeSales AS cs

LEFT JOIN CountryTax AS ct
       ON cs.CountryRegionCode = ct.CountryRegionCode

WHERE
  cs.CountryRegionCode = 'US'

ORDER BY
  cs.order_month,
  cs.CountryRegionCode,
  cs.Region;