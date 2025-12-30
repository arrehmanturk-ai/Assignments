-- Q1. List top 5 customers by total order amount.
SELECT top 5
	c.CustomerID,
	c.Name AS CustomerName,
	SUM(od.Quantity * od.UnitPrice) AS TotalSpent
FROM 
	Customer c
JOIN SalesOrder o 
	ON c.CustomerID = o.CustomerID
JOIN SalesOrderDetail od 
	ON o.OrderID = od.OrderID
Group By 
	c.CustomerID, c.Name
Order By 
	TotalSpent DESC;

-- Q2. Find the number of products supplied by each supplier.
SELECT 
    s.SupplierID,
    s.Name AS SupplierName,
    COUNT(pod.ProductID) AS ProductCount
FROM 
    supplier s
    INNER JOIN PurchaseOrder po ON s.SupplierID = po.SupplierID
	INNER JOIN PurchaseOrderDetail pod ON pod.OrderID = po.OrderID
GROUP BY 
    s.SupplierID, s.Name
HAVING 
    COUNT(pod.ProductID) > 10
ORDER BY 
    ProductCount DESC;

-- Q3. Identify products that have been ordered but never returned.
SELECT
    p.ProductID,
    p.Name AS ProductName,
    SUM(sod.Quantity) AS TotalOrderQuantity
FROM dbo.Product p
JOIN dbo.SalesOrderDetail sod
    ON p.ProductID = sod.ProductID
LEFT JOIN dbo.ReturnDetail rd
    ON sod.ProductID = rd.ProductID
WHERE
    rd.ProductID IS NULL
GROUP BY
    p.ProductID, p.Name;

-- Q4. For each category, find the most expensive product.
SELECT
    c.CategoryID,
    c.Name AS CategoryName,
    p.Name AS ProductName,
    p.Price
FROM dbo.Product p
JOIN dbo.Category c
    ON p.CategoryID = c.CategoryID
WHERE p.Price = (
    SELECT MAX(p2.Price)
    FROM dbo.Product p2
    WHERE p2.CategoryID = p.CategoryID
);

-- Q5. List all sales orders with customer name, product name, category, and supplier.
SELECT 
	so.OrderID,
	c.Name AS CustomerName,
	p.Name AS ProductName,
	cat.Name AS CategoryName,
	s.Name AS SupplierName,
	sod.Quantity
FROM
	Customer c
JOIN SalesOrder so
	ON c.CustomerID =so.CustomerID
JOIN SalesOrderDetail sod
	ON sod.OrderID = so.OrderID
JOIN Product p
	ON p.ProductID = sod.ProductID
JOIN Category cat
	ON cat.CategoryID = p.CategoryID
JOIN PurchaseOrderDetail pod
	ON pod.ProductID = p.ProductID
JOIN PurchaseOrder po
	ON po.OrderID = pod.OrderID
JOIN Supplier s
	ON s.SupplierID = po.SupplierID;


-- Q6. Find all shipments with details of warehouse, manager, and products shipped.
SELECT
    sh.ShipmentID,
    l.Name AS WarehouseName,
    e.Name AS ManagerName,
    p.Name AS ProductName,
    sd.Quantity AS QuantityShipped,
    sh.TrackingNumber
FROM dbo.Shipment sh
JOIN dbo.Warehouse w
    ON sh.WarehouseID = w.WarehouseID
JOIN Location l
	ON l.LocationID = w.LocationID
JOIN dbo.Employee e
    ON w.ManagerID = e.EmployeeID
JOIN dbo.ShipmentDetail sd
    ON sh.ShipmentID = sd.ShipmentID
JOIN dbo.Product p
    ON sd.ProductID = p.ProductID;

-- Q7. Find the top 3 highest-value orders per customer using RANK().
SELECT
    CustomerID,
    CustomerName,
    OrderID,
    TotalAmount
FROM (
    SELECT
        c.CustomerID,
        c.Name AS CustomerName,
        so.OrderID,
        so.TotalAmount,
        RANK() OVER (
            PARTITION BY c.CustomerID
            ORDER BY so.TotalAmount DESC
        ) AS OrderRank
    FROM Customer c
    JOIN SalesOrder so
        ON c.CustomerID = so.CustomerID
) ranked_orders
WHERE OrderRank <= 3
ORDER BY CustomerID, OrderRank;


-- Q8. For each product, show its sales history with the previous and next sales quantities (based on order date).
SELECT
    p.ProductID,
    p.Name AS ProductName,
    so.OrderID,
    so.OrderDate,
    sod.Quantity,
    LAG(sod.Quantity) OVER (
        PARTITION BY p.ProductID
        ORDER BY so.OrderDate
    ) AS PrevQuantity,
    LEAD(sod.Quantity) OVER (
        PARTITION BY p.ProductID
        ORDER BY so.OrderDate
    ) AS NextQuantity
FROM dbo.Product p
JOIN dbo.SalesOrderDetail sod
    ON p.ProductID = sod.ProductID
JOIN dbo.SalesOrder so
    ON sod.OrderID = so.OrderID
ORDER BY
    p.ProductID,
    so.OrderDate;


-- Q9. Create a view named vw_CustomerOrderSummary that shows for each customer.
CREATE VIEW vw_CustomerOrderSummary AS
SELECT 
    c.CustomerID,
    c.Name AS CustomerName,
    COUNT(so.OrderID) AS TotalOrders,
    SUM(so.TotalAmount) AS TotalAmountSpent,
    MAX(so.OrderDate) AS LastOrderDate
FROM dbo.Customer c
LEFT JOIN dbo.SalesOrder so 
    ON c.CustomerID = so.CustomerID
GROUP BY 
    c.CustomerID,
    c.Name;


-- Q10. Write a stored procedure sp_GetSupplierSales that takes a SupplierID as input and returns the total sales amount for all products supplied by that supplier.
CREATE PROCEDURE sp_GetSupplierSales
    @SupplierID INT
AS
BEGIN
    SELECT
        s.SupplierID,
        s.Name AS SupplierName,
        SUM(
            sod.Quantity * sod.UnitPrice
            - sod.Discount
            + sod.Tax
        ) AS TotalSalesAmount
    FROM dbo.Supplier s
    JOIN dbo.Product p
        ON s.SupplierID = p.SupplierID
    JOIN dbo.SalesOrderDetail sod
        ON p.ProductID = sod.ProductID
    WHERE s.SupplierID = @SupplierID
    GROUP BY
        s.SupplierID,
        s.Name;
END;

