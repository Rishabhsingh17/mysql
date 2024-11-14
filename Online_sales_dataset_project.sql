use project1;
select * from online_sales_dataset;

#counting total no. of rows
select count(InvoiceNo) as total from online_sales_dataset;

# Checking for Null Values in Specific Columns
SELECT InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country, Discount, PaymentMethod, ShippingCost, Category, SalesChannel, ReturnStatus, ShipmentProvider, WarehouseLocation, OrderPriority 
FROM online_sales_dataset 
WHERE InvoiceNo IS NULL 
   OR StockCode IS NULL 
   OR Description IS NULL 
   OR Quantity IS NULL 
   OR InvoiceDate IS NULL 
   OR UnitPrice IS NULL 
   OR CustomerID IS NULL 
   OR Country IS NULL 
   OR Discount IS NULL 
   OR PaymentMethod IS NULL 
   OR ShippingCost IS NULL 
   OR Category IS NULL 
   OR SalesChannel IS NULL 
   OR ReturnStatus IS NULL 
   OR ShipmentProvider IS NULL 
   OR WarehouseLocation IS NULL 
   OR OrderPriority IS NULL;
   
#Checking for Duplicate Rows
SELECT *, COUNT(*) AS duplicate_count
FROM online_sales_dataset
GROUP BY InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country, Discount, PaymentMethod, ShippingCost, Category, SalesChannel, ReturnStatus, ShipmentProvider, WarehouseLocation, OrderPriority
HAVING duplicate_count > 1;

#Counting Products by Description
select Description, count(Description) as products_count from online_sales_dataset group by Description order by products_count desc;

#Counting Products by Return Status
select Description, count(ReturnStatus) as productcount, ReturnStatus as productstatus from online_sales_dataset group by Description, ReturnStatus;

#Counting Products by Payment Method
select PaymentMethod, count(PaymentMethod) as count from online_sales_dataset group by PaymentMethod; 

#Counting Products by Category
select Category , count(Category) as count from online_sales_dataset group by Category;


#Counting Products by saleschannel
select SalesChannel, count(SalesChannel) as count from online_sales_dataset group by SalesChannel;
 
#Counting Products by Category and Country
select Country, Category, count(Category) as categorycount from online_sales_dataset group by country, Category order by Country, Category;

#Calculating Sales for Each Product Description
select Description, round(sum((Quantity * UnitPrice) - (Quantity * UnitPrice * Discount)), 2) as sales from online_sales_dataset group by Description;

#Calculating Monthly Sales and Sales Count by Product
SELECT 
    Description, 
    EXTRACT(YEAR FROM InvoiceDate) AS Year, 
    EXTRACT(MONTH FROM InvoiceDate) AS Month, 
    COUNT(InvoiceDate) AS monthly_sales_count ,
    round(sum((Quantity * UnitPrice) - (Quantity * UnitPrice * Discount)), 2) as monthlysales
FROM 
    online_sales_dataset 
GROUP BY 
    Description, Year, Month
ORDER BY 
    Year, Month, Description;
    
#Calculating Sales per Sales Channel
select SalesChannel, round(sum((Quantity * UnitPrice) - (Quantity * UnitPrice * Discount)), 2) as salesperchannel from online_sales_dataset group by SalesChannel;

#Calculating Sales by Country
select Country, round(sum((Quantity * UnitPrice) - (Quantity * UnitPrice * Discount)), 2) as countrywisesales from online_sales_dataset group by Country;

#Calculating Return Rate for Each Product
SELECT Description, 
COUNT(CASE WHEN ReturnStatus = 'Returned' THEN 1 END) AS return_count,
COUNT(*) AS total_sales,
AVG(CASE WHEN ReturnStatus = 'Returned' THEN 1 ELSE 0 END) AS return_rate
FROM online_sales_dataset
GROUP BY Description
ORDER BY return_rate DESC;

