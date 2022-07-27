IndexCol,	
VendorName,	
ProductName,
ProductCategory,	
BrandName,	
Number0fUnitSold,	
SoldDollarAmount,	
AvgUnitPriceSoldAmt,	
ReportedDate,	
ProductID,

CREATE TABLE ecommerce(
    IndexCol SERIAL,
    VendorName VARCHAR(50),
    ProductName VARCHAR(50),
    ProductCategory VARCHAR(50),
    BrandName VARCHAR(50),
    Number0fUnitSold VARCHAR(50),
    SoldDollarAmount VARCHAR(50),
    AvgUnitPriceSoldAmt VARCHAR(50),
    ReportedDate DATE,
    ProductID VARCHAR(50),
PRIMARY KEY(IndexCol)
);