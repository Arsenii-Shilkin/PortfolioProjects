
/*
Data Cleaning Project – Nashville Housing Dataset

This SQL script cleans and standardises the NashvilleHousing dataset to improve
its usability for analysis. Steps include standardising date formats,
populating missing property addresses via self-joins, splitting address fields
into separate columns, converting binary flags to readable values, removing
duplicates, and dropping unused columns. The goal is to produce a clean,
consistent, and analysis-ready dataset.
*/


-------------------------------------------------------------------------------------------------------------------------

-- 1. Standardise Date Format 

SELECT SaleDate, CONVERT (Date, SaleDate)
FROM NashvilleHousing

UPDATE NashvilleHousing
SET SaleDate = CONVERT (Date, SaleDate)

-------------------------------------------------------------------------------------------------------------------------

-- 2. Populate Property Address Data

SELECT *
FROM NashvilleHousing
WHERE PropertyAddress IS NULL


SELECT *
FROM NashvilleHousing a
JOIN NashvilleHousing b
	ON a.ParcelID = b.ParcelID
	AND a.UniqueID <> b.UniqueID


SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress , ISNULL(a.PropertyAddress, b.PropertyAddress)
FROM NashvilleHousing a
JOIN NashvilleHousing b
	ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL


UPDATE a
SET PropertyAddress = ISNULL(a.PropertyAddress, b.PropertyAddress)
FROM NashvilleHousing a
JOIN NashvilleHousing b
	ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL

-------------------------------------------------------------------------------------------------------------------------

-- 3. Breaking out Address into Individual Columns (Address, City, State)


SELECT PropertyAddress 
FROM NashvilleHousing

SELECT 
SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) - 1) as Address,
SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) + 1, LEN(PropertyAddress)) as City
FROM NashvilleHousing


ALTER TABLE NashvilleHousing
Add PropertySplitAddress Nvarchar(255)

UPDATE NashvilleHousing
SET PropertySplitAddress = SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) - 1)

ALTER TABLE NashvilleHousing
Add PropertySplitCity Nvarchar(255)

UPDATE NashvilleHousing
SET PropertySplitCity = SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) + 1, LEN(PropertyAddress))


-- Splitting OwnerAddress

SELECT OwnerAddress
FROM NashvilleHousing


SELECT
PARSENAME(REPLACE(OwnerAddress,',','.'), 3),
PARSENAME(REPLACE(OwnerAddress,',','.'), 2),
PARSENAME(REPLACE(OwnerAddress,',','.'), 1)
FROM NashvilleHousing


ALTER TABLE NashvilleHousing
Add OwnerSplitAddress Nvarchar(255)

UPDATE NashvilleHousing
SET OwnerSplitAddress = PARSENAME(REPLACE(OwnerAddress,',','.'), 3)

ALTER TABLE NashvilleHousing
Add OwnerSplitCity Nvarchar(255)

UPDATE NashvilleHousing
SET OwnerSplitCity = PARSENAME(REPLACE(OwnerAddress,',','.'), 2)

ALTER TABLE NashvilleHousing
Add OwnerSplitState Nvarchar(255)

UPDATE NashvilleHousing
SET OwnerSplitState = PARSENAME(REPLACE(OwnerAddress,',','.'), 1)

-------------------------------------------------------------------------------------------------------------------------

-- 4. Change 1 and 0 to 'Yes' and 'No' in 'Sold as Vacant' field

SELECT 
    COLUMN_NAME,
    DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'NashvilleHousing'
  AND COLUMN_NAME = 'SoldAsVacant'


ALTER TABLE NashvilleHousing
ALTER COLUMN SoldAsVacant Nvarchar(255);


SELECT 
    SoldAsVacant,
    CASE
        WHEN SoldAsVacant = 1 THEN 'Yes'
        WHEN SoldAsVacant = 0 THEN 'No'
    END
FROM NashvilleHousing


UPDATE NashvilleHousing
SET SoldAsVacant =
    CASE
        WHEN SoldAsVacant = '1' THEN 'Yes'
        WHEN SoldAsVacant = '0' THEN 'No'
    END


-------------------------------------------------------------------------------------------------------------------------

-- 5. Remove Duplicates

WITH RowNumCTE AS(
SELECT *, 
    ROW_NUMBER() OVER (
    PARTITION BY ParcelID,
                 PropertyAddress,
                 SalePrice,
                 SaleDate,
                 LegalReference
                 ORDER BY 
                    UniqueID
                    ) row_num
FROM NashvilleHousing
)
DELETE
FROM RowNumCTE
WHERE row_num > 1


-------------------------------------------------------------------------------------------------------------------------

-- 6. Delete Unused Columns

ALTER TABLE NashvilleHousing
DROP COLUMN OwnerAddress, TaxDistrict, PropertyAddress

