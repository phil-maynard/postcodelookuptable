CREATE TABLE IF NOT EXISTS Ref_GeoAreaLookup (
  Code           VARCHAR(16) PRIMARY KEY,
  Name           VARCHAR(200),
  AlternateName  VARCHAR(200),
  Status         VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Ref_PostcodeLookup (
  Postcode   VARCHAR(8) PRIMARY KEY,
  OA21       VARCHAR(12),
  LSOA21     VARCHAR(12),
  MSOA21     VARCHAR(12),
  LADCD      VARCHAR(12),
  Latitude   DECIMAL(9,6),
  Longitude  DECIMAL(9,6)
  -- , OA21_name  VARCHAR(200)
  -- , LSOA21_name VARCHAR(200)
  -- , MSOA21_name VARCHAR(200)
  -- , LADCD_name  VARCHAR(200)
);
