-- Tabla DIEZMIL (10,000 registros)
CREATE TABLE diezmil (
    -- Tipos de cadena de texto
    columna_char CHAR(10),                  -- Cadena de longitud fija
    columna_varchar VARCHAR(100),           -- Cadena de longitud variable
    columna_text TEXT,                      -- Texto de longitud máxima
    columna_nchar NCHAR(10),                -- Cadena Unicode de longitud fija
    columna_nvarchar NVARCHAR(100),         -- Cadena Unicode de longitud variable
    columna_ntext NTEXT,                    -- Texto Unicode de longitud máxima

    -- Tipos numéricos exactos
    columna_bit BIT,                        -- Bit (0 o 1)
    columna_tinyint TINYINT,                -- Entero pequeño sin signo (0 a 255)
    columna_smallint SMALLINT,              -- Entero pequeño (-32,768 a 32,767)
    columna_int INT,                        -- Entero estándar (-2^31 a 2^31 - 1)
    columna_bigint BIGINT,                  -- Entero grande (-2^63 a 2^63 - 1)
    columna_decimal DECIMAL(10,2),          -- Decimal con precisión definida
    columna_numeric NUMERIC(10,2),          -- Numérico con precisión definida

    -- Tipos numéricos aproximados
    columna_float FLOAT,                    -- Punto flotante de precisión doble
    columna_real REAL,                      -- Punto flotante de precisión simple

    -- Tipos de fecha y hora
    columna_date DATE,                      -- Solo fecha
    columna_time TIME,                      -- Solo hora
    columna_datetime DATETIME,              -- Fecha y hora (hasta 1/300 segundo)
    columna_datetime2 DATETIME2,            -- Fecha y hora con más precisión
    columna_datetimeoffset DATETIMEOFFSET,  -- Fecha y hora con zona horaria

    -- Tipos binarios
    columna_binary BINARY(50),              -- Datos binarios de longitud fija
    columna_varbinary VARBINARY(100),       -- Datos binarios de longitud variable
    columna_image IMAGE,                    -- Datos binarios de longitud máxima

    -- Tipos especiales
    columna_uniqueidentifier UNIQUEIDENTIFIER, -- Identificador único global
    columna_xml XML,                        -- Datos XML
    columna_sql_variant SQL_VARIANT         -- Almacena valores de varios tipos
);

-- Tabla CIENMIL (100,000 registros)
CREATE TABLE cienmil (
    -- Las mismas columnas que diezmil
    columna_char CHAR(10),
    columna_varchar VARCHAR(100),
    columna_text TEXT,
    columna_nchar NCHAR(10),
    columna_nvarchar NVARCHAR(100),
    columna_ntext NTEXT,
    columna_bit BIT,
    columna_tinyint TINYINT,
    columna_smallint SMALLINT,
    columna_int INT,
    columna_bigint BIGINT,
    columna_decimal DECIMAL(10,2),
    columna_numeric NUMERIC(10,2),
    columna_float FLOAT,
    columna_real REAL,
    columna_date DATE,
    columna_time TIME,
    columna_datetime DATETIME,
    columna_datetime2 DATETIME2,
    columna_datetimeoffset DATETIMEOFFSET,
    columna_binary BINARY(50),
    columna_varbinary VARBINARY(100),
    columna_image IMAGE,
    columna_uniqueidentifier UNIQUEIDENTIFIER,
    columna_xml XML,
    columna_sql_variant SQL_VARIANT
);

-- Tabla QUINIENTOSMIL (500,000 registros)
CREATE TABLE quinientosmil (
    -- Las mismas columnas que diezmil y cienmil
    columna_char CHAR(10),
    columna_varchar VARCHAR(100),
    columna_text TEXT,
    columna_nchar NCHAR(10),
    columna_nvarchar NVARCHAR(100),
    columna_ntext NTEXT,
    columna_bit BIT,
    columna_tinyint TINYINT,
    columna_smallint SMALLINT,
    columna_int INT,
    columna_bigint BIGINT,
    columna_decimal DECIMAL(10,2),
    columna_numeric NUMERIC(10,2),
    columna_float FLOAT,
    columna_real REAL,
    columna_date DATE,
    columna_time TIME,
    columna_datetime DATETIME,
    columna_datetime2 DATETIME2,
    columna_datetimeoffset DATETIMEOFFSET,
    columna_binary BINARY(50),
    columna_varbinary VARBINARY(100),
    columna_image IMAGE,
    columna_uniqueidentifier UNIQUEIDENTIFIER,
    columna_xml XML,
    columna_sql_variant SQL_VARIANT
);