SET NOCOUNT ON;

DECLARE @i INT = 1;

WHILE @i <= 10000
BEGIN
    INSERT INTO diezmil (
        columna_char,
        columna_varchar,
        columna_text,
        columna_nchar,
        columna_nvarchar,
        columna_ntext,
        columna_bit,
        columna_tinyint,
        columna_smallint,
        columna_int,
        columna_bigint,
        columna_decimal,
        columna_numeric,
        columna_float,
        columna_real,
        columna_date,
        columna_time,
        columna_datetime,
        columna_datetime2,
        columna_datetimeoffset,
        columna_binary,
        columna_varbinary,
        columna_image,
        columna_uniqueidentifier,
        columna_xml,
        columna_sql_variant
    )
    VALUES (
        LEFT(NEWID(), 10), -- columna_char (10 caracteres)
        LEFT(REPLICATE(CAST(NEWID() AS VARCHAR(36)), 3), 100), -- columna_varchar
        'Texto aleatorio ' + CAST(@i AS VARCHAR), -- columna_text
        LEFT(N'ABCDEFGHIJ', 10), -- columna_nchar
        N'Texto aleatorio ' + CAST(@i AS NVARCHAR), -- columna_nvarchar
        N'Texto NTEXT ' + CAST(@i AS NVARCHAR), -- columna_ntext
        CAST(RAND() * 2 AS BIT), -- columna_bit
        CAST(RAND() * 255 AS TINYINT), -- columna_tinyint
        CAST(RAND() * 32767 AS SMALLINT), -- columna_smallint
        CAST(RAND() * 2147483647 AS INT), -- columna_int
        CAST(RAND() * 9223372036854775807 AS BIGINT), -- columna_bigint
        CAST(RAND() * 100000 / 100 AS DECIMAL(10,2)), -- columna_decimal
        CAST(RAND() * 100000 / 100 AS NUMERIC(10,2)), -- columna_numeric
        RAND() * 1000000, -- columna_float
        RAND() * 1000000, -- columna_real
        DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 36525, '1900-01-01'), -- columna_date
        CAST(DATEADD(SECOND, ABS(CHECKSUM(NEWID())) % 86400, '00:00:00') AS TIME), -- columna_time
        DATEADD(SECOND, ABS(CHECKSUM(NEWID())) % 31557600, '2000-01-01'), -- columna_datetime
        SYSDATETIME(), -- columna_datetime2
        SWITCHOFFSET(SYSDATETIMEOFFSET(), DATEPART(TZOFFSET, SYSDATETIMEOFFSET())), -- columna_datetimeoffset
        CAST(NEWID() AS BINARY(50)), -- columna_binary
        CAST(NEWID() AS VARBINARY(100)), -- columna_varbinary
        CAST(NEWID() AS VARBINARY(100)), -- columna_image (simulación)
        NEWID(), -- columna_uniqueidentifier
        '<root><value>' + CAST(@i AS NVARCHAR) + '</value></root>', -- columna_xml
        'Texto SQL_VARIANT ' + CAST(@i AS NVARCHAR) -- columna_sql_variant
    );

    SET @i += 1;
END;

