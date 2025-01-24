package database

import (
	"database/sql"
	"fmt"
	"strings"
)

type Table struct {
	Name      string
	Columns   []string
	DataTypes []string
	Data      *sql.Rows
	Size      string
	Dimension string
}

func NewTable(schema string, tableName string, DB *sql.DB) (*Table, error) {
	t := &Table{
		Name: tableName,
	}

	var err error
	if t.Columns, err = t.getColumns(schema, DB); err != nil {
		return nil, err
	}
	if t.DataTypes, err = t.getDataTypes(schema, DB); err != nil {
		return nil, err
	}
	if t.Data, err = t.getData(schema, DB); err != nil {
		return nil, err
	}
	if t.Size, err = t.getSize(schema, DB); err != nil {
		return nil, err
	}
	if t.Dimension, err = t.getDimensions(schema, DB); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Table) getColumns(schema string, DB *sql.DB) ([]string, error) {
	columnsQuery := `
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = @p1 
        AND TABLE_SCHEMA = @p2
        ORDER BY ORDINAL_POSITION`

	rows, err := DB.Query(columnsQuery, t.Name, schema)
	if err != nil {
		return nil, fmt.Errorf("error al obtener columnas: %w", err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("error al escanear columna: %w", err)
		}
		columns = append(columns, colName)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error al iterar sobre las columnas: %w", err)
	}

	return columns, nil
}

// Obtiene los tipos de datos de las columnas
func (t *Table) getDataTypes(schema string, DB *sql.DB) ([]string, error) {
	dataTypesQuery := `
        SELECT 
            DATA_TYPE + 
            CASE 
                WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL 
                    THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
                WHEN NUMERIC_PRECISION IS NOT NULL AND NUMERIC_SCALE IS NOT NULL
                    THEN '(' + CAST(NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(NUMERIC_SCALE AS VARCHAR) + ')'
                ELSE ''
            END as DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = @p1 
        AND TABLE_SCHEMA = @p2
        ORDER BY ORDINAL_POSITION`

	rows, err := DB.Query(dataTypesQuery, t.Name, schema)
	if err != nil {
		return nil, fmt.Errorf("error al obtener tipos de datos: %w", err)
	}
	defer rows.Close()

	var dataTypes []string
	for rows.Next() {
		var dataType string
		if err := rows.Scan(&dataType); err != nil {
			return nil, fmt.Errorf("error al escanear tipo de dato: %w", err)
		}
		dataTypes = append(dataTypes, dataType)
	}
	return dataTypes, nil
}

// Obtiene los datos de la tabla
func (t *Table) getData(schema string, DB *sql.DB) (*sql.Rows, error) {
	query := fmt.Sprintf("SELECT %s FROM %s.%s",
		strings.Join(t.Columns, ", "),
		schema,
		t.Name)

	rows, err := DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error al obtener datos: %w", err)
	}
	return rows, nil
}

// Obtiene el tamaño de la tabla en MB
func (t *Table) getSize(schema string, DB *sql.DB) (string, error) {
	sizeQuery := `
        SELECT 
            CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS DECIMAL(36,2))
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
        INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
        WHERE t.NAME = @p1 AND s.name = @p2
        GROUP BY t.Name`

	var size float64
	if err := DB.QueryRow(sizeQuery, t.Name, schema).Scan(&size); err != nil {
		return "", fmt.Errorf("error al obtener tamaño de tabla: %w", err)
	}
	sizeStr := fmt.Sprintf("%.2f MB", size)
	return sizeStr, nil
}

// Obtiene las dimensiones de la tabla (filas x columnas)
func (t *Table) getDimensions(schema string, DB *sql.DB) (string, error) {
	// Obtener cantidad de filas
	var rows int
	rowCountQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, t.Name)
	if err := DB.QueryRow(rowCountQuery).Scan(&rows); err != nil {
		return "", fmt.Errorf("error al obtener cantidad de filas: %w", err)
	}

	// La cantidad de columnas es el largo del slice Columns
	columns := len(t.Columns)

	return fmt.Sprintf("%d rows x %d columns", rows, columns), nil
}
