package database

import (
	"database/sql"
	"fmt"
	"strings"
)

func MigrateData(source *sql.DB, dest *sql.DB, tables []string) error {
	for _, table := range tables {
		// Obtener estructura de la tabla
		columns, err := getTableColumns(source, table)
		if err != nil {
			return fmt.Errorf("error obteniendo columnas de %s: %v", table, err)
		}

		// Crear tabla en PostgreSQL
		createStmt := generateCreateStatement(table, columns)
		_, err = dest.Exec(createStmt)
		if err != nil {
			return fmt.Errorf("error creando tabla %s: %v", table, err)
		}

		// Copiar datos
		err = copyTableData(source, dest, table, columns)
		if err != nil {
			return fmt.Errorf("error copiando datos de %s: %v", table, err)
		}
	}
	return nil
}

func getTableColumns(db *sql.DB, table string) ([]Column, error) {
	query := `
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = @p1
        ORDER BY ORDINAL_POSITION`

	rows, err := db.Query(query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var maxLength sql.NullInt64
		err := rows.Scan(&col.Name, &col.Type, &maxLength)
		if err != nil {
			return nil, err
		}
		col.Length = maxLength.Int64
		col.Type = convertDataType(col.Type, maxLength.Int64)
		columns = append(columns, col)
	}
	return columns, nil
}

func convertDataType(sqlType string, length int64) string {
	switch strings.ToLower(sqlType) {
	case "int":
		return "integer"
	case "bigint":
		return "bigint"
	case "smallint":
		return "smallint"
	case "tinyint":
		return "smallint" // TINYINT en MSSQL no tiene un equivalente exacto en PostgreSQL
	case "bit":
		return "boolean"
	case "decimal", "numeric":
		if length > 0 {
			return fmt.Sprintf("decimal(%d,0)", length) // Puedes ajustar la escala según sea necesario
		}
		return "decimal"
	case "float":
		return "double precision"
	case "real":
		return "real"
	case "money":
		return "numeric(19,4)"
	case "smallmoney":
		return "numeric(10,4)"
	case "char":
		if length <= 0 {
			return "char(1)"
		}
		return fmt.Sprintf("char(%d)", length)
	case "varchar", "nvarchar":
		if length == -1 {
			return "text"
		}
		return fmt.Sprintf("varchar(%d)", length)
	case "text", "ntext":
		return "text"
	case "binary", "varbinary", "image":
		return "bytea"
	case "uniqueidentifier":
		return "uuid"
	case "datetime", "datetime2", "smalldatetime":
		return "timestamp"
	case "date":
		return "date"
	case "time":
		return "time"
	case "datetimeoffset":
		return "timestamp with time zone"
	case "xml":
		return "xml"
	case "geography":
		return "geography" // Requiere PostGIS
	case "geometry":
		return "geometry" // Requiere PostGIS
	default:
		return sqlType // Devuelve el tipo original si no se encuentra una equivalencia
	}
}

func generateCreateStatement(table string, columns []Column) string {
	var columnDefs []string
	for _, col := range columns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", col.Name, col.Type))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		table, strings.Join(columnDefs, ", "))
}

func copyTableData(source *sql.DB, dest *sql.DB, table string, columns []Column) error {
	// Construir query para seleccionar datos
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}

	selectQuery := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(columnNames, ", "), table)

	rows, err := source.Query(selectQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Preparar statement para inserción
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))

	stmt, err := dest.Prepare(insertQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Copiar datos en batches
	batch := make([]interface{}, len(columns))
	for rows.Next() {
		for i := range columns {
			batch[i] = new(interface{})
		}
		err = rows.Scan(batch...)
		if err != nil {
			return err
		}
		_, err = stmt.Exec(batch...)
		if err != nil {
			return err
		}
	}
	return nil
}

type Column struct {
	Name   string
	Type   string
	Length int64
}
