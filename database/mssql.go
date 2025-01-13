package database

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb" // Driver para Microsoft SQL Server
)

type MSSQL struct {
	DB       *sql.DB
	user     string
	password string
	database string
	host     string
	port     int
}

// NewMSSQL crea una nueva instancia de MSSQL
func NewMSSQL(user, password, database, host string, port int) *MSSQL {
	return &MSSQL{
		user:     user,
		password: password,
		database: database,
		host:     host,
		port:     port,
	}
}

// getConnection establece una conexión a la base de datos
func (m *MSSQL) GetConnection() error {
	if m.DB != nil {
		return nil // Ya existe una conexión
	}

	// dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s)",
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable&trustServerCertificate=true",
		m.user, m.password, m.host, m.port, m.database)

	DB, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("error al abrir la conexión: %w", err)
	}

	// Verificar la conexión
	if err := DB.Ping(); err != nil {
		return fmt.Errorf("error al verificar la conexión: %w", err)
	}

	m.DB = DB
	return nil
}

// getTables obtiene todas las tablas de la base de datos
func (m *MSSQL) GetTables() ([]string, error) {
	if m.DB == nil {
		return nil, fmt.Errorf("no hay una conexión activa")
	}

	query := `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'`
	rows, err := m.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error al obtener las tablas: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("error al escanear el resultado: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// closeConnection cierra la conexión a la base de datos
func (m *MSSQL) CloseConnection() error {
	if m.DB == nil {
		return nil // Ya está cerrada
	}

	if err := m.DB.Close(); err != nil {
		return fmt.Errorf("error al cerrar la conexión: %w", err)
	}

	m.DB = nil
	return nil
}

func (m *MSSQL) CopyDatabaseToPostgres(targetDB *Postgres) error {
	startTime := time.Now()
	tables, err := m.GetTables()
	if err != nil {
		return fmt.Errorf("error al obtener las tablas: %w", err)
	}

	fmt.Printf("\nIniciando migración de %d tablas\n", len(tables))

	// Verificar conexiones
	if m.DB == nil {
		return fmt.Errorf("no hay una conexión activa en la base de datos origen")
	}
	if targetDB.DB == nil {
		return fmt.Errorf("no hay una conexión activa en la base de datos destino")
	}

	// Iniciar transacción en la base de datos destino
	tx, err := targetDB.DB.Begin()
	if err != nil {
		return fmt.Errorf("error al iniciar la transacción: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Procesar cada tabla
	for _, tableName := range tables {
		// Obtener estructura de la tabla
		// Obtener estructura de la tabla con longitud de campos
		columnsQuery := `
			SELECT 
				COLUMN_NAME,
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
			ORDER BY ORDINAL_POSITION`

		rows, err := m.DB.Query(columnsQuery, tableName)
		if err != nil {
			return fmt.Errorf("error al obtener la estructura de la tabla %s: %w", tableName, err)
		}
		defer rows.Close()

		var columns []string
		var dataTypes []string
		for rows.Next() {
			var colName, dataType string
			if err := rows.Scan(&colName, &dataType); err != nil {
				return fmt.Errorf("error al escanear columna: %w", err)
			}
			columns = append(columns, colName)
			dataTypes = append(dataTypes, m.convertDataType(dataType))
		}

		// En CopyDatabaseToPostgres
		safeTableName := strings.ReplaceAll(tableName, "-", "_")
		dropQuery := fmt.Sprintf(`DROP TABLE IF EXISTS "%s" CASCADE`, safeTableName)
		if _, err := tx.Exec(dropQuery); err != nil {
			return fmt.Errorf("error al eliminar tabla existente %s: %w", tableName, err)
		}

		// Crear tabla en PostgreSQL
		createQuery := m.generateCreateTableQuery(tableName, columns, dataTypes)
		if _, err := tx.Exec(createQuery); err != nil {
			return fmt.Errorf("error al crear tabla %s: %w", tableName, err)
		}

		// Copiar datos
		if err := m.copyTableData(tableName, columns, tx); err != nil {
			return fmt.Errorf("error al copiar datos de la tabla %s: %w", tableName, err)
		}
	}

	// Confirmar transacción
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error al confirmar la transacción: %w", err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\nMigración completada en %v\n", elapsed.Round(time.Second))
	return nil
}

func (m *MSSQL) convertDataType(sqlServerType string) string {
	// Obtener el tipo base y la longitud
	baseType := strings.ToLower(sqlServerType)
	length := ""
	if idx := strings.Index(baseType, "("); idx != -1 {
		length = baseType[idx:]
		baseType = baseType[:idx]

		// Validar la longitud para cualquier tipo de dato
		if strings.Contains(length, "-1") ||
			strings.Contains(length, "max") ||
			strings.Contains(length, "MAX") {
			// Para tipos que pueden ser text
			switch baseType {
			case "varchar", "nvarchar", "char", "nchar", "text", "ntext":
				return "text"
			case "varbinary", "binary", "image":
				return "bytea"
			default:
				// Para otros tipos, usar la versión sin longitud
				return m.getBaseType(baseType)
			}
		}

		// Si la longitud parece válida, mantenerla
		if _, err := strconv.Atoi(strings.Trim(length, "()")); err == nil {
			return m.getBaseType(baseType) + length
		}
	}

	return m.getBaseType(baseType)
}

func (m *MSSQL) getBaseType(sqlServerType string) string {
	switch sqlServerType {
	case "varchar", "nvarchar", "text", "ntext":
		return "text"
	case "char", "nchar":
		return "char"
	case "int":
		return "integer"
	case "bigint":
		return "bigint"
	case "smallint":
		return "smallint"
	case "tinyint":
		return "smallint"
	case "decimal", "numeric":
		return "numeric"
	case "float":
		return "double precision"
	case "real":
		return "real"
	case "datetime", "datetime2", "smalldatetime":
		return "timestamp"
	case "date":
		return "date"
	case "time":
		return "time"
	case "bit":
		return "boolean"
	case "binary", "varbinary", "image":
		return "bytea"
	case "uniqueidentifier":
		return "uuid"
	case "xml":
		return "xml"
	case "money", "smallmoney":
		return "numeric"
	default:
		return "text"
	}
}

func (m *MSSQL) generateCreateTableQuery(tableName string, columns []string, dataTypes []string) string {
	var columnDefs []string
	for i := range columns {
		// Escapar nombres de columnas con comillas dobles
		columnName := strings.ReplaceAll(columns[i], "-", "_")
		columnDefs = append(columnDefs, fmt.Sprintf(`"%s" %s`, columnName, dataTypes[i]))
	}
	query := fmt.Sprintf(`CREATE TABLE "%s" (%s)`, tableName, strings.Join(columnDefs, ", "))
	fmt.Printf("Query de creación de tabla: %s\n", query)
	// Escapar nombre de tabla con comillas dobles
	return query
}

func (m *MSSQL) copyTableData(tableName string, columns []string, tx *sql.Tx) error {
	// Obtener el total de filas para calcular el progreso
	var totalRows int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	if err := m.DB.QueryRow(countQuery).Scan(&totalRows); err != nil {
		return fmt.Errorf("error al contar filas: %w", err)
	}

	// Construir consulta para seleccionar datos
	selectQuery := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), tableName)
	rows, err := m.DB.Query(selectQuery)
	if err != nil {
		return fmt.Errorf("error al seleccionar datos: %w", err)
	}
	defer rows.Close()

	const batchSize = 1000
	batch := make([][]interface{}, 0, batchSize)

	// Variables para el seguimiento del progreso
	processedRows := 0
	lastProgressPrinted := 0
	startTime := time.Now()

	// Preparar valores para escaneo
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	fmt.Printf("\nIniciando transferencia de tabla %s (%d filas totales)\n", tableName, totalRows)

	// Procesar filas en lotes
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("error al escanear fila: %w", err)
		}

		rowValues := make([]interface{}, len(values))
		for i, v := range values {
			rowValues[i] = v
		}
		batch = append(batch, rowValues)
		processedRows++

		// Mostrar progreso cada 5%
		progress := (processedRows * 100) / totalRows
		if progress%5 == 0 && progress != lastProgressPrinted {
			elapsed := time.Since(startTime)
			rate := float64(processedRows) / elapsed.Seconds()
			remaining := time.Duration(float64(totalRows-processedRows)/rate) * time.Second

			fmt.Printf("Progreso %s: %d%% (%d/%d filas) - %.0f filas/seg - Tiempo restante: %v\n",
				tableName, progress, processedRows, totalRows, rate, remaining.Round(time.Second))
			lastProgressPrinted = progress
		}

		if len(batch) >= batchSize {
			if err := m.executeBatchInsert(tx, tableName, columns, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Insertar el último lote
	if len(batch) > 0 {
		if err := m.executeBatchInsert(tx, tableName, columns, batch); err != nil {
			return err
		}
	}

	elapsed := time.Since(startTime)
	fmt.Printf("Completada la transferencia de %s: %d filas en %v\n",
		tableName, totalRows, elapsed.Round(time.Second))

	return nil
}

func (m *MSSQL) executeBatchInsert(tx *sql.Tx, tableName string, columns []string, batch [][]interface{}) error {
	valueStrings := make([]string, 0, len(batch))
	values := make([]interface{}, 0, len(batch)*len(columns))

	// Escapar nombres de columnas
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		safeColumnName := strings.ReplaceAll(col, "-", "_")
		quotedColumns[i] = fmt.Sprintf(`"%s"`, safeColumnName)
	}

	for i, row := range batch {
		placeholders := make([]string, len(columns))
		for j := range columns {
			placeholders[j] = fmt.Sprintf("$%d", i*len(columns)+j+1)
		}
		valueStrings = append(valueStrings, "("+strings.Join(placeholders, ", ")+")")
		values = append(values, row...)
	}

	safeTableName := strings.ReplaceAll(tableName, "-", "_")
	query := fmt.Sprintf(
		`INSERT INTO "%s" (%s) VALUES %s`,
		safeTableName,
		strings.Join(quotedColumns, ", "),
		strings.Join(valueStrings, ", "),
	)

	if _, err := tx.Exec(query, values...); err != nil {
		return fmt.Errorf("error al insertar lote: %w", err)
	}

	return nil
}
