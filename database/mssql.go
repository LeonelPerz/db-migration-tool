package database

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/denisenkom/go-mssqldb" // Driver para Microsoft SQL Server
)

type MSSQL struct {
	DB       *sql.DB
	user     string
	password string
	database string
	host     string
	port     int
	Schemes  []*Schema
}

type Schema struct {
	Name   string
	Tables []*Table
}

func NewMSSQL(user, password, database, host string, port int) *MSSQL {
	return &MSSQL{
		user:     user,
		password: password,
		database: database,
		host:     host,
		port:     port,
	}
}

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

func (m *MSSQL) getAllTablesNames() ([]string, error) {
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

func (m *MSSQL) GetTablesBySchema(schema string) error {
	if m.DB == nil {
		return fmt.Errorf("no hay una conexión activa")
	}

	query := `
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE' 
        AND TABLE_SCHEMA = @schema`

	rows, err := m.DB.Query(query, sql.Named("schema", schema))
	if err != nil {
		return fmt.Errorf("error al obtener las tablas del esquema %s: %w", schema, err)
	}
	defer rows.Close()

	tables := make([]*Table, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("error al escanear el resultado: %w", err)
		}

		table, err := NewTable(schema, tableName, m.DB)
		if err != nil {
			return fmt.Errorf("error al crear la tabla %s: %w", tableName, err)
		}
		tables = append(tables, table)
	}

	// Buscar el esquema existente o crear uno nuevo
	var currentSchema *Schema
	for _, s := range m.Schemes {
		if s.Name == schema {
			currentSchema = s
			break
		}
	}

	if currentSchema == nil {
		// Si no existe el esquema, crear uno nuevo
		currentSchema = &Schema{
			Name: schema,
		}
		m.Schemes = append(m.Schemes, currentSchema)
	}

	// Actualizar las tablas del esquema
	currentSchema.Tables = tables

	return nil
}

func (m *MSSQL) GetAllTables() error {
	if m.DB == nil {
		return fmt.Errorf("no hay una conexión activa a la base de datos")
	}

	// Obtener los nombres de los esquemas
	schemas, err := m.GetSchemas()
	if err != nil {
		return fmt.Errorf("error al obtener los nombres de los esquemas: %w", err)
	}

	// Inicializar el slice de esquemas
	m.Schemes = make([]*Schema, 0, len(schemas))

	// Crear un Schema por cada nombre y agregarlo a la lista
	for _, schemaName := range schemas {
		err := m.GetTablesBySchema(schemaName)
		if err != nil {
			return fmt.Errorf("error al obtener las tablas del esquema %s: %w", schemaName, err)
		}
		// m.Schemes = append(m.Schemes, schema)
	}

	return nil
}

func (m *MSSQL) GetSchemas() ([]string, error) {
	if m.DB == nil {
		return nil, fmt.Errorf("no hay una conexión activa a la base de datos")
	}

	query := `
        SELECT DISTINCT 
            SCHEMA_NAME 
        FROM INFORMATION_SCHEMA.SCHEMATA 
        WHERE SCHEMA_NAME NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 
            'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 
            'db_denydatareader', 'db_denydatawriter')
        ORDER BY SCHEMA_NAME`

	rows, err := m.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error al consultar esquemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("error al escanear esquema: %w", err)
		}
		schemas = append(schemas, schema)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error al iterar sobre los esquemas: %w", err)
	}

	return schemas, nil
}

func (m *MSSQL) ShowSchemasAndTables() error {
	if err := m.GetAllTables(); err != nil {
		return fmt.Errorf("error al cargar los esquemas y tablas: %w", err)
	}

	fmt.Println("\nEsquemas y sus tablas:")
	fmt.Println("=======================")

	for _, schema := range m.Schemes {
		fmt.Printf("\nEsquema: %s\n", schema.Name)
		fmt.Printf("------------------\n")

		for _, tabla := range schema.Tables {
			fmt.Printf("- Tabla: %s\n", tabla.Name)
			fmt.Printf("  Tamaño: %s\n", tabla.Size)
			fmt.Printf("  Dimensiones: %s\n", tabla.Dimension)
			fmt.Printf("  Columnas: %v\n", strings.Join(tabla.Columns, ", "))
			fmt.Printf("  Tipos: %v\n\n", strings.Join(tabla.DataTypes, ", "))
		}
	}

	return nil
}

// func (m *MSSQL) CopyAllTablesToPostgres(pgDb *Postgres) error {
// 	if m.DB == nil {
// 		return fmt.Errorf("no hay una conexión activa en la base de datos origen")
// 	}
// 	if pgDb.DB == nil {
// 		return fmt.Errorf("no hay una conexión activa en la base de datos destino")
// 	}

// 	if m.Tables == nil {
// 		if err := m.GetAllTables(); err != nil {
// 			return fmt.Errorf("Error obteniendo tablas: %w", err)
// 		}
// 	}

// 	tx, err := pgDb.DB.Begin()
// 	if err != nil {
// 		return fmt.Errorf("error al iniciar la transacción: %w", err)
// 	}
// 	defer func() {
// 		if err != nil {
// 			tx.Rollback()
// 		}
// 	}()
// }
