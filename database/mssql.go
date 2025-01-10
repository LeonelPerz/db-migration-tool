package database

import (
	"database/sql"
	"fmt"

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
