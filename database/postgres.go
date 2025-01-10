package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // Importa el driver de PostgreSQL
)

type Postgres struct {
	DB       *sql.DB
	user     string
	password string
	database string
	host     string
	port     int
}

// NewPostgres crea una nueva instancia de Postgres
func NewPostgres(user, password, database, host string, port int) *Postgres {
	return &Postgres{
		user:     user,
		password: password,
		database: database,
		host:     host,
		port:     port,
	}
}

// GetConnection establece una conexión a la base de datos
func (p *Postgres) GetConnection() error {
	if p.DB != nil {
		return nil // Ya existe una conexión
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		p.host, p.port, p.user, p.password, p.database)

	DB, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("error al abrir la conexión: %w", err)
	}

	// Verificar la conexión
	if err := DB.Ping(); err != nil {
		return fmt.Errorf("error al verificar la conexión: %w", err)
	}

	p.DB = DB
	return nil
}

// GetTables obtiene todas las tablas de la base de datos
func (p *Postgres) GetTables() ([]string, error) {
	if p.DB == nil {
		return nil, fmt.Errorf("no hay una conexión activa")
	}

	query := `SELECT table_name FROM information_schema.tables WHERE table_schema='public'`
	rows, err := p.DB.Query(query)
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

// CloseConnection cierra la conexión a la base de datos
func (p *Postgres) CloseConnection() error {
	if p.DB == nil {
		return nil // Ya está cerrada
	}

	if err := p.DB.Close(); err != nil {
		return fmt.Errorf("error al cerrar la conexión: %w", err)
	}

	p.DB = nil
	return nil
}
