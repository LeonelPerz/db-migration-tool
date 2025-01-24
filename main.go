package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	db "github.com/leonelperez/db-migration-tool/database"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error al cargar el archivo .env")
		return
	}
	// Read environment variables for PostgreSQL
	postgresUser := os.Getenv("POSTGRES_USER")
	postgresPassword := os.Getenv("POSTGRES_PASSWORD")
	postgresDatabase := os.Getenv("POSTGRES_DB")
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresPortStr := os.Getenv("POSTGRES_PORT")
	postgresPort, err := strconv.Atoi(postgresPortStr)
	if err != nil {
		fmt.Println("error al obtener la variable de entorno puerto:", err)
		os.Exit(1)
	}

	// Imprimir variables de entorno de PostgreSQL
	fmt.Println("PostgreSQL User:", postgresUser)
	fmt.Println("PostgreSQL Database:", postgresDatabase)
	fmt.Println("PostgreSQL Host:", postgresHost)
	fmt.Println("PostgreSQL Port:", postgresPort)

	// Read environment variables for MSSQL
	mssqlUser := os.Getenv("MSSQL_USER")
	mssqlPassword := os.Getenv("MSSQL_PASSWORD")
	mssqlDatabase := os.Getenv("MSSQL_DB")
	mssqlHost := os.Getenv("MSSQL_HOST")
	mssqlPortStr := os.Getenv("MSSQL_PORT")
	mssqlPort, err := strconv.Atoi(mssqlPortStr)
	if err != nil {
		fmt.Println("error al obtener la variable de entorno puerto:", err)
		os.Exit(1)
	}

	fmt.Println("MSSQL User:", mssqlUser)
	fmt.Println("MSSQL Password:", mssqlPassword)
	fmt.Println("MSSQL Database:", mssqlDatabase)
	fmt.Println("MSSQL Host:", mssqlHost)
	fmt.Println("MSSQL Port:", mssqlPort)

	pg := db.NewPostgres(postgresUser, postgresPassword, postgresDatabase, postgresHost, postgresPort)
	mssql := db.NewMSSQL(mssqlUser, mssqlPassword, mssqlDatabase, mssqlHost, mssqlPort)

	defer mssql.CloseConnection()
	defer pg.CloseConnection()
	if err := mssql.GetConnection(); err != nil {
		fmt.Println("Error al conectar:", err)
		return
	}
	if err := pg.GetConnection(); err != nil {
		fmt.Println("Error al conectar:", err)
		return
	}

	if err := mssql.ShowSchemasAndTables(); err != nil {
		fmt.Println("Error al mostrar esquemas y tablas:", err)
		return
	}

}
