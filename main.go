package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	db "github.com/leonelperez/rivadavia/database"
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

	// Imprimir variables de entorno de MSSQL
	fmt.Println("MSSQL User:", mssqlUser)
	fmt.Println("MSSQL Database:", mssqlDatabase)
	fmt.Println("MSSQL Host:", mssqlHost)
	fmt.Println("MSSQL Port:", mssqlPort)

	dbDatalitica := db.NewPostgres(postgresUser, postgresPassword, postgresDatabase, postgresHost, postgresPort)
	dbRivadavia := db.NewMSSQL(mssqlUser, mssqlPassword, mssqlDatabase, mssqlHost, mssqlPort)

	//CONEXION RIVADAVIA
	if err := dbRivadavia.GetConnection(); err != nil {
		fmt.Println("Error al conectar:", err)
		return
	}

	tablesRivadavia, err := dbRivadavia.GetTables()
	if err != nil {
		fmt.Println("Error al obtener tablas:", err)
		return
	}
	fmt.Println("------------------------------")
	fmt.Println("      TABLAS RIVADAVIA      ")
	fmt.Println("------------------------------")
	for i, table := range tablesRivadavia {
		fmt.Printf("Tabla %d: %s\n", i+1, table)
	}

	//CONEXION DATALITICA
	if err := dbDatalitica.GetConnection(); err != nil {
		fmt.Println("Error al conectar:", err)
		return
	}

	tablesDatalitica, err := dbDatalitica.GetTables()
	if err != nil {
		fmt.Println("Error al obtener tablas:", err)
		return
	}

	fmt.Println("------------------------------")
	fmt.Println("      TABLAS DATALITICA      ")
	fmt.Println("------------------------------")
	for i, table := range tablesDatalitica {
		fmt.Printf("Tabla %d: %s\n", i+1, table)
	}

	// if err := db.MigrateData(dbRivadavia.DB, dbDatalitica.DB, tablesRivadavia); err != nil {
	// 	fmt.Printf("Error en la migración: %v\n", err)
	// 	return
	// }
	if err := dbRivadavia.CopyDatabaseToPostgres(dbDatalitica); err != nil {
		fmt.Printf("Error en la copia de la base de datos: %v\n", err)
		return
	}

	// Cerrar conexiones al finalizar
	defer dbRivadavia.CloseConnection()
	defer dbDatalitica.CloseConnection()
}
