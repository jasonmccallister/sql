package main

import (
	"context"
	"dagger/sql/internal/dagger"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// ColumnDetails represents the details of a column in a database table
type ColumnDetails struct {
	Name       string
	DataType   string
	IsNullable bool
}

type Sql struct {
	Conn *dagger.Secret // +private
}

func New(conn *dagger.Secret) *Sql { return &Sql{Conn: conn} }

func (m *Sql) connect() (*sql.DB, string, string, error) {
	c, err := m.Conn.Plaintext(context.Background())
	if err != nil {
		return nil, "", "", fmt.Errorf("error getting plaintext connection: %w", err)
	}

	var (
		db       *sql.DB
		dbType   string
		database string
	)
	conn := strings.ToLower(c)
	switch {
	case strings.HasPrefix(conn, "postgres://"), strings.HasPrefix(conn, "postgresql://"), strings.Contains(conn, "user=") && strings.Contains(conn, "dbname="):
		d, err := sql.Open("pgx", c)
		if err != nil {
			return nil, "", "", fmt.Errorf("error opening database connection: %w", err)
		}
		db = d
		dbType = "postgres"
		u, err := url.Parse(c)
		if err != nil {
			return nil, "", "", fmt.Errorf("error parsing connection string: %w", err)
		}

		database = strings.TrimPrefix(u.Path, "/")
	case strings.HasPrefix(conn, "mysql://"), strings.Contains(conn, "@tcp("), strings.Contains(conn, "user:") && strings.Contains(conn, "@/"):
		d, err := sql.Open("mysql", c)
		if err != nil {
			return nil, "", "", fmt.Errorf("error opening database connection: %w", err)
		}
		db = d
		dbType = "mysql"
		parts := strings.SplitN(conn, "/", 2)
		if len(parts) < 2 {
			return nil, "", "", fmt.Errorf("unable to determine database name from connection string: %s", c)
		}

		pathParts := strings.SplitN(parts[1], "?", 2)
		dbName := pathParts[0]
		if dbName == "" {
			return nil, "", "", fmt.Errorf("invalid DSN: missing database name")
		}
		database = dbName
	default:
		return nil, "", "", fmt.Errorf("unable to determine database type from connection string: %s", c)
	}
	if database == "" {
		return nil, "", "", fmt.Errorf("unable to determine database name from connection string: %s", c)
	}

	return db, dbType, database, nil
}

// List the tables in a database and return the names of the tables
func (m *Sql) ListTables(
	// +default="public"
	schema string,
) ([]string, error) {
	db, dbType, database, err := m.connect()
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_catalog = '%s'", schema, database)
	if dbType == "mysql" {
		query = fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", database)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying tables: %w", err)
	}
	defer rows.Close()

	tables := []string{}
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return tables, nil
}

// List the columns in a table and and return the names
func (m *Sql) ListColumns(table string) ([]string, error) {
	db, dbType, database, err := m.connect()
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s' AND table_catalog = '%s'", table, database)
	if dbType == "mysql" {
		query = fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", table)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %w", err)
	}
	defer rows.Close()

	columns := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		columns = append(columns, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return columns, nil
}

// List the details for a specific column in a table
func (m *Sql) ListColumnDetails(table, column string) (*ColumnDetails, error) {
	db, dbType, database, err := m.connect()
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_catalog = '%s' AND column_name = '%s'", table, database, column)
	if dbType == "mysql" {
		query = fmt.Sprintf("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND column_name = '%s'", table, column)
	}

	details := &ColumnDetails{}
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var isNullable string
		if err := rows.Scan(&details.Name, &details.DataType, &isNullable); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		details.IsNullable = isNullable == "YES"
		break // We only need the first row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return details, nil
}

// Query the database and return the results in comma-separated format
func (m *Sql) RunQuery(query string) (string, error) {
	db, _, _, err := m.connect()
	if err != nil {
		return "", fmt.Errorf("error opening database connection: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("error querying database: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("error getting columns: %w", err)
	}

	var results []string
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return "", fmt.Errorf("error scanning row: %w", err)
		}
		var row []string
		for _, value := range values {
			row = append(row, fmt.Sprintf("%v", value))
		}
		results = append(results, strings.Join(row, ","))
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating rows: %w", err)
	}

	if len(results) == 0 {
		return "", fmt.Errorf("no results found")
	}

	return strings.Join(results, "\n"), nil
}
