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

const (
	MySQLListColumnsFmt     = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '%s'"
	MySQLColumnDetailFmt    = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND column_name = '%s'"
	MySQLListTablesFmt      = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'"
	PostgresColumnDetailFmt = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_catalog = '%s' AND column_name = '%s'"
	PostgresListColumnsFmt  = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_catalog = '%s'"
	PostgresListTablesFmt   = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_catalog = '%s'"
)

// ColumnDetails represents the details of a column in a database table
type ColumnDetails struct {
	Name       string
	DataType   string
	IsNullable bool
}

// TableDetails represents the details of a table in a database
type TableDetails struct {
	Name    string
	Columns []ColumnDetails
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

		strings.TrimPrefix(u.Path, "/")
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

// List the tables in a database in comma-separated format
func (m *Sql) ListTables(
	// +default="public"
	schema string,
) (*TableDetails, error) {
	db, dbType, database, err := m.connect()
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf(PostgresListTablesFmt, schema, database)
	if dbType == "mysql" {
		query = fmt.Sprintf(MySQLListTablesFmt, database)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying tables: %w", err)
	}
	defer rows.Close()

	tables := &TableDetails{}
	for rows.Next() {
		t := TableDetails{}

		if err := rows.Scan(&t.Name); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	if tables == nil {
		return nil, fmt.Errorf("no tables found, you might be in the wrong database or schema based on the connection")
	}

	return tables, nil
}

// List the columns in a table and return the details for the column
func (m *Sql) ListColumns(table string) ([]ColumnDetails, error) {
	db, dbType, database, err := m.connect()
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf(PostgresListColumnsFmt, table, database)
	if dbType == "mysql" {
		query = fmt.Sprintf(MySQLListColumnsFmt, table)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %w", err)
	}
	defer rows.Close()

	columns := []ColumnDetails{}
	for rows.Next() {
		column := ColumnDetails{}
		var isNullable string
		if err := rows.Scan(&column.Name, &column.DataType, &isNullable); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		column.IsNullable = isNullable == "YES"
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found, you might be in the wrong database or table based on the connection")
	}

	return columns, nil
}

// List details on a specific column for a table in the database
func (m *Sql) ListColumnDetails(table, column string) (*ColumnDetails, error) {
	db, dbType, database, err := m.connect()
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf(PostgresColumnDetailFmt, table, database, column)
	if dbType == "mysql" {
		query = fmt.Sprintf(MySQLColumnDetailFmt, table, column)
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
