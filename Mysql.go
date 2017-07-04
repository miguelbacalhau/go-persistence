// Package persistence provides strucures and functions to
// ease the building and execution of queries for SQL
// This package should be similar and compatible with
// the native database/sql
// TODO missing types
package persistence

import (
	"bytes"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

const database = "mysql"

// Mysql structure stores the database information needed
// to establish a connection
type Mysql struct {
	username string
	database string
	//TODO host and port => user:password@tcp(127.0.0.1:3306)/hello
}

// Opens a connections to the database
// The connection should be manually closed after usage
func (mysql *Mysql) Open() *sql.DB {
	var buffer bytes.Buffer

	buffer.WriteString(mysql.username)
	buffer.WriteString(":@/")
	buffer.WriteString(mysql.database)

	db, err := sql.Open(database, buffer.String())
	if err != nil {
		panic(err)
	}

	return db
}

// Executes a query that does not return a result
func (mysql *Mysql) Exec(query string, values ...interface{}) sql.Result {
	db := mysql.Open()
	defer db.Close()

	stmt, err := db.Prepare(query)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	result, errr := stmt.Exec(values...)
	if errr != nil {
		panic(errr)
	}

	return result
}

// Executes a query that returns rows
func (mysql *Mysql) Query(query string, values ...interface{}) *sql.Rows {
	db := mysql.Open()
	defer db.Close()

	stmt, err := db.Prepare(query)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	result, err := stmt.Query(values...)
	if err != nil {
		panic(err)
	}

	return result
}

// Mysql constructor
func NewMysql(username string, database string) *Mysql {
	return &Mysql{
		username: username,
		database: database,
	}
}

// Table strucure stroes the information relative the a
// database table
type Table struct {
	name string
	rows []*Row
}

// Adds a new row the the table
func (table *Table) AddRow(row *Row) *Table {
	table.rows = append(table.rows, row)

	return table
}

// Table constructor
func NewTable(name string) *Table {
	return &Table{
		name: name,
		rows: make([]*Row, 0),
	}
}

// Row struture stores all the information about a given row
type Row struct {
	name       string
	typing     string
	properties []string
}

// Row type setter
func (row *Row) SetType(typing string) *Row {
	row.typing = typing

	return row
}

// Adss a property to the row
func (row *Row) AddProperty(property string) *Row {
	row.properties = append(row.properties, property)

	return row
}

// Converts the Table to a string
func (row *Row) String() string {
	var buffer bytes.Buffer

	buffer.WriteString(row.name)
	buffer.WriteString(" ")
	buffer.WriteString(row.typing)
	for _, property := range row.properties {
		buffer.WriteString(" ")
		buffer.WriteString(property)
	}

	return buffer.String()
}

// Row constructor
func NewRow(name string) *Row {
	return &Row{
		name:       name,
		properties: make([]string, 0),
	}
}

// Builds the create table query string
func CreateTable(table *Table) string {
	var buffer bytes.Buffer

	buffer.WriteString("create table ")
	buffer.WriteString(table.name)
	buffer.WriteString("(")

	length := len(table.rows) - 1
	for index, row := range table.rows {
		buffer.WriteString(row.String())
		if index < length {
			buffer.WriteString(", ")
		}
	}

	buffer.WriteString(")")

	return buffer.String()
}

// Builds the insert query string
func Insert(table *Table) string {

	var buffer bytes.Buffer

	buffer.WriteString("insert ")
	buffer.WriteString(table.name)
	buffer.WriteString(" set ")

	rows := table.rows
	length := len(rows) - 1
	for index, row := range rows {
		buffer.WriteString(row.name)
		buffer.WriteString("=?")
		if index < length {
			buffer.WriteString(", ")
		}
	}

	return buffer.String()
}

// Builds the select all rows from table query
func SelectAll(table *Table) string {
	var buffer bytes.Buffer

	buffer.WriteString("select ")

	rows := table.rows
	length := len(rows) - 1
	for index, row := range rows {
		buffer.WriteString(row.name)
		if index < length {
			buffer.WriteString(", ")
		}
	}

	buffer.WriteString(" from ")
	buffer.WriteString(table.name)

	return buffer.String()
}

// Builds the Integer sql type
func Int() string {
	return "int"
}

// Builds the String sql type
func String(length int) string {
	var buffer bytes.Buffer

	buffer.WriteString("varchar(")
	buffer.WriteString(strconv.Itoa(length))
	buffer.WriteString(")")

	return buffer.String()
}

// Builds the Not Null property
func NotNull() string {
	return "not null"
}

// Builds the Primary Key property
func PrimaryKey() string {
	return "primary key"
}

// Builds the Auto Increment property
func AutoIncrement() string {
	return "auto_increment"
}
