package persistence

import (
	"bytes"
	"database/sql"
	// "fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

type Mysql struct {
	username string
	database string
	//TODO host and port => user:password@tcp(127.0.0.1:3306)/hello
}

func (mysql *Mysql) Open() *sql.DB {
	var buffer bytes.Buffer

	buffer.WriteString(mysql.username)
	buffer.WriteString(":@/")
	buffer.WriteString(mysql.database)

	db, _ := sql.Open("mysql", buffer.String())

	return db
}

func (mysql *Mysql) CreateTable(table *Table) {
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

	db := mysql.Open()
	stmt, _ := db.Prepare(buffer.String())
	stmt.Exec()

}

func (mysql *Mysql) Insert(table string, values map[string]interface{}) bool {

	var buffer bytes.Buffer

	buffer.WriteString("insert ")
	buffer.WriteString(table)
	buffer.WriteString(" set ")

	index := 0
	execValues := make([]interface{}, len(values))
	for key, value := range values {
		buffer.WriteString(key)
		buffer.WriteString("=? ")
		execValues[index] = value
		index++
	}

	//TODO handle errors
	db := mysql.Open()
	stmt, _ := db.Prepare(buffer.String())
	stmt.Exec(execValues...)

	return true
}

func NewMysql(username string, database string) *Mysql {
	return &Mysql{
		username: username,
		database: database,
	}
}

// Row
type Table struct {
	name string
	rows []*Row
}

func (table *Table) AddRow(row *Row) *Table {
	table.rows = append(table.rows, row)

	return table
}

func NewTable(name string) *Table {
	return &Table{
		name: name,
		rows: make([]*Row, 0),
	}
}

type Row struct {
	name       string
	typing     string
	properties []string
}

func (row *Row) SetType(typing string) *Row {
	row.typing = typing

	return row
}

func (row *Row) AddProperty(property string) *Row {
	row.properties = append(row.properties, property)

	return row
}

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

func NewRow(name string) *Row {
	return &Row{
		name:       name,
		properties: make([]string, 0),
	}
}

func Int() string {
	return "int"
}

func String(length int) string {
	var buffer bytes.Buffer

	buffer.WriteString("varchar(")
	buffer.WriteString(strconv.Itoa(length))
	buffer.WriteString(")")

	return buffer.String()
}

func NotNull() string {
	return "not null"
}

func PrimaryKey() string {
	return "primary key"
}

func AutoIncrement() string {
	return "auto_increment"
}
