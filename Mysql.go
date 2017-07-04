package persistence

import (
	"bytes"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

const database = "mysql"

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

	db, err := sql.Open(database, buffer.String())
	if err != nil {
		panic(err)
	}

	return db
}

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
