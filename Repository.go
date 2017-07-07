// Repository.go provides some common functions used
// to create sql querys in Entity repositories
package persistence

import (
	"database/sql"
)

type Entity interface {
}

type EntityBuilder interface {
	GetProps() []interface{}
	Build() Entity
}

type RepositoryInteface interface {
	CreateTable()
	FindAll() []*Entity
	Find(params map[string]interface{}) []Entity
	Add(entity Entity)
}

type Repository struct {
	db      Sql
	builder EntityBuilder
	table   *Table
}

func (repository *Repository) CreateTable() {
	db := repository.db
	Exec(db, db.CreateTable(repository.table))
}

func (repository *Repository) FindAll() []Entity {
	db := repository.db
	rows := Query(db, db.SelectAll(repository.table))

	entities := repository.createFromRows(rows)

	return entities
}

func (repository *Repository) Find(params map[string]interface{}) []Entity {
	keys := make([]string, len(params))
	values := make([]interface{}, len(params))
	index := 0
	for key, value := range params {
		keys[index] = key
		values[index] = value
		index++
	}

	db := repository.db
	rows := Query(
		db,
		db.SelectWhere(repository.table, keys...),
		values...,
	)

	entities := repository.createFromRows(rows)

	return entities
}

func (repository *Repository) Add(entity Entity) {

}

func (repository *Repository) createFromRows(rows *sql.Rows) []Entity {
	var entities []Entity

	for rows.Next() {
		props := repository.builder.GetProps()
		err := rows.Scan(props...)
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		entities = append(entities, repository.builder.Build())
	}

	return entities
}

func NewRepository(db Sql, builder EntityBuilder, table *Table) *Repository {
	return &Repository{
		db:      db,
		builder: builder,
		table:   table,
	}
}
