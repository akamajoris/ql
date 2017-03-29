// Package ql exposes implementations and functions that enables ngorm to work
// with ql database.
//
// ql is an embedded sql database. This database doesn't conform 100% with the
// SQL standard. The link to the project is https://github.com/cznic/ql
package ql

import (
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ngorm/ngorm/dialects"
	"github.com/ngorm/ngorm/model"
	"github.com/ngorm/ngorm/regexes"
)

//QL implements the dialects.Dialect interface that uses ql database as the SQl
//backend.
//
// For some reason the ql database doesn't support multiple databases, as
// databases are file based. So, the name of the file is the name of the
// database.. Which doesn't affect the queries, since the database name is
// irrelevant assuming the SQLCommon interface is the handle over the open
// database.
type QL struct {
	name string
	db   model.SQLCommon
}

// Memory returns the dialect for in memory ql database. This is not persistent
// everything will be lost when the process exits.
func Memory() *QL {
	return &QL{name: "ql-mem"}
}

//File returns the dialect for file backed ql database. This is the recommended
//way use the Memory only for testing else you might lose all of your data.
func File() *QL {
	return &QL{name: "ql"}
}

func init() {
	dialects.Register(Memory())
	dialects.Register(File())
}

// GetName get dialect's name
func (q *QL) GetName() string {
	return q.name
}

// SetDB set db for dialect
func (q *QL) SetDB(db model.SQLCommon) {
	q.db = db
}

// BindVar return the placeholder for actual values in SQL statements, in many dbs it is "?", Postgres using $1
func (q QL) BindVar(i int) string {
	return fmt.Sprintf("$%d", i)
}

// Quote quotes field name to avoid SQL parsing exceptions by using a reserved word as a field name
func (q *QL) Quote(key string) string {
	//return fmt.Sprintf(`"%s"`, key)
	return key
}

//PrimaryKey implements dialects.Dialect interface. This is supposed to return a
//comma separated string of primary keys.
//
// ql does not support PRIMARY KEY so no matter how many keys are passed this
// method will return an empty string.
func (q *QL) PrimaryKey(keys []string) string {
	return ""
}

// DataTypeOf return data's sql type
func (q *QL) DataTypeOf(field *model.StructField) (string, error) {
	var dataValue, sqlType, _, additionalType = model.ParseFieldStructForDialect(field)
	switch dataValue.Kind() {
	case reflect.Bool:
		sqlType = "bool"
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Float32,
		reflect.Float64,
		reflect.String:
		sqlType = dataValue.Kind().String()
	case reflect.Struct:
		switch dataValue.Interface().(type) {
		case time.Time:
			sqlType = "time"
		case big.Int:
			sqlType = "bigint"
		case big.Rat:
			sqlType = "bigrat"
		}
	default:
		if _, ok := dataValue.Interface().([]byte); ok {
			sqlType = "blob"
		}
	}
	if sqlType == "" {
		return "", fmt.Errorf("invalid sql type %s (%s) for ql", dataValue.Type().Name(), dataValue.Kind().String())
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType, nil
	}

	return fmt.Sprintf("%v %v", sqlType, additionalType), nil
}

// HasIndex check has index or not
func (q *QL) HasIndex(tableName string, indexName string) bool {
	query := "select count() from __Index where Name=$1  && TableName=$2"
	var count int
	_ = q.db.QueryRow(query, indexName, tableName).Scan(&count)
	return count > 0
}

// HasForeignKey check has foreign key or not
func (q *QL) HasForeignKey(tableName string, foreignKeyName string) bool {
	return false
}

// RemoveIndex remove index
func (q *QL) RemoveIndex(tableName string, indexName string) error {
	tx, err := q.db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(fmt.Sprintf("DROP INDEX %v", indexName))
	if err != nil {
		return err
	}
	return tx.Commit()
}

// HasTable check has table or not
func (q *QL) HasTable(tableName string) bool {
	query := "select count() from __Table where Name=$1"
	var count int
	_ = q.db.QueryRow(query, tableName).Scan(&count)
	return count > 0
}

// HasColumn check has column or not
func (q *QL) HasColumn(tableName string, columnName string) bool {
	query := "select count() from __Column where Name=$1  && TableName=$2"
	var count int
	_ = q.db.QueryRow(query, columnName, tableName).Scan(&count)
	return count > 0
}

// LimitAndOffsetSQL return generated SQL with Limit and Offset, as mssql has special case
func (q *QL) LimitAndOffsetSQL(limit, offset interface{}) (sql string) {
	if limit != nil {
		if parsedLimit, err := strconv.ParseInt(fmt.Sprint(limit), 0, 0); err == nil && parsedLimit > 0 {
			sql += fmt.Sprintf(" LIMIT %d", parsedLimit)
		}
	}
	if offset != nil {
		if parsedOffset, err := strconv.ParseInt(fmt.Sprint(offset), 0, 0); err == nil && parsedOffset > 0 {
			sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
		}
	}
	return
}

// SelectFromDummyTable return select values, for most dbs, `SELECT values` just works, mysql needs `SELECT value FROM DUAL`
func (q *QL) SelectFromDummyTable() string {
	return ""
}

// LastInsertIDReturningSuffix ost dbs support LastInsertId, but postgres needs to use `RETURNING`
func (q *QL) LastInsertIDReturningSuffix(tableName, columnName string) string {
	return ""
}

// BuildForeignKeyName returns a foreign key name for the given table, field and reference
func (q *QL) BuildForeignKeyName(tableName, field, dest string) string {
	keyName := fmt.Sprintf("%s_%s_%s_foreign", tableName, field, dest)
	keyName = regexes.KeyName.ReplaceAllString(keyName, "_")
	return keyName
}

// CurrentDatabase return current database name
func (q *QL) CurrentDatabase() string {
	return ""
}

//QueryFieldName returns prefix for field names if name. For instance users.id
//to point to users id field.
//
// ql doesn't support this, so it returns an empty string for tablename prefix on
// fields. instead of users.id to becomes id
func (q QL) QueryFieldName(name string) string {
	return ""
}
