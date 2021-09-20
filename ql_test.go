package ql

import (
	"database/sql"
	"reflect"
	"testing"

	"time"

	"math/big"

	"strings"

	"github.com/akamajoris/ngorm/engine"
	"github.com/akamajoris/ngorm/model"
	"github.com/akamajoris/ngorm/scope"
)

type Department struct {
	ID   int
	Name string
}

const migration = `
BEGIN TRANSACTION;
	CREATE TABLE Orders (CustomerID int, Date time);
	CREATE INDEX OrdersID ON Orders (id());
	CREATE INDEX OrdersDate ON Orders (Date);
	CREATE TABLE Items (OrderID int, ProductID int, Qty int);
	CREATE INDEX ItemsOrderID ON Items (OrderID);
COMMIT;
`

func TestDialect(t *testing.T) {
	d := Memory()
	if d.GetName() != "ql-mem" {
		t.Errorf("expected ql-mem got %s", d.GetName())
	}
	d = File()
	if d.GetName() != "ql" {
		t.Errorf("expected ql got %s", d.GetName())
	}
	db, err := sql.Open("ql-mem", "test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tx.Exec(migration)
	_ = tx.Commit()
	d.SetDB(db)
	if d.db == nil {
		t.Fatal("expected the database to be set")
	}

	//
	// HasIndex
	//
	if !d.HasIndex("Orders", "OrdersID") {
		t.Error("expected to be true")
	}

	//RemoveIndex
	err = d.RemoveIndex("Orders", "OrdersID")
	if err != nil {
		t.Error(err)
	}

	if d.HasIndex("Orders", "OrdersID") {
		t.Error("expected to be false")
	}

	// Has Table
	if !d.HasTable("Orders") {
		t.Error("expected to be true")
	}
	if !d.HasColumn("Orders", "Date") {
		t.Error("expected to be true")
	}

	//Query field name
	if d.QueryFieldName("users") != "" {
		t.Errorf("didn't expect %s", d.QueryFieldName("users"))
	}

	// current database
	if d.CurrentDatabase() != "" {
		t.Errorf("didn't expect %s", d.CurrentDatabase())
	}
	tn := "users"
	fn := "city"
	dest := "id"
	o := d.BuildForeignKeyName(tn, fn, dest)
	exp := "users_city_id_foreign"
	if o != exp {
		t.Errorf("expected %s got %s", exp, o)
	}

	// last insert id suffix
	if d.LastInsertIDReturningSuffix(tn, fn) != "" {
		t.Errorf("didn't expect %s", d.LastInsertIDReturningSuffix(tn, fn))
	}

	// sselect from dummy table
	if d.SelectFromDummyTable() != "" {
		t.Errorf("didn't expect %s", d.SelectFromDummyTable())
	}

	// limit and offset
	limit := 5
	offset := 10
	o = d.LimitAndOffsetSQL(limit, offset)
	exp = "LIMIT 5 OFFSET 10"
	o = strings.TrimSpace(o)
	if o != exp {
		t.Errorf("expected %s got %s", exp, o)
	}
}

func TestQL_Quote(t *testing.T) {
	q := &QL{}
	src := "quote"
	expect := `quote`
	v := q.Quote(src)
	if v != expect {
		t.Errorf("expected %s got %s", expect, v)
	}
}

func TestQL_BindVar(t *testing.T) {
	q := &QL{}
	src := 1
	expect := "$1"
	v := q.BindVar(src)
	if v != expect {
		t.Errorf("expected %s got %s", expect, v)
	}
}

type Sample struct {
	ID        int64
	CreatedAt time.Time
	Big       big.Int
	Rat       big.Rat
	Blob      []byte
	Bool      bool
}

func TestQL_DataTypeOf(t *testing.T) {
	e := &engine.Engine{
		Search:    &model.Search{},
		Scope:     &model.Scope{},
		StructMap: model.NewStructsMap(),
	}
	m, err := scope.GetModelStruct(e, &Sample{})
	if err != nil {
		t.Error(err)
	}
	q := &QL{}
	for _, f := range m.StructFields {
		switch f.Name {
		case "ID":
			e := reflect.Int64.String()
			s, err := q.DataTypeOf(f)
			if err != nil {
				t.Fatal(err)
			}
			if s != e {
				t.Errorf("expected %s got %s", e, s)
			}
		case "CreatedAt":
			e := "time"
			s, err := q.DataTypeOf(f)
			if err != nil {
				t.Fatal(err)
			}
			if s != e {
				t.Errorf("expected %s got %s", e, s)
			}
		case "Big":
			e := "bigint"
			s, err := q.DataTypeOf(f)
			if err != nil {
				t.Fatal(err)
			}
			if s != e {
				t.Errorf("expected %s got %s", e, s)
			}
		case "Rat":
			e := "bigrat"
			s, err := q.DataTypeOf(f)
			if err != nil {
				t.Fatal(err)
			}
			if s != e {
				t.Errorf("expected %s got %s", e, s)
			}
		case "Blob":
			e := "blob"
			s, err := q.DataTypeOf(f)
			if err != nil {
				t.Fatal(err)
			}
			if s != e {
				t.Errorf("expected %s got %s", e, s)
			}
		case "Bool":
			e := "bool"
			s, err := q.DataTypeOf(f)
			if err != nil {
				t.Fatal(err)
			}
			if s != e {
				t.Errorf("expected %s got %s", e, s)
			}
		}
	}
}
