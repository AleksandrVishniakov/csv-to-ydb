package csv_to_ydb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCreateTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	fmt.Println("connecting...")

	db, err := NewYDB(ctx, "grpc://localhost:2136/local")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Shutdown(ctx)
	})

	fmt.Println("db connected")

	repo := NewRepository(db.Driver)

	err = repo.CreateTable(ctx, "user1/table1", []string{"col1", "col2", "col3"})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("table created")
}

func TestLastId(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	fmt.Println("connecting...")

	db, err := NewYDB(ctx, "grpc://localhost:2136/local")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Shutdown(ctx)
	})

	fmt.Println("db connected")

	repo := NewRepository(db.Driver)

	const tableName = "user1/table1"

	primaryKey, columns, err := repo.tableColumnsWithPrimary(ctx, tableName)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%s %+v\n", primaryKey, columns)

	lastId, err := repo.lastTableId(ctx, tableName, primaryKey)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("lastId:", lastId)
}

func TestInsertQuery(t *testing.T) {
	fmt.Println(insertQuery("table1", "user_id", 1, []string{"column1", "column2", "column3"}, []string{"val1", "val2", "val3"}))
}

func TestInsertRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	fmt.Println("connecting...")

	db, err := NewYDB(ctx, "grpc://localhost:2136/local")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Shutdown(ctx)
	})

	fmt.Println("db connected")

	repo := NewRepository(db.Driver)

	const tableName = "user1/table1"

	primaryKey, columns, err := repo.tableColumnsWithPrimary(ctx, tableName)
	if err != nil {
		t.Fatal(err)
	}

	lastId, err := repo.lastTableId(ctx, tableName, primaryKey)
	if err != nil {
		t.Fatal(err)
	}

	err = repo.insertRow(ctx, tableName, []string{"val1", "val2", "val3"}, primaryKey, lastId+1, columns)
	if err != nil {
		t.Fatal(err)
	}
}

func TestInsertData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	fmt.Println("connecting...")

	db, err := NewYDB(ctx, "grpc://localhost:2136/local")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Shutdown(ctx)
	})

	fmt.Println("db connected")

	repo := NewRepository(db.Driver)

	const tableName = "user1/table1"

	err = repo.InsertData(ctx, tableName, [][]string{
		{"col1_row1", "col2_row1", "col3_row1"},
		{"col1_row2", "col2_row2", "col3_row2"},
		{"col1_row3", "col2_row3", "col3_row3"},
		{"col1_row4", "col2_row4", "col3_row4"},
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestInsertDataWithBulk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	fmt.Println("connecting...")

	db, err := NewYDB(ctx, "grpc://localhost:2136/local")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Shutdown(ctx)
	})

	fmt.Println("db connected")

	repo := NewRepository(db.Driver)
	const tableName = "user1/table1"
	dataCh := make(chan []string)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = repo.InsertDataWithBulk(ctx, tableName, dataCh)
		if err != nil {
			fmt.Println(err)
		}
	}()
	go func() {
		defer close(dataCh)
		dataCh <- []string{"col1_row1", "col2_row1", "col3_row1"}
		dataCh <- []string{"col1_row2", "col2_row2", "col3_row2"}
		dataCh <- []string{"col1_row3", "col2_row3", "col3_row3"}
	}()
	wg.Wait()
}
