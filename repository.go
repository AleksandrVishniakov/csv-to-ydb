package csv_to_ydb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type Repository struct {
	driver *ydb.Driver
}

func NewRepository(driver *ydb.Driver) *Repository {
	return &Repository{driver: driver}
}

func (r *Repository) CreateTable(ctx context.Context, tableName string, columns []string) (err error) {
	// Create unique id column for setting required primary key to
	var idColumn = fmt.Sprintf("id-%s", uuid.NewString())

	var opts = make([]options.CreateTableOption, 0)
	opts = append(
		opts, options.WithColumn(idColumn, types.TypeUint64),
		options.WithPrimaryKeyColumn(idColumn),
	)
	opts = append(opts, columnsToCreateTableOptions(columns)...)

	err = r.driver.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(r.driver.Name(), tableName),
				opts...,
			)
		},
	)

	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) InsertData(ctx context.Context, tableName string, data [][]string) (err error) {
	if len(data) == 0 {
		return nil
	}

	primaryKey, columns, err := r.tableColumnsWithPrimary(ctx, tableName)
	if err != nil {
		return err
	}

	lastId, err := r.lastTableId(ctx, tableName, primaryKey)
	if err != nil {
		return err
	}

	fmt.Println("lastId:", lastId)

	for _, row := range data {
		lastId++
		err = r.insertRow(ctx, tableName, row, primaryKey, lastId, columns)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Repository) InsertDataWithBulk(ctx context.Context, tableName string, dataCh chan []string) (err error) {
	primaryKey, columns, err := r.tableColumnsWithPrimary(ctx, tableName)
	if err != nil {
		return err
	}
	lastId, err := r.lastTableId(ctx, tableName, primaryKey)
	if err != nil {
		return err
	}
	fmt.Println(path.Join(r.driver.Name(), tableName))
	err = r.driver.Table().Do( // Do retry operation on errors with best effort
		ctx, // context manage exiting from Do
		func(ctx context.Context, s table.Session) (err error) { // retry operation
			rows := make([]types.Value, 0)
			for data := range dataCh {
				structFieldsValues := make([]types.StructValueOption, 0, len(columns))
				for i, el := range data {
					structFieldsValues = append(structFieldsValues, types.StructFieldValue(columns[i], types.TextValue(el)))
				}
				lastId++
				structFieldsValues = append(structFieldsValues, types.StructFieldValue(primaryKey, types.Uint64Value(lastId)))
				rows = append(rows, types.StructValue(
					structFieldsValues...,
				))
			}
			return s.BulkUpsert(ctx, path.Join(r.driver.Name(), tableName), types.ListValue(rows...))
		},
	)
	return err
}

func (r *Repository) insertRow(ctx context.Context, tableName string, row []string, primaryKey string, id uint64, columns []string) (err error) {
	if len(row) != len(columns) {
		return errors.New("mismatched columns and data length")
	}

	query, err := insertQuery(tableName, primaryKey, id, columns, row)
	if err != nil {
		return err
	}

	err = r.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, _, err := s.Execute(ctx, table.DefaultTxControl(), query, table.NewQueryParameters())

		return err
	})

	if err != nil {
		return err
	}

	return nil
}

func insertQuery(tableName string, primaryKey string, id uint64, columns []string, row []string) (query string, err error) {
	if !validSQLParam(tableName) {
		return "", errors.New("insertQuery: invalid table name")
	}

	colsString := fmt.Sprintf("(`%s`, %s)", primaryKey, strings.Join(columns, ", "))
	valuesString := fmt.Sprintf("(%d, ", id)
	for i := range row {
		if !validSQLParam(row[i]) {
			return "", errors.New("insertQuery: invalid row value")
		}

		row[i] = fmt.Sprintf("\"%s\"u", row[i])
	}
	valuesString += strings.Join(row, ", ") + ")"

	return fmt.Sprintf("INSERT INTO `%s` %s VALUES %s;", tableName, colsString, valuesString), nil
}

func (r *Repository) tableColumnsWithPrimary(ctx context.Context, tableName string) (primaryKey string, columns []string, err error) {
	columns = make([]string, 0)
	err = r.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		desc, err := s.DescribeTable(ctx, path.Join(r.driver.Name(), tableName))
		if err != nil {
			return err
		}

		if len(desc.PrimaryKey) != 1 {
			return errors.New("incorrect primary keys number")
		}

		primaryKey = desc.PrimaryKey[0]

		for _, col := range desc.Columns {
			if col.Name != primaryKey {
				columns = append(columns, col.Name)
			}
		}

		return nil
	})
	if err != nil {
		return "", nil, err
	}

	return primaryKey, columns, nil
}

func (r *Repository) lastTableId(ctx context.Context, tableName string, primaryKey string) (lastId uint64, err error) {
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	query, err := escapeQuery("SELECT `%s` as last_id FROM `%s` ORDER BY last_id DESC LIMIT 1", primaryKey, tableName)
	if err != nil {
		return 0, err
	}

	err = r.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, res, err := s.Execute(ctx, readTx,
			query,
			table.NewQueryParameters(),
		)

		if err != nil {
			return err
		}
		defer res.Close()

		ok := res.NextResultSet(ctx)
		if !ok {
			return nil
		}

		if res.Err() != nil {
			return res.Err()
		}

		ok = res.NextRow()
		if !ok {
			return nil
		}

		if res.Err() != nil {
			return res.Err()
		}

		return res.ScanNamed(
			named.Required("last_id", &lastId),
		)
	})

	if err != nil {
		return 0, err
	}

	return lastId, nil
}

func columnsToCreateTableOptions(columns []string) []options.CreateTableOption {
	var opts = make([]options.CreateTableOption, len(columns))

	for i, column := range columns {
		opts[i] = options.WithColumn(column, types.Optional(types.TypeString))
	}

	return opts
}

func escapeQuery(format string, args ...any) (escapedQuery string, err error) {
	for _, a := range args {
		if str, ok := a.(string); ok {
			if !validSQLParam(str) {
				return "", errors.New("argument contains forbidden symbol")
			}
		}
	}

	return fmt.Sprintf(format, args...), nil
}

func validSQLParam(str string) (ok bool) {
	const forbiddenSymbols = ";.,\"\\$&*(){}[]\n+?`'<>"

	for _, r := range str {
		if strings.ContainsRune(forbiddenSymbols, r) {
			return false
		}
	}

	return true
}
