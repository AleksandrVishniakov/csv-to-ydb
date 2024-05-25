package csv_to_ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type YDB struct {
	Driver *ydb.Driver
}

func NewYDB(ctx context.Context, dsn string) (*YDB, error) {
	driver, err := ydb.Open(ctx, dsn)
	if err != nil {
		return nil, err
	}

	return &YDB{Driver: driver}, nil
}

func (db *YDB) Shutdown(ctx context.Context) error {
	return db.Driver.Close(ctx)
}
