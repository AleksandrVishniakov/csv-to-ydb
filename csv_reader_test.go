package csv_to_ydb

import (
	"reflect"
	"sync"
	"testing"
)

func TestCsvReader(t *testing.T) {
	reader := NewCSVReader("./csv-for-tests/random_records.csv", ',')
	dataCh := make(chan []string)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		correctElements := [][]string{
			{"Jack", "66", "False"},
			{"Charlie", "54", "False"},
			{"Grace", "64", "True"},
		}
		for _, element := range correctElements {
			el := <-dataCh
			if !reflect.DeepEqual(el, element) {
				t.Errorf("%s and %s should be equal", el, element)
			}
		}
		var el []string
		for el = range dataCh {
		}
		if !reflect.DeepEqual(el, []string{"Diana", "21", "False"}) {
			t.Errorf("%s and %s should be equal", el, []string{"Diana", "21", "False"})
		}
	}()

	wg.Add(1)
	go func() {
		defer close(dataCh)
		defer wg.Done()
		err := reader.ReadFromSource(dataCh)
		if err != nil {
			t.Error(err)
			return
		}
		columns, err := reader.GetColumns()
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(columns, []string{"name", "age", "male"}) {
			t.Errorf("%s and %s should be equal", columns, []string{"name", "age", "male"})
			return
		}
	}()
	wg.Wait()
}
