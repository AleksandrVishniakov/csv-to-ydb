package csv_to_ydb

import (
	"encoding/csv"
	"io"
	"os"
	"strings"
)

type CSVReader struct {
	filePath  string
	separator rune
}

func NewCSVReader(filePath string, separator rune) *CSVReader {
	return &CSVReader{
		filePath:  filePath,
		separator: separator,
	}
}

func (c *CSVReader) ReadFromSource(dataCh chan<- []string) error {
	file, err := os.Open(c.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = c.separator
	count := 0
	for {
		record, err := reader.Read()
		if err != nil || count == 0 {
			count++
			if err == io.EOF {
				break // Конец файла достигнут
			}
			continue
		}

		// Приводим каждое поле к строке и формируем итоговый массив строк
		rowData := make([]string, len(record))
		for i, field := range record {
			rowData[i] = strings.TrimSpace(field) // Удаляем пробелы в начале и конце строки
		}
		dataCh <- rowData // Отправляем данные в канал
		count++
	}
	return nil
}

func (c *CSVReader) GetColumns() ([]string, error) {
	file, err := os.Open(c.filePath)
	if err != nil {
		return make([]string, 0), err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = c.separator
	record, err := reader.Read()
	if err != nil {
		return make([]string, 0), err
	}
	columns := make([]string, len(record))
	for i, field := range record {
		columns[i] = strings.TrimSpace(field)
	}
	return columns, nil
}
