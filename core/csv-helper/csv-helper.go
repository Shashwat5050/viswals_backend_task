package csvhelper

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
)

// ReadAll reads all records from the given CSV reader and returns them.
func ReadAll(csvReader *csv.Reader) ([][]string, error) {
	records, readErr := csvReader.ReadAll()
	if readErr != nil {
		return nil, readErr
	}

	return records, nil
}

// OpenFile opens a CSV file and returns a csv.Reader and the file itself.
func OpenFile(csvFilePath string) (*csv.Reader, error) {
	fileHandle, openErr := os.OpenFile(csvFilePath, os.O_RDONLY, 0444)
	if openErr != nil {
		return nil, openErr
	}

	csvReader := csv.NewReader(fileHandle)
	// Identify fields per record and bypass the first metadata line.
	firstRecord, readErr := csvReader.Read()
	if readErr != nil {
		return nil, readErr
	}

	csvReader.FieldsPerRecord = len(firstRecord)
	csvReader.ReuseRecord = false
	csvReader.Comma = ','

	return csvReader, nil
}

// ReadRows reads the next 'n' rows from the given CSV reader.
func ReadRows(csvReader *csv.Reader, rowCount int) ([][]string, []string, error) {
	var rows = make([][]string, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		row, readErr := csvReader.Read()
		if readErr != nil && errors.Is(readErr, io.EOF) {
			return rows, nil, readErr
		} else if readErr != nil && errors.Is(readErr, csv.ErrFieldCount) {
			return rows[:len(rows)-1], rows[len(rows)-1], nil
		}
		rows = append(rows, row)
	}
	return rows, nil, nil
}

