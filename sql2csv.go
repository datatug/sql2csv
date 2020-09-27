// sql2csv is a package to make it dead easy to turn arbitrary database query
// results (in the form of database/sql Rows) into CSV output.
//
// Source and README at https://github.com/datatug/sql2csv
package sql2csv

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/google/uuid"
	"io"
	"os"
	"time"
)

// WriteFile will write a CSV file to the file name specified (with headers)
// based on whatever is in the sql.Rows you pass in. It calls WriteCsvToWriter under
// the hood.
func WriteFile(csvFileName string, rows *sql.Rows) error {
	return NewConverter(rows).WriteFile(csvFileName)
}

// WriteString will return a string of the CSV. Don't use this unless you've
// got a small data set or a lot of memory
func WriteString(rows *sql.Rows) (string, error) {
	return NewConverter(rows).WriteString()
}

// Write will write a CSV file to the writer passed in (with headers)
// based on whatever is in the sql.Rows you pass in.
func Write(writer io.Writer, rows *sql.Rows) error {
	return NewConverter(rows).Write(writer)
}

// CsvRowPostProcessorFunc is a function type for postprocessing your CSV.
// It takes the columns after they've been munged into strings but
// before they've been passed into the CSV writer.
//
// Return an outputRow of false if you want the row skipped otherwise
// return the processed Row slice as you want it written to the CSV.
type CsvRowPostProcessorFunc func(row []string, columnTypes []*sql.ColumnType) (outputRow bool, processedRow []string)

// Converter does the actual work of converting the rows to CSV.
// There are a few settings you can override if you want to do
// some fancy stuff to your CSV.
type Converter struct {
	Headers      []string // Column headers to use (default is rows.Columns())
	WriteHeaders bool     // Flag to output headers in your CSV (default is true)
	TimeFormat   string   // Format string for any time.Time values (default is time's default)
	Delimiter    rune     // Delimiter to use in your CSV (default is comma)

	rows             *sql.Rows
	rowPostProcessor CsvRowPostProcessorFunc
}

// SetRowPostProcessor lets you specify a CsvRowPostProcessorFunc for this conversion
func (c *Converter) SetRowPostProcessor(processor CsvRowPostProcessorFunc) {
	c.rowPostProcessor = processor
}

// String returns the CSV as a string in an fmt package friendly way
func (c Converter) String() string {
	s, err := c.WriteString()
	if err != nil {
		return ""
	}
	return s
}

// WriteString returns the CSV as a string and an error if something goes wrong
func (c Converter) WriteString() (string, error) {
	buffer := bytes.Buffer{}
	err := c.Write(&buffer)
	return buffer.String(), err
}

// WriteFile writes the CSV to the filename specified, return an error if problem
func (c Converter) WriteFile(csvFileName string) error {
	f, err := os.Create(csvFileName)
	if err != nil {
		return err
	}

	err = c.Write(f)
	if err != nil {
		_ = f.Close() // close, but only return/handle the write error
		return err
	}

	return f.Close()
}

// Write writes the CSV to the Writer provided
func (c Converter) Write(writer io.Writer) error {
	rows := c.rows
	csvWriter := csv.NewWriter(writer)
	if c.Delimiter != '\x00' {
		csvWriter.Comma = c.Delimiter
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	if c.WriteHeaders {
		// use Headers if set, otherwise default to
		// query Columns
		var headers []string
		if len(c.Headers) > 0 {
			headers = c.Headers
		} else {
			columnNames := make([]string, len(columns))
			for i, col := range columns {
				columnNames[i] = col.Name()
			}
			headers = columnNames
		}
		err = csvWriter.Write(headers)
		if err != nil {
			// TODO wrap err to say it was an issue with headers?
			return err
		}
	}

	count := len(columns)
	values := make([]interface{}, count)
	valPointers := make([]interface{}, count)

	for rows.Next() {
		row := make([]string, count)

		for i := range columns {
			valPointers[i] = &values[i]
		}

		if err = rows.Scan(valPointers...); err != nil {
			return err
		}

		for i, column := range columns {
			if b, isSliceOfBytes := values[i].([]byte); isSliceOfBytes {
				switch column.DatabaseTypeName() {
				case "UNIQUEIDENTIFIER":
					var v uuid.UUID
					if v, err = uuid.FromBytes(b); err != nil {
						return err
					}
					row[i] = v.String()
				default:
					row[i] = string(b)
				}
			} else {
				var value interface{}
				value = values[i]
				if c.TimeFormat != "" {
					if timeValue, ok := value.(time.Time); ok {
						value = timeValue.Format(c.TimeFormat)
					}
				}
				if value == nil {
					row[i] = ""
				} else {
					row[i] = fmt.Sprintf("%v", value)
				}
			}
		}

		writeRow := true
		if c.rowPostProcessor != nil {
			writeRow, row = c.rowPostProcessor(row, columns)
		}
		if writeRow {
			err = csvWriter.Write(row)
			if err != nil {
				// TODO wrap this err to give context as to why it failed?
				return err
			}
		}
	}
	err = rows.Err()

	csvWriter.Flush()

	return err
}

// NewConverter will return a Converter which will write your CSV however you like
// but will allow you to set a bunch of non-default behaviour like overriding
// headers or injecting a pre-processing step into your conversion
func NewConverter(rows *sql.Rows) *Converter {
	return &Converter{
		rows:         rows,
		WriteHeaders: true,
		Delimiter:    ',',
	}
}
