package sql2csv

import (
	"bytes"
	"database/sql"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func init() {
	_ = os.Setenv("TZ", "UTC")
}

func TestWriteFile(t *testing.T) {
	checkQueryAgainstResult(t, func(rows *sql.Rows) string {
		testCsvFileName := os.TempDir() + "/github.com-datatug-sql2csv-test.csv"
		err := WriteFile(testCsvFileName, rows)
		if err != nil {
			t.Fatalf("error in WriteCsvToFile: %v", err)
		}

		content, err := ioutil.ReadFile(testCsvFileName)
		if err != nil {
			t.Fatalf("error reading %v: %v", testCsvFileName, err)
		}

		return string(content[:])
	})
}

func TestWrite(t *testing.T) {
	checkQueryAgainstResult(t, func(rows *sql.Rows) string {
		buffer := &bytes.Buffer{}

		err := Write(buffer, rows)
		if err != nil {
			t.Fatalf("error in WriteCsvToWriter: %v", err)
		}

		return buffer.String()
	})
}

func TestWriteString(t *testing.T) {
	checkQueryAgainstResult(t, func(rows *sql.Rows) string {

		csv, err := WriteString(rows)
		if err != nil {
			t.Fatalf("error in WriteCsvToWriter: %v", err)
		}

		return csv
	})
}

func TestWriteHeaders(t *testing.T) {
	converter := getConverter(t)

	converter.WriteHeaders = false

	expected := "Alice,1,1973-11-29 21:33:09 +0000 UTC\n"
	actual := converter.String()

	assertCsvMatch(t, expected, actual)
}

func TestSetHeaders(t *testing.T) {
	converter := getConverter(t)

	converter.Headers = []string{"Name", "Age", "Birthday"}

	expected := "Name,Age,Birthday\nAlice,1,1973-11-29 21:33:09 +0000 UTC\n"
	actual := converter.String()

	assertCsvMatch(t, expected, actual)
}

func TestSetRowPostProcessorModifyingRows(t *testing.T) {
	converter := getConverter(t)

	converter.SetRowPostProcessor(func(rows []string, columnTypes []*sql.ColumnType) (bool, []string) {
		return true, []string{rows[0], "X", "X"}
	})

	expected := "name,age,bdate\nAlice,X,X\n"
	actual := converter.String()

	assertCsvMatch(t, expected, actual)
}

func TestSetRowPostProcessorOmittingRows(t *testing.T) {
	converter := getConverter(t)

	converter.SetRowPostProcessor(func(rows []string, columnTypes []*sql.ColumnType) (bool, []string) {
		return false, []string{}
	})

	expected := "name,age,bdate\n"
	actual := converter.String()

	assertCsvMatch(t, expected, actual)
}

func TestSetTimeFormat(t *testing.T) {
	converter := getConverter(t)

	// Kitchen: 3:04PM
	converter.TimeFormat = time.Kitchen

	expected := "name,age,bdate\nAlice,1,9:33PM\n"
	actual := converter.String()

	assertCsvMatch(t, expected, actual)
}

func TestConvertingNilValueShouldReturnEmptyString(t *testing.T) {
	converter := NewConverter(getTestRowsByQuery(t, "SELECT|people|name,nickname,age|"))

	expected := "name,nickname,age\nAlice,,1\n"
	actual := converter.String()

	assertCsvMatch(t, expected, actual)
}

func TestAlternateDelimiter(t *testing.T) {
	converter := getConverter(t)

	converter.Delimiter = '|'

	expected := "name|age|bdate\nAlice|1|1973-11-29 21:33:09 +0000 UTC\n"
	actual := converter.String()

	assertCsvMatch(t, expected, actual)
}

func TestMissingDelimiter(t *testing.T) {
	converter := getConverter(t)

	var delimiter rune
	converter.Delimiter = delimiter

	expected := "name,age,bdate\nAlice,1,1973-11-29 21:33:09 +0000 UTC\n"
	actual := converter.String()

	assertCsvMatch(t, expected, actual)
}

func checkQueryAgainstResult(t *testing.T, innerTestFunc func(*sql.Rows) string) {
	rows := getTestRows(t)

	expected := "name,age,bdate\nAlice,1,1973-11-29 21:33:09 +0000 UTC\n"

	actual := innerTestFunc(rows)

	assertCsvMatch(t, expected, actual)
}

func getTestRows(t *testing.T) *sql.Rows {
	return getTestRowsByQuery(t, "SELECT|people|name,age,bdate|")
}

func getTestRowsByQuery(t *testing.T, query string) *sql.Rows {
	db := setupDatabase(t)

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("error querying: %v", err)
	}

	return rows
}

func getConverter(t *testing.T) *Converter {
	return NewConverter(getTestRows(t))
}

func setupDatabase(t *testing.T) *sql.DB {
	db, err := sql.Open("test", "foo")
	if err != nil {
		t.Fatalf("Error opening testdb %v", err)
	}
	exec(t, db, "WIPE")
	exec(t, db, "CREATE|people|name=string,age=int32,bdate=datetime,nickname=nullstring")
	exec(t, db, "INSERT|people|name=Alice,age=?,bdate=?,nickname=?", 1, time.Unix(123456789, 0), nil)
	return db
}

func exec(t testing.TB, db *sql.DB, query string, args ...interface{}) {
	_, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("Exec of %q: %v", query, err)
	}
}

func assertCsvMatch(t *testing.T, expected string, actual string) {
	t.Helper()
	actual = strings.Replace(actual, " +0000 GMT", " +0000 UTC", -1) // TODO: Fix it: https://github.com/datatug/sql2csv/issues/1
	if actual != expected {
		t.Errorf("Expected CSV:\n\n%v\n Got CSV:\n\n%v\n", expected, actual)
	}
}
