package sql2csv

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
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

type failWriter struct {
	failOnCall int
	calls      int
}

func (w *failWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.failOnCall == 0 || w.calls >= w.failOnCall {
		return 0, os.ErrInvalid
	}
	return len(p), nil
}

func TestUniqueIdentifier_ValidAndInvalid(t *testing.T) {
	db := setupDatabase(t)
	exec(t, db, "CREATE|uuids|id=UNIQUEIDENTIFIER")

	rawUUID := []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	exec(t, db, "INSERT|uuids|id=?", rawUUID)

	rows, err := db.Query("SELECT|uuids|id|")
	if err != nil {
		t.Fatal(err)
	}
	conv := NewConverter(rows)
	actual := conv.String()
	expected := "id\n01234567-89ab-cdef-0123-456789abcdef\n"
	if actual != expected {
		t.Fatalf("expected UUID string %q, got %q", expected, actual)
	}

	// Invalid UUID (wrong byte length)
	exec(t, db, "WIPE")
	exec(t, db, "CREATE|bad_uuids|id=UNIQUEIDENTIFIER")
	exec(t, db, "INSERT|bad_uuids|id=?", []byte{1, 2, 3})

	badRows, err := db.Query("SELECT|bad_uuids|id|")
	if err != nil {
		t.Fatal(err)
	}
	badConv := NewConverter(badRows)
	var buf bytes.Buffer
	if err := badConv.Write(&buf); err == nil {
		t.Fatal("expected error with invalid UUID bytes")
	}

	// String() on bad converter returns empty string
	badRows2, _ := db.Query("SELECT|bad_uuids|id|")
	badConv2 := NewConverter(badRows2)
	if str := badConv2.String(); str != "" {
		t.Fatalf("expected empty string on error, got %q", str)
	}

	// WriteFile on bad converter hits _ = f.Close(); return err
	tmpFile := os.TempDir() + "/bad_uuid_test.csv"
	badRows3, _ := db.Query("SELECT|bad_uuids|id|")
	badConv3 := NewConverter(badRows3)
	if err := badConv3.WriteFile(tmpFile); err == nil {
		t.Fatal("expected error in WriteFile with bad UUID")
	}
	_ = os.Remove(tmpFile)
}

func TestWriteErrors(t *testing.T) {
	// WriteFile invalid path
	conv := getConverter(t)
	if err := conv.WriteFile("/nonexistent_dir/bad/file.csv"); err == nil {
		t.Fatal("expected error with invalid file path")
	}

	// Closed rows fails on ColumnTypes
	db := setupDatabase(t)
	rows, err := db.Query("SELECT|people|name|")
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	closedConv := NewConverter(rows)
	var buf bytes.Buffer
	if err := closedConv.Write(&buf); err == nil {
		t.Fatal("expected error on closed rows")
	}

	// Write headers failure (exceed bufio.Writer 4096 byte buffer so it flushes to writer)
	rowsH, _ := db.Query("SELECT|people|name|")
	convH := NewConverter(rowsH)
	convH.Headers = []string{strings.Repeat("x", 5000)}
	if err := convH.Write(&failWriter{failOnCall: 1}); err == nil {
		t.Fatal("expected error on header write failure")
	}

	// Write row failure
	rowsR, _ := db.Query("SELECT|people|name|")
	convR := NewConverter(rowsR)
	convR.SetRowPostProcessor(func(rows []string, colTypes []*sql.ColumnType) (bool, []string) {
		return true, []string{strings.Repeat("y", 5000)}
	})
	if err := convR.Write(&failWriter{failOnCall: 1}); err == nil {
		t.Fatal("expected error on row write failure")
	}

	// Scan failure
	scanDb, err := sql.Open("failScanDriver", "")
	if err != nil {
		t.Fatal(err)
	}
	defer scanDb.Close()
	scanRows, err := scanDb.Query("SELECT")
	if err != nil {
		t.Fatal(err)
	}
	scanConv := NewConverter(scanRows)
	var scanBuf bytes.Buffer
	if err := scanConv.Write(&scanBuf); err == nil {
		t.Fatal("expected error on rows.Scan failure")
	}
}

func init() {
	sql.Register("failScanDriver", failScanDriver{})
}

type failScanDriver struct{}

func (failScanDriver) Open(name string) (driver.Conn, error) {
	return failScanConn{}, nil
}

type failScanConn struct{}

func (failScanConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}
func (failScanConn) Close() error { return nil }
func (failScanConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}
func (failScanConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return &failScanRows{}, nil
}

type failScanRows struct {
	nextCalled bool
}

func (r *failScanRows) Columns() []string { return []string{"col"} }
func (r *failScanRows) Close() error      { return nil }
func (r *failScanRows) NextRow() error {
	if r.nextCalled {
		return io.EOF
	}
	r.nextCalled = true
	return nil
}
func (r *failScanRows) Next(dest []driver.Value) error {
	return r.NextRow()
}
func (r *failScanRows) ScanColumn(scanCtx driver.ScanContext, index int, dest any) error {
	return errors.New("forced scan error")
}

