package dbgen

import (
	"bufio"

	"github.com/gozssky/dbgen/constant"
	"github.com/gozssky/dbgen/template"
)

// Writer specifies how to write rows to a file.
type Writer interface {
	// WriteValue writes a single value.
	WriteValue(value constant.Value) error
	// WriteFileHeader writes the content at the beginning of the file.
	WriteFileHeader(table *Table) error
	// WriteRowGroupHeader writes the content before a row group.
	WriteRowGroupHeader(table *Table) error
	// WriteValueHeader writes the column name before a value.
	WriteValueHeader(column template.Name) error
	// WriteValueSeparator writes the separator between the every value.
	WriteValueSeparator() error
	// WriteRowSeparator writes the separator between the every row.
	WriteRowSeparator() error
	// WriteRowGroupTrailer writes the content after a row group.
	WriteRowGroupTrailer() error
}

type CSVWriter struct {
	bufw *bufio.Writer
}

func (w *CSVWriter) WriteValue(value constant.Value) error {
	panic("unimplemented")
}

func (w *CSVWriter) WriteFileHeader(table *Table) error {
	panic("unimplemented")
}

func (w *CSVWriter) WriteRowGroupHeader(_ *Table) error {
	panic("unimplemented")
}

func (w *CSVWriter) WriteValueHeader(_ template.Name) error {
	panic("unimplemented")
}

func (w *CSVWriter) WriteValueSeparator() error {
	return w.bufw.WriteByte(',')
}

func (w *CSVWriter) WriteRowSeparator() error {
	return w.bufw.WriteByte('\n')
}

func (w *CSVWriter) WriteRowGroupTrailer() error {
	panic("unimplemented")
}

type ParquetWriter struct {
	bufw *bufio.Writer
}

func (w *ParquetWriter) WriteValue(value constant.Value) error {
	panic("unimplemented")
}

func (w *ParquetWriter) WriteFileHeader(table *Table) error {
	panic("unimplemented")
}

func (w *ParquetWriter) WriteRowGroupHeader(_ *Table) error {
	panic("unimplemented")
}

func (w *ParquetWriter) WriteValueHeader(_ template.Name) error {
	panic("unimplemented")
}

func (w *ParquetWriter) WriteValueSeparator() error {
	panic("unimplemented")
}

func (w *ParquetWriter) WriteRowSeparator() error {
	panic("unimplemented")
}

func (w *ParquetWriter) WriteRowGroupTrailer() error {
	panic("unimplemented")
}

type SQLWriter struct {
	bufw *bufio.Writer
}

func (w *SQLWriter) WriteValue(value constant.Value) error {
	panic("unimplemented")
}

func (w *SQLWriter) WriteFileHeader(table *Table) error {
	panic("unimplemented")
}

func (w *SQLWriter) WriteRowGroupHeader(_ *Table) error {
	panic("unimplemented")
}

func (w *SQLWriter) WriteValueHeader(_ template.Name) error {
	panic("unimplemented")
}

func (w *SQLWriter) WriteValueSeparator() error {
	panic("unimplemented")
}

func (w *SQLWriter) WriteRowSeparator() error {
	panic("unimplemented")
}

func (w *SQLWriter) WriteRowGroupTrailer() error {
	panic("unimplemented")
}

type SQLInsertSetWriter struct {
	bufw *bufio.Writer
}

func (w *SQLInsertSetWriter) WriteValue(value constant.Value) error {
	panic("unimplemented")
}

func (w *SQLInsertSetWriter) WriteFileHeader(table *Table) error {
	panic("unimplemented")
}

func (w *SQLInsertSetWriter) WriteRowGroupHeader(_ *Table) error {
	panic("unimplemented")
}

func (w *SQLInsertSetWriter) WriteValueHeader(_ template.Name) error {
	panic("unimplemented")
}

func (w *SQLInsertSetWriter) WriteValueSeparator() error {
	panic("unimplemented")
}

func (w *SQLInsertSetWriter) WriteRowSeparator() error {
	panic("unimplemented")
}

func (w *SQLInsertSetWriter) WriteRowGroupTrailer() error {
	panic("unimplemented")
}
