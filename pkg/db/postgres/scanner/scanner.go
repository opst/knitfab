package scanner

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type Queryer interface {
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
}

// type-safe scanner for pgx.Rows
//
// # example
//
//		type FizzBuzz struct {
//			Num int
//			Answer string
//		}
//
//		func GetAllFizzBuzz(ctx context.Context, conn pgx.Conn) ([]FizzBuzz, error) {
//			rows, err := conn.Query(ctx, `select "num", "answer" from "fizz_buzz"`)
//			if err != nil {
//				return nil, err
//			}
//			defer rows.Close()
//
//			return scanner.New[FizzBuzz]().ScanAll(rows)
//		}
//
//	example above is equiverent with below
//
//		func GetAllFizzBuzz(ctx context.Context, conn pgx.Conn) ([]FizzBuzz, error) {
//			return scanner.New[FizzBuzz]().QueryAll(ctx, conn, `select "num", "answer" from "fizz_buzz"`)
//		}
//
// # mapping rule
//
//	columns are mapped into
//
//		1. field with tag `sql:"column_name"`
//		2. or, field named as same as the column name
//		3. or, field which has a name in CamelCase version of column name.
//
//	In case 3, next characters of underscores can be lower or upper,
//	but they should be consistent in a field.
//
//	For example, column named "aa_bb__cc___dd" is mapped into field
//
//		- with tag `sql:"aa_bb__cc___dd"`  (the most priority)
//		- named "aa_bb__cc___dd"
//		- named "AaBb_Cc__Dd"
//		- named "AaBb_cc__dd"  (the worst priority)
//
//	Note that "AaBb_cc__Dd" or "AaBb_Cc__dd" are ignored.
type Scanner[T any] interface {
	// scan all rows in pgx.Rows and convert to []T
	ScanAll(pgx.Rows) ([]T, error)

	// scan all rows in response of query.
	QueryAll(context.Context, Queryer, string, ...interface{}) ([]T, error)
}

type scanner[T any] struct {
	mapByTag       map[string]reflect.StructField
	mapByFieldName map[string]reflect.StructField
	mux            sync.Mutex
}

func New[T any]() Scanner[T] {

	val := *new(T)
	tval := reflect.TypeOf(val)

	// special case: timestamp or bytes columns
	if tval.AssignableTo(reflect.TypeOf(time.Time{})) || tval.AssignableTo(reflect.TypeOf([]byte{})) {
		return &singleColumnScanner[T]{mux: sync.Mutex{}}
	}

	switch tval.Kind() {
	case
		// primitives
		reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:

		return &singleColumnScanner[T]{mux: sync.Mutex{}}
	}

	// some structs.
	mapByTag := map[string]reflect.StructField{}
	mapByFieldName := map[string]reflect.StructField{}

	pt := reflect.ValueOf(*new(T)).Type()
	for i := 0; i < pt.NumField(); i++ {
		f := pt.Field(i)
		mapByFieldName[f.Name] = f
		if tag, ok := f.Tag.Lookup("sql"); ok {
			mapByTag[tag] = f
		}
	}

	return &scanner[T]{mapByTag: mapByTag, mapByFieldName: mapByFieldName, mux: sync.Mutex{}}
}

func camel(s string) string {
	b := &strings.Builder{}
	for _, ss := range strings.Split(s, "_") {
		if len(ss) == 0 {
			b.WriteString("_")
			continue
		}
		b.WriteString(strings.ToUpper(ss[0:1]))
		b.WriteString(ss[1:])
	}

	return b.String()
}
func camelAndSnail(s string) string {
	b := &strings.Builder{}
	underscore := false
	for _, ss := range strings.Split(s, "_") {
		if len(ss) == 0 {
			b.WriteString("_")
			underscore = true
			continue
		}
		if underscore {
			b.WriteString(ss)
		} else {
			b.WriteString(strings.ToUpper(ss[0:1]))
			b.WriteString(ss[1:])
		}
		underscore = false
	}

	return b.String()

}

func (s *scanner[T]) ScanAll(rows pgx.Rows) ([]T, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	fields := make([]reflect.StructField, 0, len(rows.FieldDescriptions()))
	sqlColumns := rows.FieldDescriptions()
	for _, fd := range sqlColumns {
		col := string(fd.Name)

		var field reflect.StructField
		if f, ok := s.mapByTag[col]; ok {
			field = f
		} else if f, ok := s.mapByFieldName[col]; ok {
			field = f
		} else if f, ok := s.mapByFieldName[camel(col)]; ok {
			field = f
		} else if f, ok := s.mapByFieldName[camelAndSnail(col)]; ok {
			field = f
		} else {
			return nil, fmt.Errorf(
				`field for column "%s" is not found in type "%T"`,
				col, *new(T),
			)
		}
		fields = append(fields, field)
	}

	ret := make([]T, 0, rows.CommandTag().RowsAffected())
	// rtSqlScanner := reflect.TypeOf((*sql.Scanner)(nil)).Elem()
	for rows.Next() {
		elem := new(T)
		re := reflect.ValueOf(elem)
		rr := reflect.ValueOf(rows)

		fldPtr := make([]reflect.Value, len(fields))
		for nth, f := range fields {
			fldPtr[nth] = re.Elem().FieldByName(f.Name).Addr()
		}

		rret := rr.MethodByName("Scan").Call(fldPtr)
		if len(rret) != 1 {
			return nil, fmt.Errorf("unexpected return value from pgx.Rows.Scan: %v", rret)
		}
		if err, ok := rret[0].Interface().(error); ok {
			if err != nil {
				return nil, err
			}
		}
		ret = append(ret, *elem)
	}
	return ret, nil
}

func (s *scanner[T]) QueryAll(ctx context.Context, conn Queryer, q string, params ...interface{}) ([]T, error) {
	rows, err := conn.Query(ctx, q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.ScanAll(rows)
}

type singleColumnScanner[T any] struct {
	mux sync.Mutex
}

func (s *singleColumnScanner[T]) ScanAll(rows pgx.Rows) ([]T, error) {

	sqlColumns := rows.FieldDescriptions()
	if len(sqlColumns) != 1 {
		name := reflect.ValueOf(*new(T)).Type().Name()
		return nil, fmt.Errorf(`too much columns for %s`, name)
	}

	ret := make([]T, 0, rows.CommandTag().RowsAffected())
	for rows.Next() {
		elem := new(T)
		field := reflect.ValueOf(elem).Elem()

		sqlValues, err := rows.Values()
		if err != nil {
			return nil, err
		}

		for nth, sqlv := range sqlValues {
			if _sqlv := reflect.ValueOf(sqlv); !_sqlv.CanConvert(field.Type()) {
				return nil, fmt.Errorf(
					`field "%s" (type: %s in sql, %T in golang) can not be convert to "%T"`,
					sqlColumns[nth].Name, pgOID2String(sqlColumns[nth].DataTypeOID), sqlv, *elem,
				)
			}
			v := reflect.ValueOf(sqlv).Convert(field.Type())
			field.Set(v)
		}

		ret = append(ret, *elem)
	}
	return ret, nil
}

func (s *singleColumnScanner[T]) QueryAll(ctx context.Context, conn Queryer, q string, params ...interface{}) ([]T, error) {
	rows, err := conn.Query(ctx, q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.ScanAll(rows)
}

func pgOID2String(oid uint32) string {
	switch oid {
	case pgtype.BoolOID:
		return "bool"
	case pgtype.ByteaOID:
		return "bytea"
	case pgtype.QCharOID:
		return "qchar"
	case pgtype.NameOID:
		return "name"
	case pgtype.Int8OID:
		return "int8"
	case pgtype.Int2OID:
		return "int2"
	case pgtype.Int4OID:
		return "int4"
	case pgtype.TextOID:
		return "text"
	case pgtype.OIDOID:
		return "oid"
	case pgtype.TIDOID:
		return "tid"
	case pgtype.XIDOID:
		return "xid"
	case pgtype.CIDOID:
		return "cid"
	case pgtype.JSONOID:
		return "json"
	case pgtype.PointOID:
		return "point"
	case pgtype.LsegOID:
		return "lseg"
	case pgtype.PathOID:
		return "path"
	case pgtype.BoxOID:
		return "box"
	case pgtype.PolygonOID:
		return "polygon"
	case pgtype.LineOID:
		return "line"
	case pgtype.CIDROID:
		return "cidr"
	case pgtype.CIDRArrayOID:
		return "cidr[]"
	case pgtype.Float4OID:
		return "float4"
	case pgtype.Float8OID:
		return "float8"
	case pgtype.CircleOID:
		return "circle"
	case pgtype.UnknownOID:
		return "unknown"
	case pgtype.MacaddrOID:
		return "macaddr"
	case pgtype.InetOID:
		return "inet"
	case pgtype.BoolArrayOID:
		return "bool[]"
	case pgtype.Int2ArrayOID:
		return "int2[]"
	case pgtype.Int4ArrayOID:
		return "int4[]"
	case pgtype.TextArrayOID:
		return "text[]"
	case pgtype.ByteaArrayOID:
		return "bytea[]"
	case pgtype.BPCharArrayOID:
		return "bpchar[]"
	case pgtype.VarcharArrayOID:
		return "varchar[]"
	case pgtype.Int8ArrayOID:
		return "int8[]"
	case pgtype.Float4ArrayOID:
		return "float4[]"
	case pgtype.Float8ArrayOID:
		return "float8[]"
	case pgtype.ACLItemOID:
		return "aclitem"
	case pgtype.ACLItemArrayOID:
		return "aclitem[]"
	case pgtype.InetArrayOID:
		return "inet[]"
	case pgtype.BPCharOID:
		return "bpchar[]"
	case pgtype.VarcharOID:
		return "varchar"
	case pgtype.DateOID:
		return "date"
	case pgtype.TimeOID:
		return "time"
	case pgtype.TimestampOID:
		return "timestamp"
	case pgtype.TimestampArrayOID:
		return "timestamp[]"
	case pgtype.DateArrayOID:
		return "date[]"
	case pgtype.TimestamptzOID:
		return "timestamptz"
	case pgtype.TimestamptzArrayOID:
		return "timestamptz[]"
	case pgtype.IntervalOID:
		return "interval"
	case pgtype.NumericArrayOID:
		return "numeric[]"
	case pgtype.BitOID:
		return "bit"
	case pgtype.VarbitOID:
		return "varbit"
	case pgtype.NumericOID:
		return "numeric"
	case pgtype.RecordOID:
		return "record"
	case pgtype.UUIDOID:
		return "uuid"
	case pgtype.UUIDArrayOID:
		return "uuid[]"
	case pgtype.JSONBOID:
		return "jsonb"
	case pgtype.JSONBArrayOID:
		return "jsonb[]"
	case pgtype.DaterangeOID:
		return "daterange"
	case pgtype.Int4rangeOID:
		return "int4range"
	case pgtype.Int4multirangeOID:
		return "int4multirange"
	case pgtype.NumrangeOID:
		return "numrange"
	case pgtype.NummultirangeOID:
		return "nummultirange"
	case pgtype.TsrangeOID:
		return "tsrange"
	case pgtype.TsrangeArrayOID:
		return "tsrange[]"
	case pgtype.TstzrangeOID:
		return "tstzrange"
	case pgtype.TstzrangeArrayOID:
		return "tstzrange[]"
	case pgtype.Int8rangeOID:
		return "int8range"
	case pgtype.Int8multirangeOID:
		return "int8multirange"
	}

	return fmt.Sprintf("undefined oid(%d)", oid)
}
