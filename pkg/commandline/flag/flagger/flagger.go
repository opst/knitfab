package flagger

import (
	"flag"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

type Flag struct {
	Name      string
	ShortName string
	Help      string
	MetaVar   string
	ptr       reflect.Value
}

func (f Flag) SetFlag(fs *flag.FlagSet) error {
	switch dv := f.ptr.Interface().(type) {
	case *bool:
		fs.BoolVar(dv, f.Name, *dv, f.Help)
		if f.ShortName != "" {
			fs.BoolVar(dv, f.ShortName, *dv, fmt.Sprintf("alias for %s", f.Name))
		}
	case *string:
		fs.StringVar(dv, f.Name, *dv, f.Help)
		if f.ShortName != "" {
			fs.StringVar(dv, f.ShortName, *dv, fmt.Sprintf("alias for %s", f.Name))
		}
	case *int:
		fs.IntVar(dv, f.Name, *dv, f.Help)
		if f.ShortName != "" {
			fs.IntVar(dv, f.ShortName, *dv, fmt.Sprintf("alias for %s", f.Name))
		}
	case *int64:
		fs.Int64Var(dv, f.Name, *dv, f.Help)
		if f.ShortName != "" {
			fs.Int64Var(dv, f.ShortName, *dv, fmt.Sprintf("alias for %s", f.Name))
		}
	case *uint:
		fs.UintVar(dv, f.Name, *dv, f.Help)
		if f.ShortName != "" {
			fs.UintVar(dv, f.ShortName, *dv, fmt.Sprintf("alias for %s", f.Name))
		}
	case *uint64:
		fs.Uint64Var(dv, f.Name, *dv, f.Help)
		if f.ShortName != "" {
			fs.Uint64Var(dv, f.ShortName, *dv, fmt.Sprintf("alias for %s", f.Name))
		}
	case *float64:
		fs.Float64Var(dv, f.Name, *dv, f.Help)
		if f.ShortName != "" {
			fs.Float64Var(dv, f.ShortName, *dv, fmt.Sprintf("alias for %s", f.Name))
		}
	case *time.Duration:
		fs.DurationVar(dv, f.Name, *dv, f.Help)
		if f.ShortName != "" {
			fs.DurationVar(dv, f.ShortName, *dv, fmt.Sprintf("alias for %s", f.Name))
		}
	case flag.Value:
		fs.Var(dv, f.Name, f.Help)
		if f.ShortName != "" {
			fs.Var(dv, f.ShortName, fmt.Sprintf("alias for %s", f.Name))
		}
	default:
		return fmt.Errorf("unsupported type: %T (it must be pointer)", dv)
	}

	return nil
}

func (f Flag) String() string {
	str := "--" + f.Name
	if f.ShortName != "" {
		str += "|-" + f.ShortName
	}

	switch val := f.ptr.Interface().(type) {
	case *bool:
		str = "[" + str + "]"
	case *string:
		metavar := f.MetaVar
		if metavar == "" {
			metavar = *val
		}
		str += fmt.Sprintf(`=%s`, metavar)
	case *int:
		metavar := f.MetaVar
		if metavar == "" {
			metavar = fmt.Sprintf(`%d`, *val)
		}
		str += "=" + metavar
	case *int64:
		metavar := f.MetaVar
		if metavar == "" {
			metavar = fmt.Sprintf(`%d`, *val)
		}
		str += "=" + metavar
	case *uint:
		metavar := f.MetaVar
		if metavar == "" {
			metavar = fmt.Sprintf(`%d`, *val)
		}
		str += "=" + metavar
	case *uint64:
		metavar := f.MetaVar
		if metavar == "" {
			metavar = fmt.Sprintf(`%d`, *val)
		}
		str += "=" + metavar
	case *float64:
		metavar := f.MetaVar
		if metavar == "" {
			metavar = fmt.Sprintf(`%f`, *val)
		}
		str += "=" + metavar
	case *time.Duration:
		metavar := f.MetaVar
		if metavar == "" {
			metavar = fmt.Sprintf(`"%s"`, *val)
		}
		str += "=" + metavar
	case flag.Value:
		metavar := f.MetaVar
		if metavar == "" {
			metavar = val.String()
		}
		str += fmt.Sprintf(`=%s`, metavar)
	default:
		metavar := f.MetaVar
		if metavar == "" {
			metavar = fmt.Sprintf("%s", val)
		}
		str += fmt.Sprintf(`=%s (unsupported)`, metavar)
	}

	return str
}

// Flagger is a struct to set flags.
type Flagger[T any] struct {
	Flags  []Flag
	Values *T
}

// New returns new Flagger.
//
// # Example
//
//	type MyFlags struct {
//		Flag1 string `flag:"flag1,help=message for flag1"`
//		Flag2 int    `flag:"custom-flag-name,short=f"`
//		Flag3 string `flag:",metavar=SOMETHING GOOD"` // flag name is "flag3" after the field name,
//	}
//
//	func main() {
//		flags := MyFlags{
//			Flag1: "default value",
//			Flag2: 100,
//		}
//		f, err := New(&flags)
//		fmt.Println(f)
//		// --flag1="default value" --custom-flag-name=100 --flag3="SOMETHING GOOD"
//
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		fs := flag.NewFlagSet("myflags", flag.ExitOnError)
//		fs = f.SetFlags(fs)
//		fs.Parse("--flag1", "new value", "-f", "100", "-flag3", "another value")
//		fmt.Println(flags.Flag1)  // new value
//		fmt.Println(flags.Flag2)  // 100
//		fmt.Println(flags.Flag3)  // another value
//	}
//
// # Tags
//
// As you see in above, New uses "flag" tag.
//
// The first element is the (long) name of the flag.
//
// If omitted (like `flag:",..."`), the name of the flag is determined by the tagged field name.
//
// This tag can have attributes below, all of them are optional:
//
// - short: short name of the flag.
//
// - help: help message for the flag.
//
// - metavar: metavar in explanation of the flag. If omitted, the default value of the field is used.
//
// # Args
//
// - v any: struct to be set flags. It must be pointer to struct.
//
// # Returns
//
// - *Flagger
//
// # Panics
//
// - if v is not struct.
func New[T any](v T) *Flagger[T] {
	flgr := &Flagger[T]{Values: &v}

	rv := reflect.ValueOf(flgr.Values)
	if rv.Elem().Kind() != reflect.Struct {
		panic("flag receiver must be struct")
	}

	rt := rv.Elem().Type()
	flags := make([]Flag, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}

		flg := Flag{}
		{
			tagFlag, ok := f.Tag.Lookup("flag")
			if !ok {
				continue
			}
			attrs := strings.Split(tagFlag, ",")
			if 0 < len(attrs) {
				flg.Name = attrs[0]
				attrs = attrs[1:]
			}
			if flg.Name == "" {
				flg.Name = reUpper.ReplaceAllString(f.Name, "-${0}")
				flg.Name = strings.ToLower(flg.Name)[1:] // trim first "-"
			}

			for _, a := range attrs {
				name, value, _ := strings.Cut(a, "=")
				switch name {
				case "short":
					flg.ShortName = value
				case "help":
					flg.Help = value
				case "metavar":
					flg.MetaVar = value
				}
			}

			if _, ok := rv.Elem().Field(i).Interface().(flag.Value); ok {
				flg.ptr = rv.Elem().Field(i)
			} else {
				flg.ptr = rv.Elem().Field(i).Addr()
			}
		}
		flags = append(flags, flg)
	}

	flgr.Flags = flags
	return flgr
}

func (f *Flagger[T]) SetFlags(fs *flag.FlagSet) (*flag.FlagSet, error) {
	for _, flag := range f.Flags {
		if err := flag.SetFlag(fs); err != nil {
			return nil, err
		}
	}
	return fs, nil
}

var reUpper *regexp.Regexp = regexp.MustCompile("[A-Z][^A-Z]+")

func (f *Flagger[T]) String() string {
	var strs []string
	for _, flag := range f.Flags {
		strs = append(strs, flag.String())
	}
	return strings.Join(strs, " ")
}
