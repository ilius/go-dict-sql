package sqldict

import (
	"database/sql/driver"
	"fmt"
	"log"
	"regexp"

	"modernc.org/sqlite"
)

func init() {
	err := defineRegexp()
	if err != nil {
		log.Println(err)
	}
}

func defineRegexp() error {
	const argc = 2
	return sqlite.RegisterDeterministicScalarFunction(
		"regexp",
		argc,
		func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			s1 := args[0].(string)
			s2 := args[1].(string)
			matched, err := regexp.MatchString(s1, s2)
			if err != nil {
				return nil, fmt.Errorf("bad regular expression: %q", err)
			}
			return matched, nil
		},
	)
}
