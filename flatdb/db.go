package flatdb

import (
	"database/sql"
	"fmt"
	"strings"
)

type SQL struct {
	db *sql.DB
}

func (s *SQL) Query(query string, params map[string]string) (*sql.Rows, error) {
	var args = make([]interface{}, 0, len(params))

	var qb strings.Builder // Query builder
	var pb strings.Builder // Parameter name builder
	var inParam = false
	for i, r := range query {
		// Encounter a parameter indicator
		if r == ':' {
			if inParam {
				return nil, fmt.Errorf("query %q contains more than one %q", query, ":")
			}

			inParam = true

			continue
		}

		if inParam {
			if r != ' ' {
				pb.WriteRune(r)

				// Handle the case the if the param in the last word in the query.
				if i < len(query)-1 {
					continue
				}
			}

			// Write the parameter value into the query builder.

			arg, ok := params[pb.String()]
			if !ok {
				return nil, fmt.Errorf("param %q does not exist in the map", pb.String())
			}

			qb.WriteString("? ")
			args = append(args, arg)

			inParam = false
			pb.Reset()

			continue
		}

		qb.WriteRune(r)
	}

	rows, err := s.db.Query(qb.String(), args...)
	if err != nil {
		return nil, err
	}

	return rows, err
}
