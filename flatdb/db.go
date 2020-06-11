package flatdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type SQL struct {
	db *sql.DB
}

func (s *SQL) Query(ctx context.Context, query string, arg map[string]interface{}) (*sql.Rows, error) {
	q, args, err := compileNamedQuery(query, arg)
	if err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (s *SQL) Exec(ctx context.Context, query string, arg map[string]interface{}) (sql.Result, error) {
	q, args, err := compileNamedQuery(query, arg)
	if err != nil {
		return nil, err
	}

	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}

	return res, err
}

// Compile a named query into an unbound query (using '?') and arguments.
func compileNamedQuery(query string, names map[string]interface{}) (string, []interface{}, error) {
	var args = make([]interface{}, 0, len(names))

	var rebound strings.Builder // Query builder
	var name strings.Builder    // Name builder

	var inName = false
	for i, r := range query {
		// Encounter a name indicator
		if r == ':' {
			if inName {
				return "", nil, fmt.Errorf("name %q contains more than one %q", name.String(), ":")
			}

			inName = true

			continue
		}

		if inName {
			hasMore := i < len(query)-1

			if r != ' ' {
				name.WriteRune(r)

				// Handle the case the if the name in the last word in the query.
				if hasMore {
					continue
				}
			}

			arg, ok := names[name.String()]
			if !ok {
				return "", nil, fmt.Errorf("name %q does not exist in the map", name.String())
			}

			rebound.WriteString("?")
			if hasMore {
				rebound.WriteRune(' ')
			}

			args = append(args, arg)

			inName = false
			name.Reset()

			continue
		}

		rebound.WriteRune(r)
	}

	return rebound.String(), args, nil
}
