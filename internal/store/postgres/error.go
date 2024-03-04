package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrPgCode refers to https://www.postgresql.org/docs/current/errcodes-appendix.html
type ErrPgCode string

func (e ErrPgCode) String() string         { return string(e) }
func (e ErrPgCode) Equal(code string) bool { return e.String() == code }

const (
	ErrPgCodeUniqueConstraints ErrPgCode = "23505"
)

func ErrorCodeEqual(err error, code ErrPgCode) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && code.Equal(pgErr.Code) {
		return true
	}
	return false
}
