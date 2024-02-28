package errors

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrPgCode refers to https://www.postgresql.org/docs/current/errcodes-appendix.html

const (
	ErrPgCodeUniqueConstraints = "23505"
)

func IsPgErrorCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == code {
		return true
	}
	return false
}
