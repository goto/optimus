package processor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/plugin/upstream_identifier/processor"
)

func TestCleanQueryFromComment(t *testing.T) {
	query := `SELECT * FROM /* @ignoreupstream */ table -- single line comment
	/* multi
	line
	comment */`
	expected := `SELECT * FROM /* @ignoreupstream */ table 
	`
	cleanedQuery := processor.CleanQueryFromComment(query)
	assert.Equal(t, expected, cleanedQuery)
}

func TestCleanQuery(t *testing.T) {
	query := `SELECT * FROM /* @ignoreupstream */ table -- single line comment
	/* multi
	line
	comment */`
	expected := `SELECT * FROM  table 
	`
	cleanedQuery := processor.CleanQuery(query)
	assert.Equal(t, expected, cleanedQuery)
}
