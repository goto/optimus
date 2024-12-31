package parser_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/plugin/upstream_identifier/parser"
)

func TestParseTopLevelUpstreamsFromQuery(t *testing.T) {
	t.Run("parse test", func(t *testing.T) {
		testCases := []struct {
			Name           string
			InputQuery     string
			ExpectedTables []string
		}{
			{
				Name:           "empty query",
				InputQuery:     "",
				ExpectedTables: []string{},
			},
			{
				Name:       "simple query",
				InputQuery: "select * from data-engineering.testing.table1",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table1"),
				},
			},
			{
				Name:       "simple query with hyphenated table name",
				InputQuery: "select * from data-engineering.testing.table_name-1",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table_name-1"),
				},
			},
			{
				Name:       "simple query with quotes",
				InputQuery: "select * from `data-engineering.testing.table1`",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table1"),
				},
			},
			{
				Name:       "simple query with quotes in schema / dataset",
				InputQuery: "select * from data-engineering.`testing`.table1",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table1"),
				},
			},
			{
				Name:           "simple query without project name",
				InputQuery:     "select * from testing.table1",
				ExpectedTables: []string{},
			},
			{
				Name:       "simple query with simple join",
				InputQuery: "select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table1"),
					newTable("data-engineering", "testing", "table2"),
				},
			},
			{
				Name:       "simple query with outer join",
				InputQuery: "select * from data-engineering.testing.table1 outer join data-engineering.testing.table2 on some_field",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table1"),
					newTable("data-engineering", "testing", "table2"),
				},
			},
			{
				Name:       "subquery",
				InputQuery: "select * from (select order_id from data-engineering.testing.orders)",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "orders"),
				},
			},
			{
				Name:       "`with` clause + simple query",
				InputQuery: "with `information.foo.bar` as (select * from `data-engineering.testing.data`) select * from `information.foo.bar`",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "data"),
				},
			},
			{
				Name:       "`with` clause with missing project name",
				InputQuery: "with `foo.bar` as (select * from `data-engineering.testing.data`) select * from `foo.bar`",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "data"),
				},
			},
			{
				Name:       "project name with dashes",
				InputQuery: "select * from `foo-bar.baz.data`",
				ExpectedTables: []string{
					newTable("foo-bar", "baz", "data"),
				},
			},
			{
				Name:       "dataset and project name with dashes",
				InputQuery: "select * from `foo-bar.bar-baz.data",
				ExpectedTables: []string{
					newTable("foo-bar", "bar-baz", "data"),
				},
			},
			{
				Name:       "`with` clause + join",
				InputQuery: "with dedup_source as (select * from `project.fire.fly`) select * from dedup_source join `project.maximum.overdrive` on dedup_source.left = `project.maximum.overdrive`.right",
				ExpectedTables: []string{
					newTable("project", "fire", "fly"),
					newTable("project", "maximum", "overdrive"),
				},
			},
			{
				Name:       "double `with` + pseudoreference",
				InputQuery: "with s1 as (select * from internal.pseudo.ref), with internal.pseudo.ref as (select * from `project.another.name`) select * from s1",
				ExpectedTables: []string{
					newTable("project", "another", "name"),
				},
			},
			{
				Name:           "simple query that ignores from upstream",
				InputQuery:     "select * from /* @ignoreupstream */ data-engineering.testing.table1",
				ExpectedTables: []string{},
			},
			{
				Name:           "simple query that ignores from upstream with quotes",
				InputQuery:     "select * from /* @ignoreupstream */ `data-engineering.testing.table1`",
				ExpectedTables: []string{},
			},
			{
				Name:       "simple query with simple join that ignores from upstream",
				InputQuery: "select * from /* @ignoreupstream */ data-engineering.testing.table1 join data-engineering.testing.table2 on some_field",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table2"),
				},
			},
			{
				Name:       "simple query with simple join that has comments but does not ignores upstream",
				InputQuery: "select * from /*  */ data-engineering.testing.table1 join data-engineering.testing.table2 on some_field",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table1"),
					newTable("data-engineering", "testing", "table2"),
				},
			},
			{
				Name:       "simple query with simple join that ignores upstream of join",
				InputQuery: "select * from data-engineering.testing.table1 join /* @ignoreupstream */ data-engineering.testing.table2 on some_field",
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table1"),
				},
			},
			{
				Name: "simple query with an ignoreupstream for an alias should still consider it as dependency",
				InputQuery: `
					WITH my_temp_table AS (
						SELECT id, name FROM data-engineering.testing.an_upstream_table
					)
					SELECT id FROM /* @ignoreupstream */ my_temp_table
					`,
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "an_upstream_table"),
				},
			},
			{
				Name: "simple query should have alias in the actual name rather than with alias",
				InputQuery: `
					WITH my_temp_table AS (
						SELECT id, name FROM /* @ignoreupstream */ data-engineering.testing.an_upstream_table
					)
					SELECT id FROM my_temp_table
					`,
				ExpectedTables: []string{},
			},
			{
				Name:           "simple query with simple join that ignores upstream of join",
				InputQuery:     "WITH my_temp_table AS ( SELECT id, name FROM /* @ignoreupstream */ data-engineering.testing.an_upstream_table ) SELECT id FROM /* @ignoreupstream */ my_temp_table",
				ExpectedTables: []string{},
			},
			{
				Name: "simple query with another query inside comment",
				InputQuery: `
					select * from data-engineering.testing.tableABC
					-- select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field
					`,
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "tableABC"),
				},
			},
			{
				Name: "query with another query inside comment and a join that uses helper",
				InputQuery: `
					select * from data-engineering.testing.tableABC
					/* select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field */
					join /* @ignoreupstream */ data-engineering.testing.table2 on some_field
					`,
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "tableABC"),
				},
			},
			{
				Name: "ignore `create view` in ddl query",
				InputQuery: `
					create view data-engineering.testing.tableABC
					select *
					from
						data-engineering.testing.tableDEF,
					`,
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "tableDEF"),
				},
			},
			{
				Name: "one or more sources are stated together under from clauses",
				InputQuery: `
					select *
					from
						pseudo_table1,
						` + "`data-engineering.testing.tableABC`," + `
						pseudo_table2 as pt2
						` + "`data-engineering.testing.tableDEF`," + ` as backup_table,
						/* @ignoreupstream */ data-engineering.testing.tableGHI as ignored_table,
					`,
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "tableABC"),
					newTable("data-engineering", "testing", "tableDEF"),
				},
			},
			{
				Name: "one or more sources are from wild-card query",
				InputQuery: `
					select *
					from data-engineering.testing.tableA*

					select *
					from ` +
					"`data-engineering.testing.tableB*`" + `

					select *
					from
						/*@ignoreupstream*/ data-engineering.testing.tableC*
					`,
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "tableA*"),
					newTable("data-engineering", "testing", "tableB*"),
				},
			},
			{
				Name: "have quote in one part of table name",
				InputQuery: `
					select *
					from ` +
					"data-engineering.`testing`.tableB" + `

					select *
					from
						/*@ignoreupstream*/ data-engineering.testing.tableC
					`,
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "tableB"),
				},
			},
			{
				Name: "ignore characters after -- comment",
				InputQuery: `
				-- sources
				-- data-engineering.testing.table_a
				--
				-- related
				-- ` + "`data-engineering.testing.table_b`" + `
				-- from data-engineering.testing.table_c

				select *
				from data-engineering.testing.table_a
				join /* @ignoreupstream */ data-engineering.testing.table_d
				`,
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table_a"),
				},
			},
			{
				Name: "ignore characters within multi-line comment /* (separate line) */",
				InputQuery: `
				/*
				this the following relates to this table:

					with ` + "`data-engineering.testing.tabel_b`" + `
					from data-engineering.testing.tabel_c
				*/

				select *
				from
					data-engineering.testing.table_a
				join
					data-engineering.testing.table_d
				join
					/* @ignoreupstream */ data-engineering.testing.table_e
				`,
				ExpectedTables: []string{
					newTable("data-engineering", "testing", "table_a"),
					newTable("data-engineering", "testing", "table_d"),
				},
			},
			{
				Name:           "ignore merge into query",
				InputQuery:     "merge into `data-engineering.testing.table_a` as target",
				ExpectedTables: []string{},
			},
			{
				Name:           "ignore insert into query",
				InputQuery:     "insert into `data-engineering.testing.table_a`(id,name)",
				ExpectedTables: []string{},
			},
			{
				Name:           "ignore delete + insert query",
				InputQuery:     "delete from `data-engineering.testing.table_b`; create or replace table `data-engineering.testing.table_b`",
				ExpectedTables: []string{},
			},
			{
				Name:           "ignore create or replace query",
				InputQuery:     "create or replace table `data-engineering.testing.table_b`",
				ExpectedTables: []string{},
			},
		}
		for _, test := range testCases {
			t.Run(test.Name, func(t *testing.T) {
				actualResourceURNs := parser.ParseTopLevelUpstreamsFromQuery(test.InputQuery)
				assert.ElementsMatch(t, test.ExpectedTables, actualResourceURNs)
			})
		}
	})
}

func newTable(project, dataset, name string) string {
	return fmt.Sprintf("%s.%s.%s", project, dataset, name)
}
