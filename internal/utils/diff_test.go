package utils_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/utils"
)

type Type3 struct {
	Type31 string
	Type32 int
}

type Type2 struct {
	SomeField  string
	SomeField1 int
}

type Type1 struct {
	Field1 string
	Field2 int
	Field3 map[string]string
	Field4 Type2
	Field5 float64
	Field6 *Type3
	Field7 []string
}

func assertDiff(t *testing.T, expected map[string]string, diff []utils.Diff) {
	t.Helper()
	assert.Equal(t, len(expected), len(diff))
	for _, d := range diff {
		_, ok := expected[d.Field]
		assert.True(t, ok, "%s was not expected to be in diff", d)
	}
	for key, msg := range expected {
		var found bool
		for _, diff := range diff {
			if diff.Field == key {
				found = true
				break
			}
		}
		assert.Truef(t, found, "expected %s to be found in diff, failed %s", key, msg)
	}
}

func TestGetDiffs(t *testing.T) {
	type3Object := Type3{
		Type31: "baseValue",
		Type32: -12,
	}
	type2Object := Type2{
		SomeField:  "k",
		SomeField1: 1,
	}

	baseObject := Type1{
		Field1: "some",
		Field2: 1,
		Field3: map[string]string{
			"a": "c",
			"B": "d",
		},
		Field4: type2Object,
		Field5: 11.0,
		Field6: &type3Object,
	}
	t.Run("get diff of two interface of same kind", func(t *testing.T) {
		t.Run("case involving maps and raw json", func(t *testing.T) {
			type newType4 string

			type newType3 struct {
				A map[string]string
				B map[string]int
			}

			type newType2 struct {
				A json.RawMessage
				B newType3
			}

			type newType struct {
				A string
				B newType2
				C newType4
				D int
			}

			o1 := newType{
				A: "abcd",
				B: newType2{
					A: json.RawMessage("{\n  \"name\": \"John Doe\",\n  \"age\": 30,\n  \"city\": \"New York\",\n  \"isMarried\": true,\n  \"children\": [\"Alice\", \"Bob\"],\n  \"address\": {\n    \"street\": \"123 Main Street\",\n    \"city\": \"New York\",\n    \"zip\": \"10001\"\n  }\n}"),
					B: newType3{
						A: map[string]string{
							"A": "a",
							"B": "B",
						},
						B: map[string]int{
							"A": 1,
							"B": 2,
						},
					},
				},
				C: "cake",
			}
			o2 := newType{
				A: "abcd",
				B: newType2{
					A: json.RawMessage("{\n  \"name\": \"John Doe1\",\n  \"age\": 30,\n  \"city\": \"New York\",\n  \"isMarried\": true,\n  \"children\": [\"Alice\", \"Bob1\"],\n  \"address\": {\n    \"street\": \"123 Main Street1\",\n    \"city\": \"New York\",\n    \"zip\": \"10001\"\n  }\n}"),
					B: newType3{
						A: map[string]string{
							"A": "a",
							"B": "b",
						},
						B: map[string]int{
							"B": 1,
							"A": 1,
						},
					},
				},
				C: "cake1",
			}
			expectedDiff := map[string]string{
				"B.A.children.indicesModified": "array in a raw json",
				"B.A.address.street":           "json object in a raw json",
				"B.A.name":                     "compare raw json",
				"B.B.A.B":                      "key in a map which is in a struct object",
				"B.B.B.B":                      "comparing int in a map",
				"C":                            "simple string",
			}

			diffs, err := utils.GetDiffs(o1, o2, nil)
			assert.Nil(t, err)
			assertDiff(t, expectedDiff, diffs)
		})
		t.Run("when objects are identical, diff must be 0 length", func(t *testing.T) {
			newType3Object := Type3{
				Type31: "baseValue",
				Type32: -12,
			}
			objectToCompare := Type1{
				Field1: "some",
				Field2: 1,
				Field3: map[string]string{
					"B": "d",
					"a": "c",
				},
				Field4: type2Object,
				Field5: 11.0,
				Field6: &newType3Object,
			}
			expectedDiff := map[string]string{}

			diffs, err := utils.GetDiffs(baseObject, objectToCompare, nil)
			assert.Nil(t, err)
			assertDiff(t, expectedDiff, diffs)
		})
		t.Run("when pointer types are involved", func(t *testing.T) {
			t.Run("if one of the pointer is nil", func(t *testing.T) {
				objectToCompare := Type1{
					Field1: "some",
					Field2: 1,
					Field3: map[string]string{
						"B": "d",
						"a": "c",
					},
					Field4: type2Object,
					Field5: 11.0,
				}
				expectedDiff := map[string]string{
					"Field6": "nil pointer comparison",
				}

				diffs, err := utils.GetDiffs(baseObject, objectToCompare, nil)
				assert.Nil(t, err)
				assertDiff(t, expectedDiff, diffs)
			})
			t.Run("if both the pointers are nil", func(t *testing.T) {
				objectToCompare := Type1{
					Field1: "some",
					Field2: 1,
					Field3: map[string]string{
						"B": "d",
						"a": "c",
					},
					Field4: type2Object,
					Field5: 11.0,
				}
				anotherObjectToCompare := Type1{
					Field1: "some",
					Field2: 1,
					Field3: map[string]string{
						"B": "d",
						"a": "c",
					},
					Field4: type2Object,
					Field5: 11.0,
				}
				expectedDiff := map[string]string{}

				diffs, err := utils.GetDiffs(objectToCompare, anotherObjectToCompare, nil)
				assert.Nil(t, err)
				assertDiff(t, expectedDiff, diffs)
			})
			t.Run("if both the pointers are not nil", func(t *testing.T) {
				t.Run("non identical", func(t *testing.T) {
					newType3Object := Type3{
						Type31: "baseValue",
						Type32: -121,
					}
					objectToCompare := Type1{
						Field1: "some",
						Field2: 1,
						Field3: map[string]string{
							"B": "d",
							"a": "c",
						},
						Field4: type2Object,
						Field5: 11.0,
						Field6: &newType3Object,
					}
					expectedDiff := map[string]string{
						"Field6.Type32": "pointer field comparison",
					}
					diffs, err := utils.GetDiffs(baseObject, objectToCompare, nil)
					assert.Nil(t, err)
					assertDiff(t, expectedDiff, diffs)
				})
			})
		})
		t.Run("when objects are not identical", func(t *testing.T) {
			t.Run("diff must not be empty", func(t *testing.T) {
				newType3Object := Type3{
					Type31: "baseValue",
					Type32: -122,
				}
				objectToCompare := Type1{
					Field1: "some",
					Field2: 2,
					Field3: map[string]string{
						"a": "c",
						"B": "c",
					},
					Field4: type2Object,
					Field5: 11.0,
					Field6: &newType3Object,
				}
				diffs, err := utils.GetDiffs(baseObject, objectToCompare, nil)
				assert.Nil(t, err)
				expectedDiff := map[string]string{
					"Field6.Type32": "nested objects test",
					"Field3.B":      "full map comparison",
					"Field2":        "integer comparison",
				}
				assertDiff(t, expectedDiff, diffs)
			})
			t.Run("comparing Large texts should return unified diffs", func(t *testing.T) {
				text1 := `
version: 1
name: sample_select
owner: "@data-batching-devs"
description: "dasDEsp"
schedule:
  start_date: "2021-08-01"
  interval: '*/5 * * * *'
behavior:
  depends_on_past: true
  retry: {}
  webhook:
   - "on": success
     endpoints:
      - url: https://dfa0-2401-4900-5fcf-dd77-1a9-d6d4-f35b-f6b9.ngrok-free.app
      - url: http://localhost:8001/path/to/the/webhook?some=somethingStatic
        headers:
          auth-header1: 'bearer: {{.secret.WEBHOOK_SECRET}}'
          header2: ''
   - "on": failure
     endpoints:
       - url: http://localhost:8000/path/to/the/webhook1
         headers:
          auth: 'bearer: {{.secret.WEBHOOK_SECRET}}'
   - "on": sla_miss
     endpoints:
       - url: http://localhost:8000/path/to/the/webhook1
         headers:
          auth: 'bearer: {{.secret.WEBHOOK_SECRET}}'
  notify:
    - "on": failure
      channels:
        - slack://#de-illuminati-alerts
task:
  name: bq2bq
  config:
    BQ_SERVICE_ACCOUNT: '{{.secret.BQ_SERVICE_ACCOUNT}}'
    DATASET: playground
    LOAD_METHOD: APPEND
    PROJECT: pilotdata-integration
    SQL_TYPE: STANDARD
    TABLE: sample_select1
    YASH: '{{.secret.BQ_SERVICE_ACCOUNT}}'
  window:
    size: 24h
    offset: "0"
    truncate_to: h
labels:
  orchestrator: optimus
  owner: yash
hooks: []
dependencies: []
metadata:
  resource:
    request:
      memory: 128Mi
      cpu: 250m
    limit:
      memory: 2048Mi
      cpu: 500m
  airflow:
    pool: ""
    queue: ""

`
				text2 := `
version: 1
name: sample_select
owner: "@data-batching-devs"
description: "dasDEsp"
schedule:
  start_date: "2021-08-01"
  interval: '*/5 * * * *'
behavior:
  depends_on_past: true
  retry: {}
  webhook:
   - "on": success
     endpoints:
      - url: https://dfa0-2401-4900-5fcf-dd77-1a9-d6d4-f35b-f6b9.ngrok-free.app
        headers:
          auth-header1: 'bearer: {{.secret.WEBHOOK_SECRET}}'
          header2: 'something static '
      - url: http://localhost:8001/path/to/the/webhook?some=somethingStatic
        headers:
          auth-header1: 'bearer: {{.secret.WEBHOOK_SECRET}}'
          header2: ''
   - "on": failure
     endpoints:
       - url: http://localhost:8000/path/to/the/webhook1
         headers:
          auth: 'bearer: {{.secret.WEBHOOK_SECRET}}'
   - "on": sla_miss
     endpoints:
       - url: http://localhost:8000/path/to/the/webhook1
         headers:
          auth: 'bearer: {{.secret.WEBHOOK_SECRET}}'
  notify:
    - "on": failure
      channels:
        - slack://#de-illuminati-alerts
task:
  name: bq2bq
  config:
    BQ_SERVICE_ACCOUNT: '{{.secret.BQ_SERVICE_ACCOUNT}}'
    DATASET: playground
    LOAD_METHOD: APPEND1
    PROJECT: pilotdata-integration
    SQL_TYPE: STANDARD
    TABLE: sample_select1
    YASH: '{{.secret.BQ_SERVICE_ACCOUNT}}'
  window:
    size: 24h
    offset: "0"
    truncate_to: h
    truncate_to: h
labels:
  orchestrator: optimus
  owner: yash


hooks: []
dependencies: []
metadata:
  resource:
    request:
      cpu: 250m
      memory: 128Mi
    limit:
      memory: 2048Mi
      cpu: 500m
  airflow:
    pool: ""
    queue: ""
`
				type spec struct {
					Assets map[string]string
				}
				baseObject1 := spec{
					Assets: map[string]string{
						"query.sql": text1,
					},
				}
				objectToCompare := spec{
					Assets: map[string]string{
						"query.sql": text2,
					},
				}

				diffs, err := utils.GetDiffs(baseObject1, objectToCompare, nil)
				assert.Nil(t, err)
				expectedDiff := map[string]string{
					"Assets.query.sql": "large string with unified diff",
				}
				assert.True(t, diffs[0].IsDiffTypeUnified())
				assertDiff(t, expectedDiff, diffs)
			})
			t.Run("comparing Large texts should return unified diffs1", func(t *testing.T) {
				text1 := "select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;"
				text2 := "select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;\n    select\n    EXTRACT(DAY1 FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;select\n    EXTRACT(DAY FROM CURRENT_TIMESTAMP()) + 8000 as `column1`,\n    CAST(\"{{ .EXECUTION_TIME }}\" AS TIMESTAMP) as `column2`;"
				type spec struct {
					Assets map[string]string
				}
				baseObject1 := spec{
					Assets: map[string]string{
						"query.sql": text1,
					},
				}
				objectToCompare := spec{
					Assets: map[string]string{
						"query.sql": text2,
					},
				}

				diffs, err := utils.GetDiffs(baseObject1, objectToCompare, nil)
				assert.Nil(t, err)
				expectedDiff := map[string]string{
					"Assets.query.sql": "large string with unified diff",
				}
				assert.True(t, diffs[0].IsDiffTypeUnified())
				assertDiff(t, expectedDiff, diffs)
			})

			t.Run("comparing small texts should return non unified diffs", func(t *testing.T) {
				type spec struct {
					Assets map[string]string
				}
				baseObject1 := spec{
					Assets: map[string]string{
						"app.py": `
def apple1():
	print("hello world!")
`,
					},
				}
				objectToCompare := spec{
					Assets: map[string]string{
						"app.py": `
def apple():
	print("hello world!")
`,
					},
				}

				diffs, err := utils.GetDiffs(baseObject1, objectToCompare, nil)
				assert.Nil(t, err)
				expectedDiff := map[string]string{
					"Assets.app.py": "small string with non unified diff",
				}
				assert.False(t, diffs[0].IsDiffTypeUnified())
				assertDiff(t, expectedDiff, diffs)
			})
			t.Run("if an array ", func(t *testing.T) {
				newBaseObject := Type1{
					Field1: "some",
					Field2: 1,
					Field3: map[string]string{
						"a": "c",
						"B": "d",
					},
					Field7: []string{
						"apple",
						"orange",
						"banana",
					},
				}
				t.Run("when array are identical", func(t *testing.T) {
					objectToCompare := Type1{
						Field1: "some",
						Field2: 1,
						Field3: map[string]string{
							"a": "c",
							"B": "d",
						},
						Field7: []string{
							"apple",
							"orange",
							"banana",
						},
					}
					diffs, err := utils.GetDiffs(newBaseObject, objectToCompare, nil)
					assert.Nil(t, err)
					expectedDiff := map[string]string{}
					assertDiff(t, expectedDiff, diffs)
				})
				t.Run("when items are missing", func(t *testing.T) {
					objectToCompare := Type1{
						Field1: "some",
						Field2: 1,
						Field3: map[string]string{
							"a": "c",
							"B": "d",
						},
					}
					diffs, err := utils.GetDiffs(newBaseObject, objectToCompare, nil)
					assert.Nil(t, err)
					expectedDiff := map[string]string{
						"Field7": "array comparison",
					}
					assertDiff(t, expectedDiff, diffs)
				})
				t.Run("when items are out of order and IgnoreOrderList is true", func(t *testing.T) {
					objectToCompare := Type1{
						Field1: "some",
						Field2: 1,
						Field3: map[string]string{
							"a": "c",
							"B": "d",
						},
						Field7: []string{
							"apple",
							"banana",
							"orange",
						},
					}
					diffs, err := utils.GetDiffs(newBaseObject, objectToCompare, &utils.CmpOptions{
						IgnoreOrderList: true,
					})
					assert.Nil(t, err)
					expectedDiff := map[string]string{}
					assertDiff(t, expectedDiff, diffs)
				})
				t.Run("when items are out of order and IgnoreOrderList is false", func(t *testing.T) {
					objectToCompare := Type1{
						Field1: "some",
						Field2: 1,
						Field3: map[string]string{
							"a": "c",
							"B": "d",
						},
						Field7: []string{
							"apple",
							"banana",
							"orange",
						},
					}
					diffs, err := utils.GetDiffs(newBaseObject, objectToCompare, &utils.CmpOptions{})
					assert.Nil(t, err)
					expectedDiff := map[string]string{
						"Field7.indicesModified": "array items reordered ",
					}
					assertDiff(t, expectedDiff, diffs)
				})
			})
		})
	})
}
