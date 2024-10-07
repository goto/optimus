package maxcompute

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToViewSQL(t *testing.T) {
	type args struct {
		v *View
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "create_view",
			args: args{
				v: &View{
					Name:        "Test_View1",
					Description: "Create Test View",
					Columns:     []string{"a", "b", "c"},
					ViewQuery:   "select a, b, c from t1",
				},
			},
			want: `create or replace view if not exists Test_View1
    (a, b, c)  
    comment 'Create Test View'  
    as
	select a, b, c from t1;`,
			wantErr: nil,
		},
		{
			name: "create_view_missing_description",
			args: args{
				v: &View{
					Name:      "Test_View1",
					Columns:   []string{"a", "b", "c"},
					ViewQuery: "select a, b, c from t1",
				},
			},
			want: `create or replace view if not exists Test_View1
    (a, b, c)  
    as
	select a, b, c from t1;`,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToViewSQL(tt.args.v)
			if tt.wantErr != nil && !tt.wantErr(t, err, fmt.Sprintf("ToViewSQL error in (%s)", tt.name)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ToViewSQL(%v)", tt.args.v)
		})
	}
}
