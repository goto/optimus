package larkdrive

import (
	"testing"

	"github.com/goto/optimus/ext/sheets/lark"

	larksdk "github.com/larksuite/oapi-sdk-go/v3"
)

func TestClient_GetFolderToken(t *testing.T) {
	type fields struct {
		client      *larksdk.Client
		sheetClient *lark.Client
	}
	type args struct {
		url string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name:   "Extract folder token from valid URL without subsdomain",
			fields: fields{},
			args: args{
				url: "https://larksuite.com/drive/folder/1234567890abcdef",
			},
			want:    "1234567890abcdef",
			wantErr: false,
		},
		{
			name:   "Extract folder token from valid URL with subsdomain",
			fields: fields{},
			args: args{
				url: "https://subdomain.larksuite.com/drive/folder/1234567890abcdef",
			},
			want:    "1234567890abcdef",
			wantErr: false,
		},
		{
			name:   "Invalid URL format",
			fields: fields{},
			args: args{
				url: "https://drive.google.com/drive/folder/1234567890abcdef",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := &Client{
				client:      tt.fields.client,
				sheetClient: tt.fields.sheetClient,
			}
			got, err := cl.GetFolderToken(tt.args.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetFolderToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetFolderToken() got = %v, want %v", got, tt.want)
			}
		})
	}
}
