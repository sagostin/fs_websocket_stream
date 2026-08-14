package main

import "testing"

func TestExtractAction(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    action
		wantErr bool
	}{
		{
			name: "plain json",
			in:   `{"destination": "support", "response": "Transferring you now."}`,
			want: action{Destination: "support", Response: "Transferring you now."},
		},
		{
			name: "fenced json",
			in:   "```json\n{\"destination\": null, \"response\": \"Which issue?\"}\n```",
			want: action{Response: "Which issue?"},
		},
		{
			name: "prose around json",
			in:   `Sure! Here you go: {"destination": "billing", "response": "To billing."} hope that helps`,
			want: action{Destination: "billing", Response: "To billing."},
		},
		{
			name:    "no json",
			in:      "I cannot help with that.",
			wantErr: true,
		},
		{
			name:    "empty object",
			in:      `{}`,
			wantErr: true,
		},
		{
			name:    "bad json",
			in:      `{destination: support}`,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractAction(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("extractAction: %v", err)
			}
			if got != tc.want {
				t.Fatalf("want %+v, got %+v", tc.want, got)
			}
		})
	}
}
