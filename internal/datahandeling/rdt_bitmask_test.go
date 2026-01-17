package datahandeling

import "testing"

func TestHexBitmaskToBinaryString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     string
		want   string
		wantOK bool
	}{
		{
			name:   "preserves leading zeros by hex width",
			in:     "007",
			want:   "000000000111",
			wantOK: true,
		},
		{
			name:   "accepts 0x prefix",
			in:     "0x007",
			want:   "000000000111",
			wantOK: true,
		},
		{
			name:   "single nibble",
			in:     "f",
			want:   "1111",
			wantOK: true,
		},
		{
			name:   "empty string",
			in:     "",
			want:   "",
			wantOK: false,
		},
		{
			name:   "invalid hex",
			in:     "zz",
			want:   "",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, ok := hexBitmaskToBinaryString(tt.in)
			if ok != tt.wantOK {
				t.Fatalf("ok=%v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}
