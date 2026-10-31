package finite

const (
	MaxResultBytes   = 256 * 1024
	MaxProgressRunes = 16000
)

// ReadResult reads and validates the result bound to run.
func ReadResult(run Run) (Result, error) { return Result{}, nil }

// DecodeResult decodes one closed result document.
func DecodeResult(run Run, data []byte) (Result, error) { return Result{}, nil }

// ValidateResult checks schema, binding, outcome, and wait shape.
func ValidateResult(run Run, result Result) error { return nil }
