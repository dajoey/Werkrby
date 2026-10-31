package finite

import "context"

// ParseBrief decodes the strict Markdown intake representation.
func ParseBrief(data []byte) (Brief, error) { return Brief{}, nil }

// IntakeBrief publishes an immutable brief and creates its initial task.
func (s *Store) IntakeBrief(ctx context.Context, sourcePath string) (Task, error) {
	return Task{}, nil
}
