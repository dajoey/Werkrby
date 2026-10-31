package finite

import "context"

// Dispatch atomically reserves a run before invoking its launcher.
func (s *Store) Dispatch(ctx context.Context, req DispatchRequest, launch LaunchFunc) (Run, error) {
	return Run{}, nil
}
