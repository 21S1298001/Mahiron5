package contextvalue

import "context"

type Key[T any] struct{}

func With[T any](ctx context.Context, key Key[T], value T) context.Context {
	return context.WithValue(ctx, key, value)
}

func Get[T any](ctx context.Context, key Key[T]) (T, bool) {
	value, ok := ctx.Value(key).(T)
	return value, ok
}
