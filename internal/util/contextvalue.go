package util

import "context"

type ContextKey[T any] struct{}

func ContextWith[T any](ctx context.Context, key ContextKey[T], value T) context.Context {
	return context.WithValue(ctx, key, value)
}

func ContextGet[T any](ctx context.Context, key ContextKey[T]) (T, bool) {
	value, ok := ctx.Value(key).(T)
	return value, ok
}
