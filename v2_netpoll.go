package netpoll

import "context"

type OnEvent interface {
	OnActive(ctx context.Context, connection Connection) error

	OnData(ctx context.Context, connection Connection) error

	OnClose(ctx context.Context, connection Connection) error
}

type OnEventFactory interface {
	NewOnEvent(connection Connection) OnEvent
}
