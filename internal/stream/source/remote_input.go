package source

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/tuner"
)

type remoteInput struct {
	channel config.ChannelConfig
	client  RemoteClient
	mu      sync.Mutex
	remote  string
	users   map[string]trackedUser
}

type trackedUser struct {
	user tuner.User
	refs int
}

func newRemoteInput(client RemoteClient, channel config.ChannelConfig, remoteName string) *remoteInput {
	return &remoteInput{client: client, channel: channel, remote: remoteName, users: map[string]trackedUser{}}
}

func (i *remoteInput) Subscribe(ctx context.Context, variant StreamVariant, dst io.Writer) error {
	return i.client.ChannelStream(ctx, i.channel.Type, i.channel.Channel, variant == StreamDecoded, dst)
}

func (*remoteInput) SupportsDecodedInput() bool { return true }

func (i *remoteInput) WithUser(ctx context.Context, run func(context.Context) error) error {
	user, ok := tuner.UserFromContext(ctx)
	if !ok || user.ID == "" {
		return run(ctx)
	}
	i.mu.Lock()
	tracked := i.users[user.ID]
	tracked.user, tracked.refs = user, tracked.refs+1
	i.users[user.ID] = tracked
	i.mu.Unlock()
	defer func() {
		i.mu.Lock()
		tracked := i.users[user.ID]
		if tracked.refs <= 1 {
			delete(i.users, user.ID)
		} else {
			tracked.refs--
			i.users[user.ID] = tracked
		}
		i.mu.Unlock()
	}()
	return run(ctx)
}

func (i *remoteInput) RemoteName() string { return i.remote }

func (i *remoteInput) MatchesTuner(status tuner.Status) bool {
	return status.TunedChannelType == i.channel.Type && status.TunedChannel == i.channel.Channel ||
		status.CurrentChannelType == i.channel.Type && status.CurrentChannel == i.channel.Channel
}

func (i *remoteInput) Users() []tuner.User {
	i.mu.Lock()
	defer i.mu.Unlock()
	users := make([]tuner.User, 0, len(i.users))
	for _, tracked := range i.users {
		users = append(users, tracked.user)
	}
	sort.Slice(users, func(a, b int) bool { return users[a].ID < users[b].ID })
	return users
}
