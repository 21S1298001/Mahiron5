import type { Service, Tuner } from '../api'
import { channelKey, serviceKey } from './service'

export function openServiceMap(tuners: Tuner[]) {
  const services = new Map<string, Array<Tuner['users'][number]>>()
  for (const tuner of tuners) {
    for (const user of tuner.users) {
      const networkId = user.streamSetting?.networkId
      const serviceId = user.streamSetting?.serviceId
      if (networkId != null && serviceId != null) {
        appendOpenUser(services, `service:${networkId}:${serviceId}`, user)
      }

      const userChannelKey = channelKey(user.streamSetting?.channel)
      if (userChannelKey) {
        appendOpenUser(services, userChannelKey, user)
      }

      const tunedChannelKey = channelKey({
        type: tuner.currentChannelType ?? '',
        channel: tuner.currentChannel ?? '',
      })
      if (tunedChannelKey) {
        appendOpenUser(services, tunedChannelKey, user)
      }
    }
  }
  return services
}

export function openServiceUsers(
  openServices: Map<string, Array<Tuner['users'][number]>>,
  service: Service,
) {
  const byService = openServices.get(serviceKey(service)) ?? []
  const byChannel = openServices.get(channelKey(service.channel)) ?? []
  return [...byService, ...byChannel]
}

export function appendOpenUser(
  services: Map<string, Array<Tuner['users'][number]>>,
  key: string,
  user: Tuner['users'][number],
) {
  services.set(key, [...(services.get(key) ?? []), user])
}
