import type { Channel, Service } from '../api'

export function isVisibleService(service: Service) {
  return service.type === 0x01 || service.type === 0xad
}

export function sortServicesForDisplay(services: Service[]) {
  const channelTypeOrder = new Map<string, number>()
  for (const service of services) {
    const type = service.channel?.type ?? ''
    if (!channelTypeOrder.has(type)) {
      channelTypeOrder.set(type, channelTypeOrder.size)
    }
  }

  return [...services].sort(
    (a, b) =>
      compareNumbers(
        channelTypeOrder.get(a.channel?.type ?? '') ?? 0,
        channelTypeOrder.get(b.channel?.type ?? '') ?? 0,
      ) ||
      compareOptionalNumbers(a.remoteControlKeyId, b.remoteControlKeyId) ||
      compareTerrestrialNetworkIds(a, b) ||
      compareNumbers(
        logicalChannelSortNumber(a),
        logicalChannelSortNumber(b),
      ) ||
      compareNumbers(a.serviceId, b.serviceId) ||
      compareNumbers(a.networkId, b.networkId) ||
      compareNumbers(a.id, b.id),
  )
}

export function isTerrestrialService(service: Service) {
  // remoteControlKeyId is set from TSInformationDescriptor (tag 0xCD), terrestrial NIT only
  return service.remoteControlKeyId != null
}

export function isStableEpgService(service: Service) {
  return service.eitScheduleFlag !== false && service.epgReady === true
}

export function channelLabel(type?: string, channel?: string) {
  return type && channel ? `${type} ${channel}` : '-'
}

export function serviceKey(service: Pick<Service, 'networkId' | 'serviceId'>) {
  return `service:${service.networkId}:${service.serviceId}`
}

export function epgServiceUnitKey(
  service: Pick<
    Service,
    'id' | 'networkId' | 'serviceId' | 'transportStreamId'
  >,
) {
  if (service.id != null) {
    return `service-id:${service.id}`
  }
  if (service.transportStreamId != null) {
    return `service:${service.networkId}:${service.transportStreamId}:${service.serviceId}`
  }
  return serviceKey(service)
}

export function epgServiceGroupKey(service: Service) {
  if (
    isTerrestrialService(service) &&
    service.transportStreamId != null &&
    service.remoteControlKeyId != null
  ) {
    return [
      'terrestrial',
      service.channel?.type ?? '',
      service.channel?.channel ?? '',
      service.networkId,
      service.transportStreamId,
      service.remoteControlKeyId,
    ].join(':')
  }
  return epgServiceUnitKey(service)
}

export function channelKey(channel?: Pick<Channel, 'type' | 'channel'>) {
  if (!channel?.type || !channel.channel) return ''
  return `channel:${channel.type}:${channel.channel}`
}

function compareTerrestrialNetworkIds(a: Service, b: Service) {
  if (!isTerrestrialService(a) || !isTerrestrialService(b)) {
    return 0
  }
  return compareNumbers(a.networkId, b.networkId)
}

function logicalChannelSortNumber(service: Service) {
  const serviceType = (service.serviceId >> 7) & 0x03
  const serviceNumber = service.serviceId & 0x07
  const remoteControlKeyId = service.remoteControlKeyId ?? 0
  return serviceType * 200 + remoteControlKeyId * 10 + serviceNumber + 1
}

function compareOptionalNumbers(a: number | undefined, b: number | undefined) {
  if (a == null && b == null) return 0
  if (a == null) return 1
  if (b == null) return -1
  return compareNumbers(a, b)
}

function compareNumbers(a: number, b: number) {
  return a - b
}
