import type { Channel, Service } from "../api";

export function isVisibleService(service: Service) {
  return service.type === 0x01 || service.type === 0xad;
}

export function isTerrestrialService(service: Service) {
  // remoteControlKeyId is set from TSInformationDescriptor (tag 0xCD), terrestrial NIT only
  return service.remoteControlKeyId != null;
}

export function isStableEpgService(service: Service) {
  return service.eitScheduleFlag !== false && service.epgReady === true;
}

export function channelLabel(type?: string, channel?: string) {
  return type && channel ? `${type} ${channel}` : "-";
}

export function serviceKey(service: Pick<Service, "networkId" | "serviceId">) {
  return `service:${service.networkId}:${service.serviceId}`;
}

export function epgServiceUnitKey(service: Pick<Service, "id" | "networkId" | "serviceId" | "transportStreamId">) {
  if (service.id != null) {
    return `service-id:${service.id}`;
  }
  if (service.transportStreamId != null) {
    return `service:${service.networkId}:${service.transportStreamId}:${service.serviceId}`;
  }
  return serviceKey(service);
}

export function epgServiceGroupKey(service: Service) {
  if (isTerrestrialService(service) && service.transportStreamId != null && service.remoteControlKeyId != null) {
    return [
      "terrestrial",
      service.channel?.type ?? "",
      service.channel?.channel ?? "",
      service.networkId,
      service.transportStreamId,
      service.remoteControlKeyId,
    ].join(":");
  }
  return epgServiceUnitKey(service);
}

export function channelKey(channel?: Pick<Channel, "type" | "channel">) {
  if (!channel?.type || !channel.channel) return "";
  return `channel:${channel.type}:${channel.channel}`;
}
