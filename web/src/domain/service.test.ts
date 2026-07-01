import { describe, expect, it } from "vitest";
import type { Service } from "../api";
import {
  channelKey,
  channelLabel,
  epgServiceGroupKey,
  epgServiceUnitKey,
  isStableEpgService,
  isTerrestrialService,
  isVisibleService,
  serviceKey,
} from "./service";

const service = (overrides: Partial<Service>): Service => ({
  id: 1,
  serviceId: 10,
  networkId: 1,
  name: "svc",
  type: 1,
  ...overrides,
});

describe("isVisibleService", () => {
  it("is true for digital TV (0x01) and IPTV (0xad) service types", () => {
    expect(isVisibleService(service({ type: 0x01 }))).toBe(true);
    expect(isVisibleService(service({ type: 0xad }))).toBe(true);
  });

  it("is false for other service types", () => {
    expect(isVisibleService(service({ type: 0x02 }))).toBe(false);
  });
});

describe("isTerrestrialService", () => {
  it("is true only when remoteControlKeyId is set", () => {
    expect(isTerrestrialService(service({ remoteControlKeyId: 5 }))).toBe(true);
    expect(isTerrestrialService(service({}))).toBe(false);
  });
});

describe("isStableEpgService", () => {
  it("requires epgReady and a non-false eitScheduleFlag", () => {
    expect(isStableEpgService(service({ epgReady: true, eitScheduleFlag: true }))).toBe(true);
    expect(isStableEpgService(service({ epgReady: true }))).toBe(true);
    expect(isStableEpgService(service({ epgReady: true, eitScheduleFlag: false }))).toBe(false);
    expect(isStableEpgService(service({ epgReady: false }))).toBe(false);
  });
});

describe("channelLabel", () => {
  it("joins type and channel when both are present", () => {
    expect(channelLabel("GR", "27")).toBe("GR 27");
  });

  it("returns - when either is missing", () => {
    expect(channelLabel(undefined, "27")).toBe("-");
    expect(channelLabel("GR", undefined)).toBe("-");
  });
});

describe("serviceKey / epgServiceUnitKey", () => {
  it("prefers the numeric service id when present", () => {
    expect(epgServiceUnitKey(service({ id: 42 }))).toBe("service-id:42");
  });

  it("falls back to transport stream id, then network/service id", () => {
    expect(epgServiceUnitKey({ id: undefined as unknown as number, networkId: 1, serviceId: 10, transportStreamId: 5 })).toBe("service:1:5:10");
    expect(epgServiceUnitKey({ id: undefined as unknown as number, networkId: 1, serviceId: 10 })).toBe(serviceKey({ networkId: 1, serviceId: 10 }));
  });
});

describe("epgServiceGroupKey", () => {
  it("groups terrestrial services by channel/network/TSID/remote-control-key", () => {
    const svc = service({
      remoteControlKeyId: 5,
      transportStreamId: 32736,
      channel: { type: "GR", channel: "27" },
    });
    expect(epgServiceGroupKey(svc)).toBe("terrestrial:GR:27:1:32736:5");
  });

  it("falls back to the unit key for non-terrestrial services", () => {
    const svc = service({ id: 7 });
    expect(epgServiceGroupKey(svc)).toBe(epgServiceUnitKey(svc));
  });
});

describe("channelKey", () => {
  it("returns empty string when type or channel is missing", () => {
    expect(channelKey(undefined)).toBe("");
    expect(channelKey({ type: "", channel: "27" })).toBe("");
  });

  it("builds a composite key otherwise", () => {
    expect(channelKey({ type: "GR", channel: "27" })).toBe("channel:GR:27");
  });
});
