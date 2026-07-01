import { describe, expect, it } from "vitest";
import type { Job } from "../api";
import { currentGatheringNetworks, jobStatusLabel } from "./job";

const job = (overrides: Partial<Job>): Job => ({
  id: "1",
  key: "job",
  name: "job",
  status: "queued",
  retryCount: 0,
  isAborting: false,
  createdAt: 0,
  updatedAt: 0,
  ...overrides,
});

describe("jobStatusLabel", () => {
  it("maps known statuses to Japanese labels", () => {
    expect(jobStatusLabel("queued")).toBe("待機中");
    expect(jobStatusLabel("running")).toBe("実行中");
  });
});

describe("currentGatheringNetworks", () => {
  it("extracts network ids from running epg-gather jobs", () => {
    const jobs = [
      job({ status: "running", key: "epg-gather:nid:1" }),
      job({ status: "finished", key: "epg-gather:nid:2" }),
      job({ status: "running", key: "other:job" }),
    ];
    expect(currentGatheringNetworks(jobs)).toEqual(["1"]);
  });
});
