import { describe, expect, it } from "vitest";
import { floorHour, formatHourOnly, formatMinute, formatMonthDayWeekday, isSameDate } from "./date";

describe("formatHourOnly", () => {
  it("pads single digit hours", () => {
    expect(formatHourOnly(new Date(2026, 0, 1, 5, 30).getTime())).toBe("05時");
  });
});

describe("formatMinute", () => {
  it("pads single digit minutes", () => {
    expect(formatMinute(new Date(2026, 0, 1, 5, 3).getTime())).toBe("03");
  });
});

describe("formatMonthDayWeekday", () => {
  it("formats month/day and Japanese weekday", () => {
    // 2026-01-01 is a Thursday
    expect(formatMonthDayWeekday(new Date(2026, 0, 1).getTime())).toBe("1/1(木)");
  });
});

describe("isSameDate", () => {
  it("is true for timestamps on the same calendar day", () => {
    const morning = new Date(2026, 0, 1, 1).getTime();
    const evening = new Date(2026, 0, 1, 23).getTime();
    expect(isSameDate(morning, evening)).toBe(true);
  });

  it("is false across a day boundary", () => {
    const day1 = new Date(2026, 0, 1, 23).getTime();
    const day2 = new Date(2026, 0, 2, 1).getTime();
    expect(isSameDate(day1, day2)).toBe(false);
  });
});

describe("floorHour", () => {
  it("zeroes out minutes, seconds and milliseconds", () => {
    const value = new Date(2026, 0, 1, 5, 42, 30, 500).getTime();
    expect(new Date(floorHour(value))).toEqual(new Date(2026, 0, 1, 5, 0, 0, 0));
  });
});
