export function formatDate(value?: number) {
  if (value == null) return "-";
  return new Intl.DateTimeFormat(undefined, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

export function formatHourOnly(value: number) {
  return `${new Date(value).getHours().toString().padStart(2, "0")}時`;
}

export function formatMonthDayWeekday(value: number) {
  const date = new Date(value);
  const weekdays = ["日", "月", "火", "水", "木", "金", "土"];
  return `${date.getMonth() + 1}/${date.getDate()}(${weekdays[date.getDay()]})`;
}

export function formatMinute(value: number) {
  return new Date(value).getMinutes().toString().padStart(2, "0");
}

export function isSameDate(a: number, b: number) {
  const dateA = new Date(a);
  const dateB = new Date(b);
  return dateA.getFullYear() === dateB.getFullYear()
    && dateA.getMonth() === dateB.getMonth()
    && dateA.getDate() === dateB.getDate();
}

export function floorHour(value: number) {
  const date = new Date(value);
  date.setMinutes(0, 0, 0);
  return date.getTime();
}
