export function formatNumber(value?: number) {
  return value == null ? "-" : new Intl.NumberFormat().format(value);
}

export function formatBytes(value?: number) {
  if (value == null) return "-";
  const units = ["B", "KiB", "MiB", "GiB"];
  let amount = value;
  let index = 0;
  while (amount >= 1024 && index < units.length - 1) {
    amount /= 1024;
    index += 1;
  }
  return `${amount.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

export function formatDuration(value?: number) {
  if (value == null) return "-";
  const minutes = Math.round(value / 60000);
  if (minutes < 60) return `${minutes}分`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest === 0 ? `${hours}時間` : `${hours}時間${rest}分`;
}

export function formatCode(value?: number) {
  if (value == null) return "-";
  return `${value} / 0x${value.toString(16).toUpperCase()}`;
}
