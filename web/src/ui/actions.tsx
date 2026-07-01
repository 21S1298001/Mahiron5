import { useState } from "react";
import { StreamState } from "./metrics";

export function CopyRow({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false);
  async function copy() {
    await navigator.clipboard.writeText(value);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1400);
  }
  return (
    <div className="copy-row">
      <span>{label}</span>
      <code>{value}</code>
      <button onClick={copy} type="button">{copied ? "コピー済み" : "コピー"}</button>
    </div>
  );
}

export function LogPanelActions({ connected, following, onFollow }: { connected: boolean; following: boolean; onFollow: () => void }) {
  return (
    <div className="log-panel-actions">
      <button className="follow-button" data-visible={!following} onClick={onFollow} tabIndex={following ? -1 : 0} type="button">
        下へ
      </button>
      <StreamState connected={connected} />
    </div>
  );
}
