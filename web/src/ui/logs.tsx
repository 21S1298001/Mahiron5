import type { UIEventHandler } from "react";
import type { EventItem } from "../api";
import { formatDate } from "../format/date";
import { Empty } from "./layout";

export function EventList({
  events,
  scrollRef,
  onScroll,
}: {
  events: EventItem[];
  scrollRef?: React.RefObject<HTMLDivElement | null>;
  onScroll?: UIEventHandler<HTMLDivElement>;
}) {
  if (events.length === 0) return <Empty message="イベントはありません。" />;
  return (
    <div className="event-list" onScroll={onScroll} ref={scrollRef}>
      {events.map((event, index) => (
        <div className="event-item" key={`${event.time}-${index}`}>
          <time>{formatDate(event.time)}</time>
          <strong>{event.resource} / {event.type}</strong>
          <code>{JSON.stringify(event.data)}</code>
        </div>
      ))}
    </div>
  );
}

export function ErrorList({ errors }: { errors: Array<string | null> }) {
  const visible = errors.filter(Boolean);
  if (visible.length === 0) return null;
  return (
    <div className="error-list">
      {visible.map((error, index) => <span key={index}>{error}</span>)}
    </div>
  );
}
