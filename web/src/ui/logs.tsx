import type { UIEventHandler } from 'react'
import type { EventItem } from '../api'
import { formatDate } from '../format/date'
import { Empty } from './layout'

export function EventList({
  events,
  scrollRef,
  onScroll,
}: {
  events: EventItem[]
  scrollRef?: React.RefObject<HTMLDivElement | null>
  onScroll?: UIEventHandler<HTMLDivElement>
}) {
  if (events.length === 0) return <Empty message="イベントはありません。" />
  return (
    <div className="event-list" onScroll={onScroll} ref={scrollRef}>
      {events.map((event, index) => (
        <div className="event-item" key={`${event.time}-${index}`}>
          <time>{formatDate(event.time)}</time>
          <strong>
            {event.resource} / {event.type}
          </strong>
          <EventData data={event.data} resource={event.resource} />
        </div>
      ))}
    </div>
  )
}

function EventData({ data, resource }: { data: unknown; resource: string }) {
  if (resource === 'job' && isRecord(data) && isRecord(data.result)) {
    return (
      <>
        <span>
          {String(data.name ?? data.key ?? 'job')}:{' '}
          {String(data.result.summary ?? data.result.kind ?? '')}
        </span>
        <code>{JSON.stringify(data)}</code>
      </>
    )
  }
  return <code>{JSON.stringify(data)}</code>
}

export function ErrorList({ errors }: { errors: Array<string | null> }) {
  const visible = errors.filter(Boolean)
  if (visible.length === 0) return null
  return (
    <div className="error-list">
      {visible.map((error, index) => (
        <span key={index}>{error}</span>
      ))}
    </div>
  )
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
