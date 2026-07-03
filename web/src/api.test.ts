import { describe, expect, it } from 'vitest'
import { parseProgramEventData, type EventItem, type Program } from './api'

const program: Program = {
  id: 1,
  eventId: 100,
  serviceId: 10,
  networkId: 1,
  startAt: 1700000000000,
  duration: 1800000,
  isFree: true,
}

function event(overrides: Partial<EventItem>): EventItem {
  return {
    resource: 'program',
    type: 'update',
    data: null,
    time: 0,
    ...overrides,
  }
}

describe('parseProgramEventData', () => {
  it('returns null when the resource is not a program', () => {
    expect(
      parseProgramEventData(event({ resource: 'service', data: program })),
    ).toBeNull()
  })

  it('parses a remove event with a valid id', () => {
    expect(
      parseProgramEventData(event({ type: 'remove', data: { id: 1 } })),
    ).toEqual({ id: 1 })
  })

  it('rejects a remove event with an invalid payload', () => {
    expect(
      parseProgramEventData(event({ type: 'remove', data: { id: '1' } })),
    ).toBeNull()
    expect(
      parseProgramEventData(event({ type: 'remove', data: {} })),
    ).toBeNull()
  })

  it.each(['create', 'update'] as const)(
    'parses a %s event with a valid program',
    (type) => {
      expect(parseProgramEventData(event({ type, data: program }))).toEqual(
        program,
      )
    },
  )

  it('rejects a create event missing required fields', () => {
    const { isFree: _isFree, ...incomplete } = program
    expect(
      parseProgramEventData(event({ type: 'create', data: incomplete })),
    ).toBeNull()
  })

  it('returns null for an unknown event type', () => {
    expect(
      parseProgramEventData(event({ type: 'unknown', data: program })),
    ).toBeNull()
  })
})
