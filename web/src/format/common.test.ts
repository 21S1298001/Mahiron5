import { describe, expect, it } from 'vitest'
import { formatBytes, formatCode, formatDuration, formatNumber } from './common'

describe('formatNumber', () => {
  it('returns - for nullish values', () => {
    expect(formatNumber(undefined)).toBe('-')
  })

  it('groups digits', () => {
    expect(formatNumber(1234)).toBe('1,234')
  })
})

describe('formatBytes', () => {
  it('returns - for nullish values', () => {
    expect(formatBytes(undefined)).toBe('-')
  })

  it('keeps small byte counts unscaled', () => {
    expect(formatBytes(512)).toBe('512 B')
  })

  it('scales up through binary units', () => {
    expect(formatBytes(1536)).toBe('1.5 KiB')
    expect(formatBytes(1024 * 1024 * 2)).toBe('2.0 MiB')
  })
})

describe('formatDuration', () => {
  it('returns - for nullish values', () => {
    expect(formatDuration(undefined)).toBe('-')
  })

  it('formats sub-hour durations in minutes', () => {
    expect(formatDuration(30 * 60000)).toBe('30分')
  })

  it('formats exact hours without a minute remainder', () => {
    expect(formatDuration(120 * 60000)).toBe('2時間')
  })

  it('formats hours with a minute remainder', () => {
    expect(formatDuration(90 * 60000)).toBe('1時間30分')
  })
})

describe('formatCode', () => {
  it('returns - for nullish values', () => {
    expect(formatCode(undefined)).toBe('-')
  })

  it('shows decimal and uppercase hex', () => {
    expect(formatCode(255)).toBe('255 / 0xFF')
  })
})
