import { describe, expect, it, vi } from 'vitest'
import type { Program } from '../api'
import {
  audioLabel,
  normalizeProgramText,
  programGenreClass,
  programGenreLabels,
  programStatus,
} from './program'

const program = (overrides: Partial<Program>): Program => ({
  id: 1,
  eventId: 100,
  serviceId: 10,
  networkId: 1,
  startAt: 0,
  duration: 3600,
  isFree: true,
  ...overrides,
})

describe('programStatus', () => {
  it('reports upcoming, ongoing and finished programs', () => {
    vi.setSystemTime(1000)
    expect(programStatus(program({ startAt: 2000, duration: 100 }))).toBe(
      '放送予定',
    )
    expect(programStatus(program({ startAt: 500, duration: 1000 }))).toBe(
      '放送中',
    )
    expect(programStatus(program({ startAt: 0, duration: 100 }))).toBe(
      '放送終了',
    )
    vi.useRealTimers()
  })
})

describe('programGenreLabels', () => {
  it('labels a known lv1/lv2 pair', () => {
    expect(
      programGenreLabels(program({ genres: [{ lv1: 0, lv2: 1 }] })),
    ).toEqual(['ニュース／報道 / 天気'])
  })

  it('falls back to lv1-only label when lv2 is unknown', () => {
    expect(programGenreLabels(program({ genres: [{ lv1: 0 }] }))).toEqual([
      'ニュース／報道',
    ])
  })

  it('labels lv1 15 as other', () => {
    expect(programGenreLabels(program({ genres: [{ lv1: 15 }] }))).toEqual([
      'その他',
    ])
  })
})

describe('programGenreClass', () => {
  it("uses the first genre's lv1 when in range", () => {
    expect(programGenreClass(program({ genres: [{ lv1: 3 }] }))).toBe('genre-3')
  })

  it('falls back to genre-other when missing or out of range', () => {
    expect(programGenreClass(program({}))).toBe('genre-other')
    expect(programGenreClass(program({ genres: [{ lv1: 15 }] }))).toBe(
      'genre-other',
    )
  })
})

describe('audioLabel', () => {
  it('joins available audio fields', () => {
    const label = audioLabel({
      componentType: 1,
      componentTag: 2,
      samplingRate: 48000,
      langs: ['jpn'],
    })
    expect(label).toBe('jpn / コンポーネント 1 / タグ 2 / 48000 Hz')
  })

  it('falls back to the component type alone when nothing else is available', () => {
    expect(audioLabel({ componentType: 1 })).toBe('コンポーネント 1')
  })
})

describe('normalizeProgramText', () => {
  it('collapses whitespace and trims', () => {
    expect(normalizeProgramText('  a  b\n c ')).toBe('a b c')
  })

  it('treats undefined as empty string', () => {
    expect(normalizeProgramText(undefined)).toBe('')
  })
})
