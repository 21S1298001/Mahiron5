import { describe, expect, it } from 'vitest'
import { openServiceMetricValue } from '../domain/service'

describe('openServiceMetricValue', () => {
  it('keeps the service count visible while refreshing existing data', () => {
    expect(openServiceMetricValue(2, true, 5)).toBe('2/5')
  })

  it('shows a placeholder only before service data is loaded', () => {
    expect(openServiceMetricValue(0, false, 0)).toBe('0/-')
  })
})
