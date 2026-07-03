import type { Job } from '../api'

export function jobStatusLabel(status: Job['status']) {
  const labels: Record<Job['status'], string> = {
    queued: '待機中',
    standby: '準備中',
    running: '実行中',
    finished: '完了',
  }
  return labels[status] ?? status
}

export function currentGatheringNetworks(jobs: Job[]) {
  return jobs
    .filter(
      (job) =>
        job.status === 'running' && job.key.startsWith('epg-gather:nid:'),
    )
    .map((job) => job.key.slice('epg-gather:nid:'.length))
    .filter(Boolean)
}
