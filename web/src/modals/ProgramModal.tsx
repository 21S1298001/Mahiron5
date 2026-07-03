import { useEffect } from 'react'
import type { Program, Service } from '../api'
import { formatCode, formatDuration, formatNumber } from '../format/common'
import { formatDate } from '../format/date'
import {
  audioLabel,
  programGenreLabels,
  programStatus,
} from '../domain/program'
import { Definition } from '../ui/metrics'

export function ProgramModal({
  program,
  service,
  onClose,
}: {
  program: Program
  service?: Service
  onClose: () => void
}) {
  const genres = programGenreLabels(program)
  const extended = Object.entries(program.extended ?? {}).filter(
    ([, value]) => value.trim() !== '',
  )
  const audios = program.audios ?? []
  const status = programStatus(program)

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onClose])

  return (
    <div className="modal-backdrop" onMouseDown={onClose} role="presentation">
      <aside
        className="program-modal"
        aria-modal="true"
        role="dialog"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header>
          <div>
            <span className="eyebrow">
              番組表 /{' '}
              {service?.name ?? `${program.networkId}/${program.serviceId}`}
            </span>
            <h2>{program.name || 'タイトルなし'}</h2>
            <div className="program-meta-line">
              <span>
                {formatDate(program.startAt)} -{' '}
                {formatDate(program.startAt + program.duration)}
              </span>
              <span>{formatDuration(program.duration)}</span>
              <span>{status}</span>
            </div>
          </div>
          <button
            className="icon-button"
            onClick={onClose}
            type="button"
            aria-label="閉じる"
          >
            x
          </button>
        </header>
        <section className="program-summary">
          <div className="summary-lead">
            <strong>概要</strong>
            <p>{program.description || '説明はありません。'}</p>
          </div>
          {extended.length > 0 && (
            <div className="extended-list">
              {extended.map(([label, value]) => (
                <div key={label}>
                  <strong>{label}</strong>
                  <p>{value}</p>
                </div>
              ))}
            </div>
          )}
        </section>
        {genres.length > 0 && (
          <section className="program-section">
            <h3>ジャンル</h3>
            <div className="tag-list">
              {genres.map((genre) => (
                <span key={genre}>{genre}</span>
              ))}
            </div>
          </section>
        )}
        <section className="program-section">
          <h3>放送情報</h3>
          <div className="definition-grid">
            <Definition label="サービス" value={service?.name ?? '-'} />
            <Definition
              label="チャンネル"
              value={
                service?.channel
                  ? `${service.channel.type} ${service.channel.channel}`
                  : '-'
              }
            />
            <Definition
              label="無料放送"
              value={program.isFree ? 'はい' : 'いいえ'}
            />
            <Definition
              label="映像"
              value={
                [program.video?.type, program.video?.resolution]
                  .filter(Boolean)
                  .join(' / ') || '-'
              }
            />
            <Definition label="番組ID" value={String(program.id)} />
            <Definition
              label="イベントID"
              value={formatCode(program.eventId)}
            />
            <Definition
              label="サービスID"
              value={formatCode(program.serviceId)}
            />
            <Definition
              label="ネットワークID"
              value={formatCode(program.networkId)}
            />
          </div>
        </section>
        {audios.length > 0 && (
          <section className="program-section">
            <h3>音声</h3>
            <div className="definition-grid">
              {audios.map((audio, index) => (
                <Definition
                  key={`${audio.componentTag ?? index}:${index}`}
                  label={
                    audio.isMain ? `主音声 ${index + 1}` : `音声 ${index + 1}`
                  }
                  value={audioLabel(audio)}
                />
              ))}
            </div>
          </section>
        )}
        {program.series && (
          <section className="program-section">
            <h3>シリーズ</h3>
            <div className="definition-grid">
              <Definition label="名前" value={program.series.name || '-'} />
              <Definition
                label="話数"
                value={
                  program.series.episode
                    ? `${program.series.episode}${program.series.lastEpisode ? ` / ${program.series.lastEpisode}` : ''}`
                    : '-'
                }
              />
              <Definition
                label="再放送"
                value={formatNumber(program.series.repeat)}
              />
              <Definition
                label="パターン"
                value={formatNumber(program.series.pattern)}
              />
              <Definition
                label="有効期限"
                value={formatDate(program.series.expiresAt)}
              />
            </div>
          </section>
        )}
      </aside>
    </div>
  )
}
