import type { Program } from '../api'
import { genreLv1Names, genreLv2Names } from '../data/genres'

export function programStatus(program: Program) {
  const now = Date.now()
  if (now < program.startAt) return '放送予定'
  if (now < program.startAt + program.duration) return '放送中'
  return '放送終了'
}

export function programGenreLabels(program: Program) {
  return (program.genres ?? []).map((genre) => {
    const lv1 = genre.lv1 ?? -1
    const lv2 = genre.lv2 ?? -1
    const main =
      genreLv1Names[lv1] ?? (lv1 === 15 ? 'その他' : `ジャンル ${lv1}`)
    const sub = genreLv2Names[lv1]?.[lv2]
    return sub ? `${main} / ${sub}` : main
  })
}

export function programGenreClass(program: Program) {
  const lv1 = program.genres?.[0]?.lv1
  if (lv1 == null || lv1 < 0 || lv1 > 11) return 'genre-other'
  return `genre-${lv1}`
}

export function audioLabel(audio: NonNullable<Program['audios']>[number]) {
  return (
    [
      audio.langs?.join(', '),
      audio.componentType != null
        ? `コンポーネント ${audio.componentType}`
        : undefined,
      audio.componentTag != null ? `タグ ${audio.componentTag}` : undefined,
      audio.samplingRate != null ? `${audio.samplingRate} Hz` : undefined,
    ]
      .filter(Boolean)
      .join(' / ') || '-'
  )
}

export function normalizeProgramText(value?: string) {
  return (value ?? '').replace(/\s+/g, ' ').trim()
}
