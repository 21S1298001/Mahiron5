import type { ReactNode } from 'react'
import type { Service } from '../api'

export function PageFrame({
  title,
  subtitle,
  children,
}: {
  title: string
  subtitle: string
  children: ReactNode
}) {
  return (
    <>
      <header className="page-header">
        <div>
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </div>
      </header>
      {children}
    </>
  )
}

export function Panel({
  title,
  action,
  children,
}: {
  title: string
  action?: ReactNode
  children: ReactNode
}) {
  return (
    <section className="panel">
      <header>
        <h2>{title}</h2>
        {action}
      </header>
      {children}
    </section>
  )
}

export function Empty({ message }: { message: string }) {
  return <div className="empty">{message}</div>
}

export function Logo({ service }: { service: Service }) {
  if (!service.hasLogoData)
    return (
      <div className="logo-fallback">
        {service.name.slice(0, 2).toUpperCase()}
      </div>
    )
  return (
    <img
      className="service-logo"
      src={`/api/services/${service.id}/logo`}
      alt=""
      loading="lazy"
    />
  )
}
