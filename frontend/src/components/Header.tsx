"use client";

export function Header() {
  return (
    <header className="fixed top-0 left-0 right-0 z-50 border-b border-border bg-background/80 backdrop-blur-xl">
      <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-3">
        <a href="#" className="flex items-center gap-3">
          <img src="/logo.png" alt="РУССКОР" width={36} height={36} />
          <span className="text-lg font-semibold tracking-tight text-navy">РУССКОР</span>
        </a>

        <nav className="hidden items-center gap-8 text-sm text-muted md:flex">
          <a href="#features" className="transition-colors hover:text-foreground">Продукт</a>
          <a href="#how" className="transition-colors hover:text-foreground">Как работает</a>
          <a href="#sources" className="transition-colors hover:text-foreground">Источники</a>
        </nav>

        <a
          href="#start"
          className="rounded-full bg-navy px-5 py-2 text-sm font-semibold text-white transition-all hover:bg-navy/90"
        >
          Начать анализ
        </a>
      </div>
    </header>
  );
}
