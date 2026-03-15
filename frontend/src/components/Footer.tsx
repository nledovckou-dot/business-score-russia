export function Footer() {
  return (
    <footer className="border-t border-border bg-card px-6 pt-16 pb-8">
      <div className="mx-auto max-w-6xl">
        <div className="flex flex-col justify-between gap-12 md:flex-row">
          {/* Brand */}
          <div className="max-w-xs">
            <div className="flex items-center gap-3">
              <img src="/logo.png" alt="РУССКОР" width={28} height={28} />
              <span className="text-base font-semibold text-navy">РУССКОР</span>
            </div>
            <p className="mt-4 text-sm leading-relaxed text-muted">
              Анализ бизнеса 360. Полная картина компании за 10 минут из официальных и рыночных источников.
            </p>
          </div>

          {/* Links */}
          <div className="grid grid-cols-2 gap-12 sm:grid-cols-3">
            <div>
              <h4 className="mb-4 text-xs font-semibold tracking-widest uppercase text-muted-foreground">Продукт</h4>
              <ul className="space-y-3 text-sm text-muted">
                <li><a href="#features" className="transition-colors hover:text-foreground">Возможности</a></li>
                <li><a href="#how" className="transition-colors hover:text-foreground">Как работает</a></li>
                <li><a href="#sources" className="transition-colors hover:text-foreground">Данные</a></li>
              </ul>
            </div>
            <div>
              <h4 className="mb-4 text-xs font-semibold tracking-widest uppercase text-muted-foreground">Компания</h4>
              <ul className="space-y-3 text-sm text-muted">
                <li><a href="#" className="transition-colors hover:text-foreground">О нас</a></li>
                <li><a href="#" className="transition-colors hover:text-foreground">Контакты</a></li>
              </ul>
            </div>
            <div>
              <h4 className="mb-4 text-xs font-semibold tracking-widest uppercase text-muted-foreground">Правовое</h4>
              <ul className="space-y-3 text-sm text-muted">
                <li><a href="#" className="transition-colors hover:text-foreground">Конфиденциальность</a></li>
                <li><a href="#" className="transition-colors hover:text-foreground">Условия</a></li>
              </ul>
            </div>
          </div>
        </div>

        {/* Bottom */}
        <div className="mt-16 flex items-center justify-between border-t border-border pt-6 text-xs text-muted-foreground">
          <span>&copy; 2026 РУССКОР</span>
          <div className="flex items-center gap-2">
            <span className="size-1.5 rounded-full bg-green animate-pulse" />
            Все системы работают
          </div>
        </div>
      </div>
    </footer>
  );
}
