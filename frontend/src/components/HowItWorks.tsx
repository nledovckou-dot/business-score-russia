const steps = [
  {
    version: "Шаг 1",
    title: "Вставьте ссылку",
    description: "Укажите сайт компании. Мы определим бренд, юрлицо и тип бизнеса автоматически.",
    active: true,
  },
  {
    version: "Шаг 2",
    title: "Проверьте данные",
    description: "Уточните название, ИНН и список конкурентов. Поправьте, если нужно.",
  },
  {
    version: "Шаг 3",
    title: "Мы собираем отчёт",
    description: "Сбор из 10+ источников, верификация фактов, визуализация данных.",
  },
  {
    version: "Шаг 4",
    title: "Получите документ",
    description: "Готовый отчёт с графиками, таблицами и стратегическими выводами.",
  },
];

export function HowItWorks() {
  return (
    <section className="px-6 py-24" id="how">
      <div className="mx-auto max-w-6xl">
        <div className="mb-16 max-w-2xl">
          <p className="mb-3 text-xs font-semibold tracking-widest uppercase text-accent">Как работает</p>
          <h2 className="font-serif text-3xl leading-tight text-navy lg:text-4xl">
            От ссылки до готового отчёта.
          </h2>
          <div className="mt-4 h-px w-16 bg-accent/40" />
        </div>

        <div className="grid gap-12 md:grid-cols-[200px_1fr]">
          {/* Timeline */}
          <div className="relative hidden md:block">
            <div className="absolute top-2 bottom-2 left-[5px] w-px bg-border" />
            {steps.map((s, i) => (
              <div key={i} className="group relative flex items-start gap-4 pb-20 last:pb-0">
                <div
                  className={`relative z-10 size-3 rounded-full border-2 transition-transform group-hover:scale-125 ${
                    s.active ? "border-accent bg-accent" : "border-border bg-background"
                  }`}
                />
                <span className={`text-xs font-mono ${s.active ? "text-accent" : "text-muted-foreground"}`}>
                  {s.version}
                </span>
              </div>
            ))}
          </div>

          {/* Content */}
          <div className="space-y-16">
            {steps.map((s) => (
              <div key={s.title} className="group">
                <span className="mb-2 block text-xs font-mono text-muted-foreground md:hidden">{s.version}</span>
                <h3 className="mb-2 text-xl font-semibold text-navy transition-colors group-hover:text-accent">{s.title}</h3>
                <p className="max-w-lg leading-relaxed text-muted">{s.description}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
