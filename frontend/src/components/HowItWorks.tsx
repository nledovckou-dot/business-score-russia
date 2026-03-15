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

        {/* Timeline — dot + content aligned in same row */}
        <div className="relative max-w-2xl">
          {/* Vertical line */}
          <div className="absolute top-3 bottom-3 left-[5px] w-px bg-border hidden md:block" />

          <div className="space-y-12">
            {steps.map((s) => (
              <div key={s.title} className="group flex items-start gap-6">
                {/* Dot */}
                <div className="hidden md:flex flex-col items-center pt-1.5">
                  <div
                    className={`relative z-10 size-3 rounded-full border-2 transition-transform group-hover:scale-125 ${
                      s.active ? "border-accent bg-accent" : "border-border bg-background"
                    }`}
                  />
                </div>

                {/* Content */}
                <div className="flex-1">
                  <span className={`mb-1 block text-xs font-mono ${s.active ? "text-accent" : "text-muted-foreground"}`}>
                    {s.version}
                  </span>
                  <h3 className="mb-2 text-xl font-semibold text-navy transition-colors group-hover:text-accent">{s.title}</h3>
                  <p className="max-w-lg leading-relaxed text-muted">{s.description}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
