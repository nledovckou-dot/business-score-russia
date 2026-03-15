"use client";

import { ArrowRight } from "lucide-react";

export function Hero() {
  return (
    <section className="relative px-6 pt-28 pb-24 lg:pt-40 lg:pb-32" id="start">
      <div className="mx-auto max-w-6xl">
        <div className="grid gap-16 lg:grid-cols-2 lg:items-center">
          {/* Copy */}
          <div>
            <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-border px-4 py-1.5 text-xs font-semibold tracking-widest uppercase text-accent">
              <span className="size-1.5 rounded-full bg-green animate-pulse" />
              Бета-версия
            </div>

            <h1 className="font-serif text-4xl leading-[1.08] tracking-tight text-navy lg:text-6xl">
              Сначала понять<br />
              <span className="text-accent">бизнес.</span>{" "}
              Потом действовать.
            </h1>

            <p className="mt-6 max-w-lg text-lg leading-relaxed text-muted">
              Вставьте ссылку на сайт компании — мы соберём полный отчёт
              с финансами, конкурентами и стратегией.
            </p>

            <div className="mt-10 flex gap-3" id="url-input">
              <input
                type="url"
                placeholder="https://example.com"
                className="flex-1 rounded-2xl border border-border bg-card px-5 py-4 text-foreground placeholder:text-muted-foreground outline-none transition-colors focus:border-accent/50 focus:ring-2 focus:ring-accent/10"
              />
              <button className="group flex items-center gap-2 rounded-2xl bg-navy px-7 py-4 font-semibold text-white transition-all hover:bg-navy/90 whitespace-nowrap">
                Анализировать
                <ArrowRight size={16} className="transition-transform group-hover:translate-x-0.5" />
              </button>
            </div>

            <div className="mt-8 flex flex-wrap gap-6 text-sm text-muted-foreground">
              <span>10+ источников</span>
              <span className="text-border">|</span>
              <span>40+ проверяемых фактов</span>
            </div>
          </div>

          {/* Preview — process of analysis */}
          <div className="relative">
            <div className="overflow-hidden rounded-2xl border border-border bg-card shadow-xl">
              {/* Window chrome */}
              <div className="flex items-center gap-2 border-b border-border px-4 py-3">
                <div className="size-2.5 rounded-full bg-red/40" />
                <div className="size-2.5 rounded-full bg-accent/40" />
                <div className="size-2.5 rounded-full bg-green/40" />
                <span className="ml-3 text-xs text-muted-foreground font-mono">анализ · в процессе</span>
              </div>

              {/* Analysis process */}
              <div className="p-6 space-y-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs font-semibold tracking-widest uppercase text-accent">Сбор данных</span>
                  <span className="text-xs font-mono text-muted-foreground">72%</span>
                </div>

                {/* Progress bar */}
                <div className="h-1 w-full rounded-full bg-border overflow-hidden">
                  <div className="h-full w-[72%] rounded-full bg-gradient-to-r from-accent to-accent/60" />
                </div>

                {/* Steps */}
                <div className="space-y-3 pt-2">
                  {[
                    { status: "done", label: "Сайт и продукт", detail: "бренд определён, тип бизнеса — B2C" },
                    { status: "done", label: "Юридический профиль", detail: "ИНН найден, выручка — 48М ₽" },
                    { status: "done", label: "Конкурентное поле", detail: "9 игроков, 3 прямых конкурента" },
                    { status: "done", label: "Финансы и динамика", detail: "рост выручки +23% за год" },
                    { status: "active", label: "Digital и видимость", detail: "собираем SEO, трафик, соцсети..." },
                    { status: "pending", label: "Кадровые сигналы", detail: "" },
                    { status: "pending", label: "Стратегический вывод", detail: "" },
                  ].map((step) => (
                    <div key={step.label} className="flex items-start gap-3">
                      <div className={`mt-1 size-4 shrink-0 rounded-full border flex items-center justify-center text-[9px] ${
                        step.status === "done"
                          ? "bg-green border-green text-white"
                          : step.status === "active"
                          ? "border-accent text-accent animate-pulse"
                          : "border-border text-transparent"
                      }`}>
                        {step.status === "done" ? "✓" : step.status === "active" ? "·" : ""}
                      </div>
                      <div className="min-w-0">
                        <div className={`text-sm font-medium ${
                          step.status === "pending" ? "text-muted-foreground" : "text-navy"
                        }`}>{step.label}</div>
                        {step.detail && (
                          <div className={`text-xs mt-0.5 ${
                            step.status === "active" ? "text-accent" : "text-muted"
                          }`}>{step.detail}</div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Background gradient */}
      <div className="pointer-events-none absolute inset-0 -z-10">
        <div className="absolute top-0 left-1/4 h-96 w-96 rounded-full bg-accent/5 blur-[120px]" />
        <div className="absolute right-0 bottom-0 h-64 w-64 rounded-full bg-navy/5 blur-[100px]" />
      </div>
    </section>
  );
}
