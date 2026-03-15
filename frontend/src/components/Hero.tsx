"use client";

import { ArrowRight } from "lucide-react";
import { Globe } from "./Globe";

export function Hero() {
  return (
    <section className="relative px-6 pt-28 pb-24 lg:pt-40 lg:pb-32" id="start">
      <div className="mx-auto max-w-6xl">
        <div className="grid gap-12 lg:grid-cols-2 lg:items-center">
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

          {/* Globe */}
          <div className="relative flex items-center justify-center">
            <div className="w-full aspect-square max-w-[600px]">
              <Globe />
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
