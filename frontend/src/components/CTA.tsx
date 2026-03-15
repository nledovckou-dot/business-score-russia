import { ArrowRight } from "lucide-react";

export function CTA() {
  return (
    <section className="px-6 py-24">
      <div className="mx-auto max-w-3xl text-center">
        <h2 className="font-serif text-3xl leading-tight text-navy lg:text-4xl">
          Попробуйте бесплатно.
        </h2>
        <p className="mx-auto mt-4 max-w-lg text-muted">
          Вставьте ссылку на сайт компании — мы соберём полный отчёт.
        </p>
        <a
          href="#start"
          className="group mt-8 inline-flex items-center gap-2 rounded-full bg-navy px-8 py-4 text-lg font-semibold text-white transition-all hover:bg-navy/90"
        >
          Начать анализ
          <ArrowRight size={18} className="transition-transform group-hover:translate-x-0.5" />
        </a>
      </div>
    </section>
  );
}
