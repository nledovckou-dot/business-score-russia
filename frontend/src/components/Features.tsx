import { Building2, TrendingUp, Target } from "lucide-react";

const features = [
  {
    icon: Building2,
    title: "Компания и рынок",
    description: "Юрлицо, ИНН, ОКВЭД, выручка, прибыль. Тип бизнеса и продуктовый контур из официальных реестров.",
  },
  {
    icon: TrendingUp,
    title: "Финансы и конкуренты",
    description: "8-12 прямых конкурентов с финансами, каналами продаж и сравнительным анализом за 3 года.",
  },
  {
    icon: Target,
    title: "Стратегия и выводы",
    description: "SWOT, риски, возможности и конкретный план действий.",
  },
];

export function Features() {
  return (
    <section className="px-6 py-24" id="features">
      <div className="mx-auto max-w-6xl">
        <div className="mb-16 max-w-2xl">
          <p className="mb-3 text-xs font-semibold tracking-widest uppercase text-accent">Что на выходе</p>
          <h2 className="font-serif text-3xl leading-tight text-navy lg:text-4xl">
            Не справка, а собранная управленческая картина.
          </h2>
          <div className="mt-4 h-px w-16 bg-accent/40" />
        </div>

        <div className="grid gap-px rounded-2xl bg-border md:grid-cols-3">
          {features.map((f) => (
            <div
              key={f.title}
              className="group bg-background p-10 transition-colors duration-500 first:rounded-l-2xl last:rounded-r-2xl hover:bg-card"
            >
              <f.icon size={32} strokeWidth={1} className="mb-6 text-muted-foreground transition-colors group-hover:text-accent" />
              <h3 className="mb-3 text-lg font-semibold text-navy">{f.title}</h3>
              <p className="leading-relaxed text-muted">{f.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
