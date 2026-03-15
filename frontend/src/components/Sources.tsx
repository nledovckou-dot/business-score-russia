import { Landmark, TrendingUp, Search, MapPin, Users, ShieldCheck } from "lucide-react";

const sources = [
  { icon: Landmark, name: "Финансы и отчётность", desc: "Выручка, прибыль, активы, рентабельность за 3 года" },
  { icon: TrendingUp, name: "Рынок и конкуренты", desc: "8-12 игроков, доли рынка, каналы продаж" },
  { icon: Search, name: "Digital и видимость", desc: "SEO-трафик, позиции, рекламные бюджеты" },
  { icon: MapPin, name: "Репутация и отзывы", desc: "Рейтинги на картах, отзывы клиентов" },
  { icon: Users, name: "Команда и найм", desc: "Вакансии, зарплаты, структура команды" },
  { icon: ShieldCheck, name: "Юридический профиль", desc: "Учредители, лицензии, суды, госконтракты" },
];

export function Sources() {
  return (
    <section className="px-6 py-24" id="sources">
      <div className="mx-auto max-w-6xl">
        <div className="mb-16 max-w-2xl">
          <p className="mb-3 text-xs font-semibold tracking-widest uppercase text-accent">Данные в отчёте</p>
          <h2 className="font-serif text-3xl leading-tight text-navy lg:text-4xl">
            Собираем полную картину из официальных и рыночных сигналов.
          </h2>
          <div className="mt-4 h-px w-16 bg-accent/40" />
        </div>

        <div className="grid gap-px rounded-2xl bg-border sm:grid-cols-2 lg:grid-cols-3">
          {sources.map((s) => (
            <div
              key={s.name}
              className="group flex items-start gap-4 bg-background p-8 transition-colors duration-500 hover:bg-card"
            >
              <s.icon size={24} strokeWidth={1} className="mt-0.5 shrink-0 text-muted-foreground transition-colors group-hover:text-accent" />
              <div>
                <div className="font-semibold text-navy">{s.name}</div>
                <div className="mt-1 text-sm text-muted">{s.desc}</div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
