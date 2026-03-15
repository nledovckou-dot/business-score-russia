import { Quote } from "lucide-react";
export function Testimonial() {
  return (
    <section className="px-6 py-24">
      <div className="mx-auto max-w-4xl rounded-3xl border border-border bg-card p-12 text-center lg:p-20">
        <Quote size={40} strokeWidth={1} className="mx-auto mb-8 text-accent/30" />

        <blockquote className="font-serif text-2xl leading-relaxed text-navy lg:text-3xl">
          За 10 минут получили то, что раньше собирали неделю из пяти разных сервисов.
          Один документ — и сразу видно, где мы и куда двигаться.
        </blockquote>

        <div className="mt-10 flex flex-col items-center gap-4">
          <img
            src="/pavel.jpg"
            alt="Павел Овсянников"
            width={56}
            height={56}
            className="rounded-full object-cover grayscale"
            style={{ width: 56, height: 56 }}
          />
          <div>
            <div className="text-sm font-semibold text-navy">Павел Овсянников</div>
            <div className="mt-1 text-xs tracking-widest uppercase text-muted-foreground">
              Ovsyannikov Soap
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
