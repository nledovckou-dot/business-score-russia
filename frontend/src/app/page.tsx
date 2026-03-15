import { Header } from "@/components/Header";
import { Hero } from "@/components/Hero";
import { Features } from "@/components/Features";
import { HowItWorks } from "@/components/HowItWorks";
import { Sources } from "@/components/Sources";
import { Testimonial } from "@/components/Testimonial";
import { CTA } from "@/components/CTA";
import { Footer } from "@/components/Footer";

export default function Home() {
  return (
    <>
      <Header />
      <main>
        <Hero />
        <Features />
        <HowItWorks />
        <Sources />
        <Testimonial />
        <CTA />
      </main>
      <Footer />
    </>
  );
}
