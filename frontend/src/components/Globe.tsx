"use client";

import { useEffect, useRef } from "react";

interface Point3D {
  x: number;
  y: number;
  z: number;
}

export function Globe() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let animId: number;
    let angle = 0;
    const dpr = window.devicePixelRatio || 1;

    const resize = () => {
      const rect = canvas.getBoundingClientRect();
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.scale(dpr, dpr);
    };
    resize();
    window.addEventListener("resize", resize);

    // Generate sphere points (fibonacci distribution)
    const NUM_POINTS = 120;
    const points: Point3D[] = [];
    const goldenAngle = Math.PI * (3 - Math.sqrt(5));

    for (let i = 0; i < NUM_POINTS; i++) {
      const y = 1 - (i / (NUM_POINTS - 1)) * 2;
      const radius = Math.sqrt(1 - y * y);
      const theta = goldenAngle * i;
      points.push({
        x: Math.cos(theta) * radius,
        y: y,
        z: Math.sin(theta) * radius,
      });
    }

    // Pre-compute connections (close neighbors)
    const connections: [number, number][] = [];
    const CONN_DIST = 0.58;
    for (let i = 0; i < points.length; i++) {
      for (let j = i + 1; j < points.length; j++) {
        const dx = points[i].x - points[j].x;
        const dy = points[i].y - points[j].y;
        const dz = points[i].z - points[j].z;
        if (Math.sqrt(dx * dx + dy * dy + dz * dz) < CONN_DIST) {
          connections.push([i, j]);
        }
      }
    }

    const draw = () => {
      const w = canvas.width / dpr;
      const h = canvas.height / dpr;
      const cx = w / 2;
      const cy = h / 2;
      const R = Math.min(w, h) * 0.38;

      ctx.clearRect(0, 0, w, h);
      angle += 0.003;

      // Rotate points
      const cosA = Math.cos(angle);
      const sinA = Math.sin(angle);
      const tilt = 0.3;
      const cosT = Math.cos(tilt);
      const sinT = Math.sin(tilt);

      const projected = points.map((p) => {
        // Y-axis rotation
        const rx = p.x * cosA - p.z * sinA;
        const rz = p.x * sinA + p.z * cosA;
        // X-axis tilt
        const ry = p.y * cosT - rz * sinT;
        const rz2 = p.y * sinT + rz * cosT;

        const scale = 1 / (1 - rz2 * 0.15); // subtle perspective
        return {
          sx: cx + rx * R * scale,
          sy: cy + ry * R * scale,
          z: rz2,
          scale,
        };
      });

      // Draw connections
      for (const [i, j] of connections) {
        const a = projected[i];
        const b = projected[j];
        const avgZ = (a.z + b.z) / 2;
        if (avgZ < -0.2) continue; // cull backface

        const alpha = Math.max(0, Math.min(0.18, (avgZ + 1) * 0.12));
        ctx.beginPath();
        ctx.moveTo(a.sx, a.sy);
        ctx.lineTo(b.sx, b.sy);
        ctx.strokeStyle = `rgba(26, 43, 74, ${alpha})`;
        ctx.lineWidth = 0.5;
        ctx.stroke();
      }

      // Draw points
      for (const p of projected) {
        if (p.z < -0.3) continue;

        const alpha = Math.max(0.05, Math.min(0.8, (p.z + 1) * 0.5));
        const size = Math.max(1, 2.5 * ((p.z + 1) / 2));

        // Gold for front points, navy for back
        const isGold = p.z > 0.5;
        if (isGold) {
          ctx.beginPath();
          ctx.arc(p.sx, p.sy, size + 1, 0, Math.PI * 2);
          ctx.fillStyle = `rgba(201, 164, 76, ${alpha * 0.3})`;
          ctx.fill();
        }

        ctx.beginPath();
        ctx.arc(p.sx, p.sy, size, 0, Math.PI * 2);
        ctx.fillStyle = isGold
          ? `rgba(201, 164, 76, ${alpha})`
          : `rgba(26, 43, 74, ${alpha})`;
        ctx.fill();
      }

      // Subtle glow ring
      const gradient = ctx.createRadialGradient(cx, cy, R * 0.85, cx, cy, R * 1.1);
      gradient.addColorStop(0, "rgba(201, 164, 76, 0)");
      gradient.addColorStop(0.5, "rgba(201, 164, 76, 0.03)");
      gradient.addColorStop(1, "rgba(201, 164, 76, 0)");
      ctx.beginPath();
      ctx.arc(cx, cy, R * 1.1, 0, Math.PI * 2);
      ctx.fillStyle = gradient;
      ctx.fill();

      animId = requestAnimationFrame(draw);
    };

    draw();

    return () => {
      cancelAnimationFrame(animId);
      window.removeEventListener("resize", resize);
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      className="w-full h-full"
      style={{ display: "block" }}
    />
  );
}
